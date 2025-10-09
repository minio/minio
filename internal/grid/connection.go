// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package grid

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/pubsub"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/tinylib/msgp/msgp"
	"github.com/zeebo/xxh3"
)

func gridLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "grid", err, errKind...)
}

func gridLogIfNot(ctx context.Context, err error, ignored ...error) {
	logger.LogIfNot(ctx, "grid", err, ignored...)
}

func gridLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "grid", err, id, errKind...)
}

// A Connection is a remote connection.
// There is no distinction externally whether the connection was initiated from
// this server or from the remote.
type Connection struct {
	// NextID is the next ID that can be used (atomic).
	NextID uint64

	// LastPong is last pong time (atomic)
	// Only valid when StateConnected.
	LastPong int64

	// State of the connection (atomic)
	state State

	// Non-atomic
	Remote string
	Local  string

	// ID of this connection instance.
	id uuid.UUID

	// Remote uuid, if we have been connected.
	remoteID    *uuid.UUID
	reconnectMu sync.Mutex

	// Context for the server.
	ctx context.Context

	// Active mux connections.
	outgoing *xsync.MapOf[uint64, *muxClient]

	// Incoming streams
	inStream *xsync.MapOf[uint64, *muxServer]

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State

	// Dialer for outgoing connections.
	dial   ConnDialer
	authFn AuthFn

	handleMsgWg sync.WaitGroup

	// connChange will be signaled whenever State has been updated, or at regular intervals.
	// Holding the lock allows safe reads of State, and guarantees that changes will be detected.
	connChange *sync.Cond
	handlers   *handlers

	remote             *RemoteClient
	clientPingInterval time.Duration
	connPingInterval   time.Duration
	blockConnect       chan struct{}

	incomingBytes func(n int64) // Record incoming bytes.
	outgoingBytes func(n int64) // Record outgoing bytes.
	trace         *tracer       // tracer for this connection.
	baseFlags     Flags
	outBytes      atomic.Int64
	inBytes       atomic.Int64
	inMessages    atomic.Int64
	outMessages   atomic.Int64
	reconnects    atomic.Int64
	lastConnect   atomic.Pointer[time.Time]
	lastPingDur   atomic.Int64

	// For testing only
	debugInConn   net.Conn
	debugOutConn  net.Conn
	blockMessages atomic.Pointer[<-chan struct{}]
	addDeadline   time.Duration
	connMu        sync.Mutex
}

// Subroute is a connection subroute that can be used to route to a specific handler with the same handler ID.
type Subroute struct {
	*Connection
	trace *tracer
	route string
	subID subHandlerID
}

// String returns a string representation of the connection.
func (c *Connection) String() string {
	return fmt.Sprintf("%s->%s", c.Local, c.Remote)
}

// StringReverse returns a string representation of the reverse connection.
func (c *Connection) StringReverse() string {
	return fmt.Sprintf("%s->%s", c.Remote, c.Local)
}

// State is a connection state.
type State uint32

// MANUAL go:generate stringer -type=State -output=state_string.go -trimprefix=State $GOFILE

const (
	// StateUnconnected is the initial state of a connection.
	// When the first message is sent it will attempt to connect.
	StateUnconnected = iota

	// StateConnecting is the state from StateUnconnected while the connection is attempted to be established.
	// After this connection will be StateConnected or StateConnectionError.
	StateConnecting

	// StateConnected is the state when the connection has been established and is considered stable.
	// If the connection is lost, state will switch to StateConnecting.
	StateConnected

	// StateConnectionError is the state once a connection attempt has been made, and it failed.
	// The connection will remain in this stat until the connection has been successfully re-established.
	StateConnectionError

	// StateShutdown is the state when the server has been shut down.
	// This will not be used under normal operation.
	StateShutdown

	// MaxDeadline is the maximum deadline allowed,
	// Approx 49 days.
	MaxDeadline = time.Duration(math.MaxUint32) * time.Millisecond
)

// ContextDialer is a dialer that can be used to dial a remote.
type ContextDialer func(ctx context.Context, network, address string) (net.Conn, error)

// DialContext implements the Dialer interface.
func (c ContextDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return c(ctx, network, address)
}

const (
	defaultOutQueue    = 65535    // kind of close to max open fds per user
	readBufferSize     = 32 << 10 // 32 KiB is the most optimal on Linux
	writeBufferSize    = 32 << 10 // 32 KiB is the most optimal on Linux
	defaultDialTimeout = 2 * time.Second
	connPingInterval   = 10 * time.Second
	connWriteTimeout   = 3 * time.Second
)

type connectionParams struct {
	ctx           context.Context
	id            uuid.UUID
	local, remote string
	handlers      *handlers
	incomingBytes func(n int64) // Record incoming bytes.
	outgoingBytes func(n int64) // Record outgoing bytes.
	publisher     *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]
	dialer        ConnDialer
	authFn        AuthFn

	blockConnect chan struct{}
}

// newConnection will create an unconnected connection to a remote.
func newConnection(o connectionParams) *Connection {
	c := &Connection{
		state:              StateUnconnected,
		Remote:             o.remote,
		Local:              o.local,
		id:                 o.id,
		ctx:                o.ctx,
		outgoing:           xsync.NewMapOfPresized[uint64, *muxClient](1000),
		inStream:           xsync.NewMapOfPresized[uint64, *muxServer](1000),
		outQueue:           make(chan []byte, defaultOutQueue),
		side:               ws.StateServerSide,
		connChange:         &sync.Cond{L: &sync.Mutex{}},
		handlers:           o.handlers,
		remote:             &RemoteClient{Name: o.remote},
		clientPingInterval: clientPingInterval,
		connPingInterval:   connPingInterval,
		dial:               o.dialer,
		authFn:             o.authFn,
	}
	if debugPrint {
		// Random Mux ID
		c.NextID = rand.Uint64()
	}

	// Record per connection stats.
	c.outgoingBytes = func(n int64) {
		if o.outgoingBytes != nil {
			o.outgoingBytes(n)
		}
		c.outBytes.Add(n)
	}
	c.incomingBytes = func(n int64) {
		if o.incomingBytes != nil {
			o.incomingBytes(n)
		}
		c.inBytes.Add(n)
	}
	if !strings.HasPrefix(o.remote, "https://") && !strings.HasPrefix(o.remote, "wss://") {
		c.baseFlags |= FlagCRCxxh3
	}
	if !strings.HasPrefix(o.local, "https://") && !strings.HasPrefix(o.local, "wss://") {
		c.baseFlags |= FlagCRCxxh3
	}
	if o.publisher != nil {
		c.traceRequests(o.publisher)
	}
	if o.local == o.remote {
		panic("equal hosts")
	}
	if c.shouldConnect() {
		c.side = ws.StateClientSide

		go func() {
			if o.blockConnect != nil {
				<-o.blockConnect
			}
			c.connect()
		}()
	}
	if debugPrint {
		fmt.Println(c.Local, "->", c.Remote, "Should local connect:", c.shouldConnect(), "side:", c.side)
	}
	if debugReqs {
		fmt.Println("Created connection", c.String())
	}
	return c
}

// Subroute returns a static subroute for the connection.
func (c *Connection) Subroute(s string) *Subroute {
	if c == nil {
		return nil
	}
	return &Subroute{
		Connection: c,
		route:      s,
		subID:      makeSubHandlerID(0, s),
		trace:      c.trace.subroute(s),
	}
}

// Subroute adds a subroute to the subroute.
// The subroutes are combined with '/'.
func (c *Subroute) Subroute(s string) *Subroute {
	route := strings.Join([]string{c.route, s}, "/")
	return &Subroute{
		Connection: c.Connection,
		route:      route,
		subID:      makeSubHandlerID(0, route),
		trace:      c.trace.subroute(route),
	}
}

// newMuxClient returns a mux client for manual use.
func (c *Connection) newMuxClient(ctx context.Context) (*muxClient, error) {
	client := newMuxClient(ctx, atomic.AddUint64(&c.NextID, 1), c)
	if dl, ok := ctx.Deadline(); ok {
		client.deadline = getDeadline(time.Until(dl))
		if client.deadline == 0 {
			client.cancelFn(context.DeadlineExceeded)
			return nil, context.DeadlineExceeded
		}
	}
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, loaded := c.outgoing.LoadOrStore(client.MuxID, client); client.MuxID != 0 && !loaded {
			if debugReqs {
				_, found := c.outgoing.Load(client.MuxID)
				fmt.Println(client.MuxID, c.String(), "Connection.newMuxClient: RELOADED MUX. loaded:", loaded, "found:", found)
			}
			return client, nil
		}
		client.MuxID = atomic.AddUint64(&c.NextID, 1)
	}
}

// newMuxClient returns a mux client for manual use.
func (c *Subroute) newMuxClient(ctx context.Context) (*muxClient, error) {
	cl, err := c.Connection.newMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	cl.subroute = &c.subID
	return cl, nil
}

// Request allows to do a single remote request.
// 'req' will not be used after the call and caller can reuse.
// If no deadline is set on ctx, a 1-minute deadline will be added.
func (c *Connection) Request(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	// Create mux client and call.
	client, err := c.newMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if debugReqs {
			_, ok := c.outgoing.Load(client.MuxID)
			fmt.Println(client.MuxID, c.String(), "Connection.Request: DELETING MUX. Exists:", ok)
		}
		client.cancelFn(context.Canceled)
		c.outgoing.Delete(client.MuxID)
	}()
	return client.traceRoundtrip(ctx, c.trace, h, req)
}

// Request allows to do a single remote request.
// 'req' will not be used after the call and caller can reuse.
// If no deadline is set on ctx, a 1-minute deadline will be added.
func (c *Subroute) Request(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	// Create mux client and call.
	client, err := c.newMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	client.subroute = &c.subID
	defer func() {
		if debugReqs {
			fmt.Println(client.MuxID, c.String(), "Subroute.Request: DELETING MUX")
		}
		client.cancelFn(context.Canceled)
		c.outgoing.Delete(client.MuxID)
	}()
	return client.traceRoundtrip(ctx, c.trace, h, req)
}

// NewStream creates a new stream.
// Initial payload can be reused by the caller.
func (c *Connection) NewStream(ctx context.Context, h HandlerID, payload []byte) (st *Stream, err error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	handler := c.handlers.streams[h]
	if handler == nil {
		return nil, ErrUnknownHandler
	}

	var requests chan []byte
	var responses chan Response
	if handler.InCapacity > 0 {
		requests = make(chan []byte, handler.InCapacity)
	}
	if handler.OutCapacity > 0 {
		responses = make(chan Response, handler.OutCapacity)
	} else {
		responses = make(chan Response, 1)
	}

	cl, err := c.newMuxClient(ctx)
	if err != nil {
		return nil, err
	}

	return cl.RequestStream(h, payload, requests, responses)
}

// NewStream creates a new stream.
// Initial payload can be reused by the caller.
func (c *Subroute) NewStream(ctx context.Context, h HandlerID, payload []byte) (st *Stream, err error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	handler := c.handlers.subStreams[makeZeroSubHandlerID(h)]
	if handler == nil {
		if debugPrint {
			fmt.Println("want", makeZeroSubHandlerID(h), c.route, "got", c.handlers.subStreams)
		}
		return nil, ErrUnknownHandler
	}

	var requests chan []byte
	var responses chan Response
	if handler.InCapacity > 0 {
		requests = make(chan []byte, handler.InCapacity)
	}
	if handler.OutCapacity > 0 {
		responses = make(chan Response, handler.OutCapacity)
	} else {
		responses = make(chan Response, 1)
	}

	cl, err := c.newMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	cl.subroute = &c.subID

	return cl.RequestStream(h, payload, requests, responses)
}

// WaitForConnect will block until a connection has been established or
// the context is canceled, in which case the context error is returned.
func (c *Connection) WaitForConnect(ctx context.Context) error {
	if debugPrint {
		fmt.Println(c.Local, "->", c.Remote, "WaitForConnect")
		defer fmt.Println(c.Local, "->", c.Remote, "WaitForConnect done")
	}
	c.connChange.L.Lock()
	if atomic.LoadUint32((*uint32)(&c.state)) == StateConnected {
		c.connChange.L.Unlock()
		// Happy path.
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	changed := make(chan State, 1)
	go func() {
		defer xioutil.SafeClose(changed)
		for {
			c.connChange.Wait()
			newState := c.State()
			select {
			case changed <- newState:
				if newState == StateConnected || newState == StateShutdown {
					c.connChange.L.Unlock()
					return
				}
			case <-ctx.Done():
				c.connChange.L.Unlock()
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case newState := <-changed:
			if newState == StateConnected {
				return nil
			}
		}
	}
}

/*
var ErrDone = errors.New("done for now")

var ErrRemoteRestart = errors.New("remote restarted")


// Stateless connects to the remote handler and return all packets sent back.
// If the remote is restarted will return ErrRemoteRestart.
// If nil will be returned remote call sent EOF or ErrDone is returned by the callback.
// If ErrDone is returned on cb nil will be returned.
func (c *Connection) Stateless(ctx context.Context, h HandlerID, req []byte, cb func([]byte) error) error {
	client, err := c.newMuxClient(ctx)
	if err != nil {
		return err
	}
	defer c.outgoing.Delete(client.MuxID)
	resp := make(chan Response, 10)
	client.RequestStateless(h, req, resp)

	for r := range resp {
		if r.Err != nil {
			return r.Err
		}
		if len(r.Msg) > 0 {
			err := cb(r.Msg)
			if err != nil {
				if errors.Is(err, ErrDone) {
					break
				}
				return err
			}
		}
	}
	return nil
}
*/

// shouldConnect returns a deterministic bool whether the local should initiate the connection.
// It should be 50% chance of any host initiating the connection.
func (c *Connection) shouldConnect() bool {
	// The remote should have the opposite result.
	h0 := xxh3.HashString(c.Local + c.Remote)
	h1 := xxh3.HashString(c.Remote + c.Local)
	if h0 == h1 {
		return c.Local < c.Remote
	}
	return h0 < h1
}

func (c *Connection) send(ctx context.Context, msg []byte) error {
	select {
	case <-ctx.Done():
		// Returning error here is too noisy.
		return nil
	case c.outQueue <- msg:
		return nil
	}
}

// queueMsg queues a message, with an optional payload.
// sender should not reference msg.Payload
func (c *Connection) queueMsg(msg message, payload sender) error {
	// Add baseflags.
	msg.Flags.Set(c.baseFlags)
	// This cannot encode subroute.
	msg.Flags.Clear(FlagSubroute)
	if payload != nil {
		if sz := payload.Msgsize(); cap(msg.Payload) < sz {
			PutByteBuffer(msg.Payload)
			msg.Payload = GetByteBufferCap(sz)
		}
		var err error
		msg.Payload, err = payload.MarshalMsg(msg.Payload[:0])
		msg.Op = payload.Op()
		if err != nil {
			return err
		}
	}
	defer PutByteBuffer(msg.Payload)
	dst := GetByteBufferCap(msg.Msgsize())
	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	return c.send(c.ctx, dst)
}

// sendMsg will send
func (c *Connection) sendMsg(conn net.Conn, msg message, payload msgp.MarshalSizer) error {
	if payload != nil {
		if sz := payload.Msgsize(); cap(msg.Payload) < sz {
			PutByteBuffer(msg.Payload)
			msg.Payload = GetByteBufferCap(sz)[:0]
		}
		var err error
		msg.Payload, err = payload.MarshalMsg(msg.Payload)
		if err != nil {
			return err
		}
		defer PutByteBuffer(msg.Payload)
	}
	dst := GetByteBufferCap(msg.Msgsize())
	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	if debugPrint {
		fmt.Println(c.Local, "sendMsg: Sending", msg.Op, "as", len(dst), "bytes")
	}
	if c.outgoingBytes != nil {
		c.outgoingBytes(int64(len(dst)))
	}
	err = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
	if err != nil {
		return err
	}
	return wsutil.WriteMessage(conn, c.side, ws.OpBinary, dst)
}

func (c *Connection) connect() {
	c.updateState(StateConnecting)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Runs until the server is shut down.
	for {
		if c.State() == StateShutdown {
			return
		}
		dialStarted := time.Now()
		if debugPrint {
			fmt.Println(c.Local, "Connecting to ", c.Remote)
		}
		conn, err := c.dial(c.ctx, c.Remote)
		c.connMu.Lock()
		c.debugOutConn = conn
		c.connMu.Unlock()
		retry := func(err error) {
			if debugPrint {
				fmt.Printf("%v Connecting to %v: %v. Retrying.\n", c.Local, c.Remote, err)
			}
			sleep := defaultDialTimeout + time.Duration(rng.Int63n(int64(defaultDialTimeout)))
			next := dialStarted.Add(sleep / 2)
			sleep = max(time.Until(next).Round(time.Millisecond), 0)
			gotState := c.State()
			if gotState == StateShutdown {
				return
			}
			if gotState != StateConnecting {
				// Don't print error on first attempt, and after that only once per hour.
				gridLogOnceIf(c.ctx, fmt.Errorf("grid: %s re-connecting to %s: %w (%T) Sleeping %v (%v)", c.Local, c.Remote, err, err, sleep, gotState), c.Remote)
			}
			c.updateState(StateConnectionError)
			time.Sleep(sleep)
		}
		if err != nil {
			retry(err)
			continue
		}
		// Send connect message.
		m := message{
			Op: OpConnect,
		}
		req := connectReq{
			Host: c.Local,
			ID:   c.id,
			Time: time.Now(),
		}
		req.addToken(c.authFn)
		err = c.sendMsg(conn, m, &req)
		if err != nil {
			retry(err)
			continue
		}
		// Wait for response
		var r connectResp
		err = c.receive(conn, &r)
		if err != nil {
			if debugPrint {
				fmt.Println(c.Local, "receive err:", err, "side:", c.side)
			}
			retry(err)
			continue
		}
		if debugPrint {
			fmt.Println(c.Local, "Got connectResp:", r)
		}
		if !r.Accepted {
			retry(fmt.Errorf("connection rejected: %s", r.RejectedReason))
			continue
		}
		t := time.Now().UTC()
		c.lastConnect.Store(&t)
		c.reconnectMu.Lock()
		remoteUUID := uuid.UUID(r.ID)
		if c.remoteID != nil {
			c.reconnected()
		}
		c.remoteID = &remoteUUID
		if debugPrint {
			fmt.Println(c.Local, "Connected Waiting for Messages")
		}
		// Handle messages...
		c.handleMessages(c.ctx, conn)
		// Reconnect unless we are shutting down (debug only).
		if c.State() == StateShutdown {
			conn.Close()
			return
		}
		if debugPrint {
			fmt.Println(c.Local, "Disconnected. Attempting to reconnect.")
		}
	}
}

func (c *Connection) disconnected() {
	c.outgoing.Range(func(key uint64, client *muxClient) bool {
		if !client.stateless {
			client.cancelFn(ErrDisconnected)
		}
		return true
	})
	if debugReqs {
		fmt.Println(c.String(), "Disconnected. Clearing outgoing.")
	}
	c.outgoing.Clear()
	c.inStream.Range(func(key uint64, client *muxServer) bool {
		client.cancel()
		return true
	})
	c.inStream.Clear()
}

func (c *Connection) receive(conn net.Conn, r receiver) error {
	b, op, err := wsutil.ReadData(conn, c.side)
	if err != nil {
		return err
	}
	if op != ws.OpBinary {
		return fmt.Errorf("unexpected connect response type %v", op)
	}
	if c.incomingBytes != nil {
		c.incomingBytes(int64(len(b)))
	}

	var m message
	_, _, err = m.parse(b)
	if err != nil {
		return err
	}
	if m.Op != r.Op() {
		return fmt.Errorf("unexpected response OP, want %v, got %v", r.Op(), m.Op)
	}
	_, err = r.UnmarshalMsg(m.Payload)
	return err
}

func (c *Connection) handleIncoming(ctx context.Context, conn net.Conn, req connectReq) error {
	c.connMu.Lock()
	c.debugInConn = conn
	c.connMu.Unlock()
	if c.blockConnect != nil {
		// Block until we are allowed to connect.
		<-c.blockConnect
	}
	if req.Host != c.Remote {
		err := fmt.Errorf("expected remote '%s', got '%s'", c.Remote, req.Host)
		if debugPrint {
			fmt.Println(err)
		}
		return err
	}
	if c.shouldConnect() {
		if debugPrint {
			fmt.Println("expected to be client side, not server side")
		}
		return errors.New("grid: expected to be client side, not server side")
	}
	msg := message{
		Op: OpConnectResponse,
	}

	resp := connectResp{
		ID:       c.id,
		Accepted: true,
	}
	err := c.sendMsg(conn, msg, &resp)
	if debugPrint {
		fmt.Printf("grid: Queued Response %+v Side: %v\n", resp, c.side)
	}
	if err != nil {
		return err
	}
	t := time.Now().UTC()
	c.lastConnect.Store(&t)
	// Signal that we are reconnected, update state and handle messages.
	// Prevent other connections from connecting while we process.
	c.reconnectMu.Lock()
	if c.remoteID != nil {
		c.reconnected()
	}
	rid := uuid.UUID(req.ID)
	c.remoteID = &rid

	// Handle incoming messages until disconnect.
	c.handleMessages(ctx, conn)
	return nil
}

// reconnected signals the connection has been reconnected.
// It will close all active requests and streams.
// caller *must* hold reconnectMu.
func (c *Connection) reconnected() {
	c.updateState(StateConnectionError)
	c.reconnects.Add(1)

	// Drain the outQueue, so any blocked messages can be sent.
	// We keep the queue, but start draining it, if it gets full.
	stopDraining := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	defer func() {
		close(stopDraining)
		wg.Wait()
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopDraining:
				return
			default:
				if cap(c.outQueue)-len(c.outQueue) > 100 {
					// Queue is not full, wait a bit.
					time.Sleep(1 * time.Millisecond)
					continue
				}
				select {
				case v := <-c.outQueue:
					PutByteBuffer(v)
				case <-stopDraining:
					return
				}
			}
		}
	}()
	// Close all active requests.
	if debugReqs {
		fmt.Println(c.String(), "Reconnected. Clearing outgoing.")
	}
	c.outgoing.Range(func(key uint64, client *muxClient) bool {
		client.close()
		return true
	})
	c.inStream.Range(func(key uint64, value *muxServer) bool {
		value.close()
		return true
	})

	c.inStream.Clear()
	c.outgoing.Clear()

	// Wait for existing to exit
	c.handleMsgWg.Wait()
}

func (c *Connection) updateState(s State) {
	c.connChange.L.Lock()
	defer c.connChange.L.Unlock()

	// We may have reads that aren't locked, so update atomically.
	gotState := atomic.LoadUint32((*uint32)(&c.state))
	if gotState == StateShutdown || State(gotState) == s {
		return
	}
	if s == StateConnected {
		atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())
	}
	atomic.StoreUint32((*uint32)(&c.state), uint32(s))
	if debugPrint {
		fmt.Println(c.Local, "updateState:", gotState, "->", s)
	}
	c.connChange.Broadcast()
}

// monitorState will monitor the state of the connection and close the net.Conn if it changes.
func (c *Connection) monitorState(conn net.Conn, cancel context.CancelCauseFunc) {
	c.connChange.L.Lock()
	defer c.connChange.L.Unlock()
	for {
		newState := c.State()
		if newState != StateConnected {
			conn.Close()
			cancel(ErrDisconnected)
			return
		}
		// Unlock and wait for state change.
		c.connChange.Wait()
	}
}

// handleMessages will handle incoming messages on conn.
// caller *must* hold reconnectMu.
func (c *Connection) handleMessages(ctx context.Context, conn net.Conn) {
	c.updateState(StateConnected)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(ErrDisconnected)

	// This will ensure that is something asks to disconnect and we are blocked on reads/writes
	// the connection will be closed and readers/writers will unblock.
	go c.monitorState(conn, cancel)

	c.handleMsgWg.Add(2)
	c.reconnectMu.Unlock()

	// Start reader and writer
	go c.readStream(ctx, conn, cancel)
	c.writeStream(ctx, conn, cancel)
}

// readStream handles the read side of the connection.
// It will read messages and send them to c.handleMsg.
// If an error occurs the cancel function will be called and conn be closed.
// The function will block until the connection is closed or an error occurs.
func (c *Connection) readStream(ctx context.Context, conn net.Conn, cancel context.CancelCauseFunc) {
	defer func() {
		if rec := recover(); rec != nil {
			gridLogIf(ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
			debug.PrintStack()
		}
		cancel(ErrDisconnected)
		c.connChange.L.Lock()
		if atomic.CompareAndSwapUint32((*uint32)(&c.state), StateConnected, StateConnectionError) {
			c.connChange.Broadcast()
		}
		c.connChange.L.Unlock()
		conn.Close()
		c.handleMsgWg.Done()
	}()

	controlHandler := wsutil.ControlFrameHandler(conn, c.side)
	wsReader := wsutil.Reader{
		Source:          conn,
		State:           c.side,
		CheckUTF8:       true,
		SkipHeaderCheck: false,
		OnIntermediate:  controlHandler,
	}
	readDataInto := func(dst []byte, s ws.State, want ws.OpCode) ([]byte, error) {
		dst = dst[:0]
		for {
			hdr, err := wsReader.NextFrame()
			if err != nil {
				return nil, err
			}
			if hdr.OpCode.IsControl() {
				if err := controlHandler(hdr, &wsReader); err != nil {
					return nil, err
				}
				continue
			}
			if hdr.OpCode&want == 0 {
				if err := wsReader.Discard(); err != nil {
					return nil, err
				}
				continue
			}
			if int64(cap(dst)) < hdr.Length+1 {
				dst = make([]byte, 0, hdr.Length+hdr.Length>>3)
			}
			if !hdr.Fin {
				hdr.Length = -1
			}
			return readAllInto(dst[:0], &wsReader, hdr.Length)
		}
	}

	// Keep reusing the same buffer.
	var msg []byte
	for atomic.LoadUint32((*uint32)(&c.state)) == StateConnected {
		if cap(msg) > readBufferSize*4 {
			// Don't keep too much memory around.
			msg = nil
		}

		var err error
		msg, err = readDataInto(msg, c.side, ws.OpBinary)
		if err != nil {
			if !xnet.IsNetworkOrHostDown(err, true) {
				gridLogIfNot(ctx, fmt.Errorf("ws read: %w", err), net.ErrClosed, io.EOF)
			}
			return
		}
		block := c.blockMessages.Load()
		if block != nil && *block != nil {
			<-*block
		}

		if c.incomingBytes != nil {
			c.incomingBytes(int64(len(msg)))
		}

		// Parse the received message
		var m message
		subID, remain, err := m.parse(msg)
		if err != nil {
			if !xnet.IsNetworkOrHostDown(err, true) {
				gridLogIf(ctx, fmt.Errorf("ws parse package: %w", err))
			}
			return
		}
		if debugPrint {
			fmt.Printf("%s Got msg: %v\n", c.Local, m)
		}
		if m.Op != OpMerged {
			c.inMessages.Add(1)
			c.handleMsg(ctx, m, subID)
			continue
		}
		// Handle merged messages.
		messages := int(m.Seq)
		c.inMessages.Add(int64(messages))
		for range messages {
			if atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
				return
			}
			var next []byte
			next, remain, err = msgp.ReadBytesZC(remain)
			if err != nil {
				if !xnet.IsNetworkOrHostDown(err, true) {
					gridLogIf(ctx, fmt.Errorf("ws read merged: %w", err))
				}
				return
			}

			m.Payload = nil
			subID, _, err = m.parse(next)
			if err != nil {
				if !xnet.IsNetworkOrHostDown(err, true) {
					gridLogIf(ctx, fmt.Errorf("ws parse merged: %w", err))
				}
				return
			}
			c.handleMsg(ctx, m, subID)
		}
	}
}

// writeStream handles the read side of the connection.
// It will grab messages from c.outQueue and write them to the connection.
// If an error occurs the cancel function will be called and conn be closed.
// The function will block until the connection is closed or an error occurs.
func (c *Connection) writeStream(ctx context.Context, conn net.Conn, cancel context.CancelCauseFunc) {
	defer func() {
		if rec := recover(); rec != nil {
			gridLogIf(ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
			debug.PrintStack()
		}
		if debugPrint {
			fmt.Println("handleMessages: write goroutine exited")
		}
		cancel(ErrDisconnected)
		c.connChange.L.Lock()
		if atomic.CompareAndSwapUint32((*uint32)(&c.state), StateConnected, StateConnectionError) {
			c.connChange.Broadcast()
		}
		c.disconnected()
		c.connChange.L.Unlock()

		conn.Close()
		c.handleMsgWg.Done()
	}()

	c.connMu.Lock()
	connPingInterval := c.connPingInterval
	c.connMu.Unlock()
	ping := time.NewTicker(connPingInterval)
	pingFrame := message{
		Op:         OpPing,
		DeadlineMS: uint32(connPingInterval.Milliseconds()),
		Payload:    make([]byte, pingMsg{}.Msgsize()),
	}

	defer ping.Stop()
	queue := make([][]byte, 0, maxMergeMessages)
	var queueSize int
	var buf bytes.Buffer
	var wsw wsWriter
	var lastSetDeadline time.Time

	// Helper to write everything in buf.
	// Return false if an error occurred and the connection is unusable.
	// Buffer will be reset empty when returning successfully.
	writeBuffer := func() (ok bool) {
		now := time.Now()
		// Only set write deadline once every second
		if now.Sub(lastSetDeadline) > time.Second {
			err := conn.SetWriteDeadline(now.Add(connWriteTimeout + time.Second))
			if err != nil {
				gridLogIf(ctx, fmt.Errorf("conn.SetWriteDeadline: %w", err))
				return false
			}
			lastSetDeadline = now
		}

		_, err := buf.WriteTo(conn)
		if err != nil {
			if !xnet.IsNetworkOrHostDown(err, true) {
				gridLogIf(ctx, fmt.Errorf("ws write: %w", err))
			}
			return false
		}
		if buf.Cap() > writeBufferSize*8 {
			// Reset buffer if it gets too big, so we don't keep it around.
			buf = bytes.Buffer{}
		}
		buf.Reset()
		return true
	}

	// Merge buffer to keep between calls
	merged := make([]byte, 0, writeBufferSize)
	for {
		var toSend []byte
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if c.State() != StateConnected {
				continue
			}
			lastPong := atomic.LoadInt64(&c.LastPong)
			if lastPong > 0 {
				lastPongTime := time.Unix(0, lastPong)
				if d := time.Since(lastPongTime); d > connPingInterval*2 {
					gridLogIf(ctx, fmt.Errorf("host %s last pong too old (%v); disconnecting", c.Remote, d.Round(time.Millisecond)))
					return
				}
			}
			ping := pingMsg{
				T: time.Now(),
			}
			var err error
			if pingFrame.Payload, err = ping.MarshalMsg(pingFrame.Payload[:0]); err != nil {
				gridLogIf(ctx, err) // Fake it... Though this should never fail.
				atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())
				continue
			}
			toSend, err = pingFrame.MarshalMsg(GetByteBuffer()[:0])
			if err != nil {
				gridLogIf(ctx, err) // Fake it... Though this should never fail.
				atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())
				continue
			}
		case toSend = <-c.outQueue:
			if len(toSend) == 0 {
				continue
			}
		}
		if len(queue) < maxMergeMessages && queueSize+len(toSend) < writeBufferSize-1024 {
			if len(c.outQueue) == 0 {
				// Yield to allow more messages to fill.
				runtime.Gosched()
			}
			if len(c.outQueue) > 0 {
				queue = append(queue, toSend)
				queueSize += len(toSend)
				continue
			}
		}
		c.outMessages.Add(int64(len(queue) + 1))
		if c.outgoingBytes != nil {
			c.outgoingBytes(int64(len(toSend) + queueSize))
		}

		c.connChange.L.Lock()
		for {
			state := c.State()
			if state == StateConnected {
				break
			}
			if debugPrint {
				fmt.Println(c.Local, "Waiting for connection ->", c.Remote, "state: ", state)
			}
			if state == StateShutdown || state == StateConnectionError {
				c.connChange.L.Unlock()
				return
			}
			c.connChange.Wait()
			select {
			case <-ctx.Done():
				c.connChange.L.Unlock()
				return
			default:
			}
		}
		c.connChange.L.Unlock()
		if len(queue) == 0 {
			// Send single message without merging.
			err := wsw.writeMessage(&buf, c.side, ws.OpBinary, toSend)
			if err != nil {
				if !xnet.IsNetworkOrHostDown(err, true) {
					gridLogIf(ctx, fmt.Errorf("ws writeMessage: %w", err))
				}
				return
			}
			PutByteBuffer(toSend)

			if !writeBuffer() {
				return
			}
			continue
		}

		// Merge entries and send
		queue = append(queue, toSend)
		if debugPrint {
			fmt.Println("Merging", len(queue), "messages")
		}

		merged = merged[:0]
		m := message{Op: OpMerged, Seq: uint32(len(queue))}
		var err error
		merged, err = m.MarshalMsg(merged)
		if err != nil {
			gridLogIf(ctx, fmt.Errorf("msg.MarshalMsg: %w", err))
			return
		}
		// Append as byte slices.
		for _, q := range queue {
			merged = msgp.AppendBytes(merged, q)
			PutByteBuffer(q)
		}
		queue = queue[:0]
		queueSize = 0

		// Combine writes.
		// Consider avoiding buffer copy.
		err = wsw.writeMessage(&buf, c.side, ws.OpBinary, merged)
		if err != nil {
			if !xnet.IsNetworkOrHostDown(err, true) {
				gridLogIf(ctx, fmt.Errorf("ws writeMessage: %w", err))
			}
			return
		}
		if cap(merged) > writeBufferSize*8 {
			// If we had to send an excessively large package, reset size.
			merged = make([]byte, 0, writeBufferSize)
		}
		if !writeBuffer() {
			return
		}
	}
}

func (c *Connection) handleMsg(ctx context.Context, m message, subID *subHandlerID) {
	switch m.Op {
	case OpMuxServerMsg:
		c.handleMuxServerMsg(ctx, m)
	case OpResponse:
		c.handleResponse(m)
	case OpMuxClientMsg:
		c.handleMuxClientMsg(ctx, m)
	case OpUnblockSrvMux:
		c.handleUnblockSrvMux(m)
	case OpUnblockClMux:
		c.handleUnblockClMux(m)
	case OpDisconnectServerMux:
		c.handleDisconnectServerMux(m)
	case OpDisconnectClientMux:
		c.handleDisconnectClientMux(m)
	case OpPing:
		c.handlePing(ctx, m)
	case OpPong:
		c.handlePong(ctx, m)
	case OpRequest:
		c.handleRequest(ctx, m, subID)
	case OpAckMux:
		c.handleAckMux(ctx, m)
	case OpConnectMux:
		c.handleConnectMux(ctx, m, subID)
	case OpMuxConnectError:
		c.handleConnectMuxError(ctx, m)
	default:
		gridLogIf(ctx, fmt.Errorf("unknown message type: %v", m.Op))
	}
}

func (c *Connection) handleConnectMux(ctx context.Context, m message, subID *subHandlerID) {
	// Stateless stream:
	if m.Flags&FlagStateless != 0 {
		// Reject for now, so we can safely add it later.
		if true {
			gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Stateless streams not supported"}))
			return
		}

		var handler *StatelessHandler
		if subID == nil {
			handler = c.handlers.stateless[m.Handler]
		} else {
			handler = c.handlers.subStateless[*subID]
		}
		if handler == nil {
			msg := fmt.Sprintf("Invalid Handler for type: %v", m.Handler)
			if subID != nil {
				msg = fmt.Sprintf("Invalid Handler for type: %v", *subID)
			}
			gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: msg}))
			return
		}
		_, _ = c.inStream.LoadOrCompute(m.MuxID, func() *muxServer {
			return newMuxStateless(ctx, m, c, *handler)
		})
	} else {
		// Stream:
		var handler *StreamHandler
		if subID == nil {
			if !m.Handler.valid() {
				gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
				return
			}
			handler = c.handlers.streams[m.Handler]
		} else {
			handler = c.handlers.subStreams[*subID]
		}
		if handler == nil {
			msg := fmt.Sprintf("Invalid Handler for type: %v", m.Handler)
			if subID != nil {
				msg = fmt.Sprintf("Invalid Handler for type: %v", *subID)
			}
			gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: msg}))
			return
		}

		// Start a new server handler if none exists.
		_, _ = c.inStream.LoadOrCompute(m.MuxID, func() *muxServer {
			return newMuxStream(ctx, m, c, *handler)
		})
	}
}

// handleConnectMuxError when mux connect was rejected.
func (c *Connection) handleConnectMuxError(ctx context.Context, m message) {
	if v, ok := c.outgoing.Load(m.MuxID); ok {
		var cErr muxConnectError
		_, err := cErr.UnmarshalMsg(m.Payload)
		gridLogIf(ctx, err)
		v.error(RemoteErr(cErr.Error))
		return
	}
	PutByteBuffer(m.Payload)
}

func (c *Connection) handleAckMux(ctx context.Context, m message) {
	PutByteBuffer(m.Payload)
	v, ok := c.outgoing.Load(m.MuxID)
	if !ok {
		if m.Flags&FlagEOF == 0 {
			gridLogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
		}
		return
	}
	if debugPrint {
		fmt.Println(c.Local, "Mux", m.MuxID, "Acknowledged")
	}
	v.ack(m.Seq)
}

func (c *Connection) handleRequest(ctx context.Context, m message, subID *subHandlerID) {
	if !m.Handler.valid() {
		gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
		return
	}
	if debugReqs {
		fmt.Println(m.MuxID, c.StringReverse(), "INCOMING")
	}
	// Singleshot message
	var handler SingleHandlerFn
	if subID == nil {
		handler = c.handlers.single[m.Handler]
	} else {
		handler = c.handlers.subSingle[*subID]
	}
	if handler == nil {
		msg := fmt.Sprintf("Invalid Handler for type: %v", m.Handler)
		if subID != nil {
			msg = fmt.Sprintf("Invalid Handler for type: %v", *subID)
		}
		gridLogIf(ctx, c.queueMsg(m, muxConnectError{Error: msg}))
		return
	}

	// TODO: This causes allocations, but escape analysis doesn't really show the cause.
	// If another faithful engineer wants to take a stab, feel free.
	go func(m message) {
		var start time.Time
		if m.DeadlineMS > 0 {
			start = time.Now()
		}
		var b []byte
		var err *RemoteErr
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					err = NewRemoteErrString(fmt.Sprintf("handleMessages: panic recovered: %v", rec))
					debug.PrintStack()
					gridLogIf(ctx, err)
				}
			}()
			b, err = handler(m.Payload)
			if debugPrint {
				fmt.Println(c.Local, "Handler returned payload:", bytesOrLength(b), "err:", err)
			}
		}()

		if m.DeadlineMS > 0 && time.Since(start).Milliseconds()+c.addDeadline.Milliseconds() > int64(m.DeadlineMS) {
			if debugReqs {
				fmt.Println(m.MuxID, c.StringReverse(), "DEADLINE EXCEEDED")
			}
			// No need to return result
			PutByteBuffer(b)
			return
		}
		if debugReqs {
			fmt.Println(m.MuxID, c.StringReverse(), "RESPONDING")
		}
		m = message{
			MuxID: m.MuxID,
			Seq:   m.Seq,
			Op:    OpResponse,
			Flags: FlagEOF,
		}
		if err != nil {
			m.Flags |= FlagPayloadIsErr
			m.Payload = []byte(*err)
		} else {
			m.Payload = b
			m.setZeroPayloadFlag()
		}
		gridLogIf(ctx, c.queueMsg(m, nil))
	}(m)
}

func (c *Connection) handlePong(ctx context.Context, m message) {
	var pong pongMsg
	_, err := pong.UnmarshalMsg(m.Payload)
	PutByteBuffer(m.Payload)
	m.Payload = nil

	if m.MuxID == 0 {
		atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())
		c.lastPingDur.Store(int64(time.Since(pong.T)))
		return
	}
	gridLogIf(ctx, err)
	if m.MuxID == 0 {
		atomic.StoreInt64(&c.LastPong, time.Now().UnixNano())
		return
	}
	if v, ok := c.outgoing.Load(m.MuxID); ok {
		v.pong(pong)
	} else {
		// We don't care if the client was removed in the meantime,
		// but we send a disconnect message to the server just in case.
		gridLogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
	}
}

func (c *Connection) handlePing(ctx context.Context, m message) {
	var ping pingMsg
	if len(m.Payload) > 0 {
		_, err := ping.UnmarshalMsg(m.Payload)
		if err != nil {
			gridLogIf(ctx, err)
		}
	}
	// c.queueMsg will reuse m.Payload

	if m.MuxID == 0 {
		gridLogIf(ctx, c.queueMsg(m, &pongMsg{T: ping.T}))
		return
	}
	// Single calls do not support pinging.
	if v, ok := c.inStream.Load(m.MuxID); ok {
		pong := v.ping(m.Seq)
		pong.T = ping.T
		gridLogIf(ctx, c.queueMsg(m, &pong))
	} else {
		pong := pongMsg{NotFound: true, T: ping.T}
		gridLogIf(ctx, c.queueMsg(m, &pong))
	}
}

func (c *Connection) handleDisconnectClientMux(m message) {
	if v, ok := c.outgoing.Load(m.MuxID); ok {
		if m.Flags&FlagPayloadIsErr != 0 {
			v.error(RemoteErr(m.Payload))
		} else {
			v.error(ErrDisconnected)
		}
		return
	}
	PutByteBuffer(m.Payload)
}

func (c *Connection) handleDisconnectServerMux(m message) {
	if debugPrint {
		fmt.Println(c.Local, "Disconnect server mux:", m.MuxID)
	}
	PutByteBuffer(m.Payload)
	m.Payload = nil
	if v, ok := c.inStream.Load(m.MuxID); ok {
		v.close()
	}
}

func (c *Connection) handleUnblockClMux(m message) {
	PutByteBuffer(m.Payload)
	m.Payload = nil
	v, ok := c.outgoing.Load(m.MuxID)
	if !ok {
		if debugPrint {
			fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
		}
		// We can expect to receive unblocks for closed muxes
		return
	}
	v.unblockSend(m.Seq)
}

func (c *Connection) handleUnblockSrvMux(m message) {
	if m.Payload != nil {
		PutByteBuffer(m.Payload)
	}
	m.Payload = nil
	if v, ok := c.inStream.Load(m.MuxID); ok {
		v.unblockSend(m.Seq)
		return
	}
	// We can expect to receive unblocks for closed muxes
	if debugPrint {
		fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
	}
}

func (c *Connection) handleMuxClientMsg(ctx context.Context, m message) {
	v, ok := c.inStream.Load(m.MuxID)
	if !ok {
		if debugPrint {
			fmt.Println(c.Local, "OpMuxClientMsg: Unknown Mux:", m.MuxID)
		}
		gridLogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
		PutByteBuffer(m.Payload)
		return
	}
	v.message(m)
}

func (c *Connection) handleResponse(m message) {
	if debugPrint {
		fmt.Printf("%s Got mux response: %v\n", c.Local, m)
	}
	v, ok := c.outgoing.Load(m.MuxID)
	if !ok {
		if debugReqs {
			fmt.Println(m.MuxID, c.String(), "Got response for unknown mux")
		}
		PutByteBuffer(m.Payload)
		return
	}
	if m.Flags&FlagPayloadIsErr != 0 {
		v.response(m.Seq, Response{
			Msg: nil,
			Err: RemoteErr(m.Payload),
		})
		PutByteBuffer(m.Payload)
	} else {
		v.response(m.Seq, Response{
			Msg: m.Payload,
			Err: nil,
		})
	}
	v.close()
	if debugReqs {
		fmt.Println(m.MuxID, c.String(), "handleResponse: closing mux")
	}
}

func (c *Connection) handleMuxServerMsg(ctx context.Context, m message) {
	if debugPrint {
		fmt.Printf("%s Got mux msg: %v\n", c.Local, m)
	}
	v, ok := c.outgoing.Load(m.MuxID)
	if !ok {
		if m.Flags&FlagEOF == 0 {
			gridLogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
		}
		PutByteBuffer(m.Payload)
		return
	}
	if m.Flags&FlagPayloadIsErr != 0 {
		v.response(m.Seq, Response{
			Msg: nil,
			Err: RemoteErr(m.Payload),
		})
		if v.cancelFn != nil {
			v.cancelFn(RemoteErr(m.Payload))
		}
		PutByteBuffer(m.Payload)
		v.close()
		c.outgoing.Delete(m.MuxID)
		return
	}
	// Return payload.
	if m.Payload != nil {
		v.response(m.Seq, Response{
			Msg: m.Payload,
			Err: nil,
		})
	}
	// Close when EOF.
	if m.Flags&FlagEOF != 0 {
		// We must obtain the lock before closing
		// Otherwise others may pick up the error before close is called.
		v.respMu.Lock()
		v.closeLocked()
		v.respMu.Unlock()
		if debugReqs {
			fmt.Println(m.MuxID, c.String(), "handleMuxServerMsg: DELETING MUX")
		}
		c.outgoing.Delete(m.MuxID)
	}
}

func (c *Connection) deleteMux(incoming bool, muxID uint64) {
	if incoming {
		if debugPrint {
			fmt.Println("deleteMux: disconnect incoming mux", muxID)
		}
		v, loaded := c.inStream.LoadAndDelete(muxID)
		if loaded && v != nil {
			gridLogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: muxID}, nil))
			v.close()
		}
	} else {
		if debugPrint {
			fmt.Println("deleteMux: disconnect outgoing mux", muxID)
		}
		v, loaded := c.outgoing.LoadAndDelete(muxID)
		if loaded && v != nil {
			if debugReqs {
				fmt.Println(muxID, c.String(), "deleteMux: DELETING MUX")
			}
			v.close()
			gridLogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectServerMux, MuxID: muxID}, nil))
		}
	}
}

// State returns the current connection status.
func (c *Connection) State() State {
	return State(atomic.LoadUint32((*uint32)(&c.state)))
}

// Stats returns the current connection stats.
func (c *Connection) Stats() madmin.RPCMetrics {
	conn := 0
	if c.State() == StateConnected {
		conn++
	}
	var lastConn time.Time
	if t := c.lastConnect.Load(); t != nil {
		lastConn = *t
	}
	pingMS := float64(c.lastPingDur.Load()) / float64(time.Millisecond)
	m := madmin.RPCMetrics{
		CollectedAt:      time.Now(),
		Connected:        conn,
		Disconnected:     1 - conn,
		IncomingStreams:  c.inStream.Size(),
		OutgoingStreams:  c.outgoing.Size(),
		IncomingBytes:    c.inBytes.Load(),
		OutgoingBytes:    c.outBytes.Load(),
		IncomingMessages: c.inMessages.Load(),
		OutgoingMessages: c.outMessages.Load(),
		OutQueue:         len(c.outQueue),
		LastPongTime:     time.Unix(0, c.LastPong).UTC(),
		LastConnectTime:  lastConn,
		ReconnectCount:   int(c.reconnects.Load()),
		LastPingMS:       pingMS,
		MaxPingDurMS:     pingMS,
	}
	m.ByDestination = map[string]madmin.RPCMetrics{
		c.Remote: m,
	}
	return m
}

func (c *Connection) debugMsg(d debugMsg, args ...any) {
	if debugPrint {
		fmt.Println("debug: sending message", d, args)
	}

	switch d {
	case debugShutdown:
		c.updateState(StateShutdown)
	case debugKillInbound:
		c.connMu.Lock()
		defer c.connMu.Unlock()
		if c.debugInConn != nil {
			if debugPrint {
				fmt.Println("debug: closing inbound connection")
			}
			c.debugInConn.Close()
		}
	case debugKillOutbound:
		c.connMu.Lock()
		defer c.connMu.Unlock()
		if c.debugInConn != nil {
			if debugPrint {
				fmt.Println("debug: closing outgoing connection")
			}
			c.debugInConn.Close()
		}
	case debugWaitForExit:
		c.reconnectMu.Lock()
		c.handleMsgWg.Wait()
		c.reconnectMu.Unlock()
	case debugSetConnPingDuration:
		c.connMu.Lock()
		defer c.connMu.Unlock()
		c.connPingInterval, _ = args[0].(time.Duration)
		if c.connPingInterval < time.Second {
			panic("CONN ping interval too low")
		}
	case debugSetClientPingDuration:
		c.connMu.Lock()
		defer c.connMu.Unlock()
		c.clientPingInterval, _ = args[0].(time.Duration)
	case debugAddToDeadline:
		c.addDeadline, _ = args[0].(time.Duration)
	case debugIsOutgoingClosed:
		// params: muxID uint64, isClosed func(bool)
		muxID, _ := args[0].(uint64)
		resp, _ := args[1].(func(b bool))
		mid, ok := c.outgoing.Load(muxID)
		if !ok || mid == nil {
			resp(true)
			return
		}
		mid.respMu.Lock()
		resp(mid.closed)
		mid.respMu.Unlock()
	case debugBlockInboundMessages:
		c.connMu.Lock()
		a, _ := args[0].(chan struct{})
		block := (<-chan struct{})(a)
		c.blockMessages.Store(&block)
		c.connMu.Unlock()
	}
}

// wsWriter writes websocket messages.
type wsWriter struct {
	tmp [ws.MaxHeaderSize]byte
}

// writeMessage writes a message to w without allocations.
func (ww *wsWriter) writeMessage(w io.Writer, s ws.State, op ws.OpCode, p []byte) error {
	const fin = true
	var frame ws.Frame
	if s.ClientSide() {
		// We do not need to copy the payload, since we own it.
		payload := p

		frame = ws.NewFrame(op, fin, payload)
		frame = ws.MaskFrameInPlace(frame)
	} else {
		frame = ws.NewFrame(op, fin, p)
	}

	return ww.writeFrame(w, frame)
}

// writeFrame writes frame binary representation into w.
func (ww *wsWriter) writeFrame(w io.Writer, f ws.Frame) error {
	const (
		bit0  = 0x80
		len7  = int64(125)
		len16 = int64(^(uint16(0)))
		len64 = int64(^(uint64(0)) >> 1)
	)

	bts := ww.tmp[:]
	if f.Header.Fin {
		bts[0] |= bit0
	}
	bts[0] |= f.Header.Rsv << 4
	bts[0] |= byte(f.Header.OpCode)

	var n int
	switch {
	case f.Header.Length <= len7:
		bts[1] = byte(f.Header.Length)
		n = 2

	case f.Header.Length <= len16:
		bts[1] = 126
		binary.BigEndian.PutUint16(bts[2:4], uint16(f.Header.Length))
		n = 4

	case f.Header.Length <= len64:
		bts[1] = 127
		binary.BigEndian.PutUint64(bts[2:10], uint64(f.Header.Length))
		n = 10

	default:
		return ws.ErrHeaderLengthUnexpected
	}

	if f.Header.Masked {
		bts[1] |= bit0
		n += copy(bts[n:], f.Header.Mask[:])
	}

	if _, err := w.Write(bts[:n]); err != nil {
		return err
	}

	_, err := w.Write(f.Payload)
	return err
}
