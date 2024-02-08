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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
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
	"github.com/tinylib/msgp/msgp"
	"github.com/zeebo/xxh3"
)

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
	outgoing *lockedClientMap

	// Incoming streams
	inStream *lockedServerMap

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State

	// Transport for outgoing connections.
	dialer ContextDialer
	header http.Header

	handleMsgWg sync.WaitGroup

	// connChange will be signaled whenever State has been updated, or at regular intervals.
	// Holding the lock allows safe reads of State, and guarantees that changes will be detected.
	connChange *sync.Cond
	handlers   *handlers

	remote             *RemoteClient
	auth               AuthFn
	clientPingInterval time.Duration
	connPingInterval   time.Duration
	tlsConfig          *tls.Config
	blockConnect       chan struct{}

	incomingBytes func(n int64) // Record incoming bytes.
	outgoingBytes func(n int64) // Record outgoing bytes.
	trace         *tracer       // tracer for this connection.
	baseFlags     Flags

	// For testing only
	debugInConn  net.Conn
	debugOutConn net.Conn
	addDeadline  time.Duration
	connMu       sync.Mutex
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
	defaultOutQueue    = 10000
	readBufferSize     = 16 << 10
	writeBufferSize    = 16 << 10
	defaultDialTimeout = 2 * time.Second
	connPingInterval   = 10 * time.Second
	connWriteTimeout   = 3 * time.Second
)

type connectionParams struct {
	ctx           context.Context
	id            uuid.UUID
	local, remote string
	dial          ContextDialer
	handlers      *handlers
	auth          AuthFn
	tlsConfig     *tls.Config
	incomingBytes func(n int64) // Record incoming bytes.
	outgoingBytes func(n int64) // Record outgoing bytes.
	publisher     *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]

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
		outgoing:           &lockedClientMap{m: make(map[uint64]*muxClient, 1000)},
		inStream:           &lockedServerMap{m: make(map[uint64]*muxServer, 1000)},
		outQueue:           make(chan []byte, defaultOutQueue),
		dialer:             o.dial,
		side:               ws.StateServerSide,
		connChange:         &sync.Cond{L: &sync.Mutex{}},
		handlers:           o.handlers,
		auth:               o.auth,
		header:             make(http.Header, 1),
		remote:             &RemoteClient{Name: o.remote},
		clientPingInterval: clientPingInterval,
		connPingInterval:   connPingInterval,
		tlsConfig:          o.tlsConfig,
		incomingBytes:      o.incomingBytes,
		outgoingBytes:      o.outgoingBytes,
	}
	if debugPrint {
		// Random Mux ID
		c.NextID = rand.Uint64()
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

func (c *Connection) send(msg []byte) error {
	select {
	case <-c.ctx.Done():
		return context.Cause(c.ctx)
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
		if cap(msg.Payload) < payload.Msgsize() {
			old := msg.Payload
			msg.Payload = GetByteBuffer()[:0]
			PutByteBuffer(old)
		}
		var err error
		msg.Payload, err = payload.MarshalMsg(msg.Payload[:0])
		msg.Op = payload.Op()
		if err != nil {
			return err
		}
	}
	defer PutByteBuffer(msg.Payload)
	dst := GetByteBuffer()[:0]
	dst, err := msg.MarshalMsg(dst)
	if err != nil {
		return err
	}
	if msg.Flags&FlagCRCxxh3 != 0 {
		h := xxh3.Hash(dst)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(h))
	}
	return c.send(dst)
}

// sendMsg will send
func (c *Connection) sendMsg(conn net.Conn, msg message, payload msgp.MarshalSizer) error {
	if payload != nil {
		if cap(msg.Payload) < payload.Msgsize() {
			PutByteBuffer(msg.Payload)
			msg.Payload = GetByteBuffer()[:0]
		}
		var err error
		msg.Payload, err = payload.MarshalMsg(msg.Payload)
		if err != nil {
			return err
		}
		defer PutByteBuffer(msg.Payload)
	}
	dst := GetByteBuffer()[:0]
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
		toDial := strings.Replace(c.Remote, "http://", "ws://", 1)
		toDial = strings.Replace(toDial, "https://", "wss://", 1)
		toDial += RoutePath

		dialer := ws.DefaultDialer
		dialer.ReadBufferSize = readBufferSize
		dialer.WriteBufferSize = writeBufferSize
		dialer.Timeout = defaultDialTimeout
		if c.dialer != nil {
			dialer.NetDial = c.dialer.DialContext
		}
		if c.header == nil {
			c.header = make(http.Header, 2)
		}
		c.header.Set("Authorization", "Bearer "+c.auth(""))
		c.header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))

		if len(c.header) > 0 {
			dialer.Header = ws.HandshakeHeaderHTTP(c.header)
		}
		dialer.TLSConfig = c.tlsConfig
		dialStarted := time.Now()
		if debugPrint {
			fmt.Println(c.Local, "Connecting to ", toDial)
		}
		conn, br, _, err := dialer.Dial(c.ctx, toDial)
		if br != nil {
			ws.PutReader(br)
		}
		c.connMu.Lock()
		c.debugOutConn = conn
		c.connMu.Unlock()
		retry := func(err error) {
			if debugPrint {
				fmt.Printf("%v Connecting to %v: %v. Retrying.\n", c.Local, toDial, err)
			}
			sleep := defaultDialTimeout + time.Duration(rng.Int63n(int64(defaultDialTimeout)))
			next := dialStarted.Add(sleep)
			sleep = time.Until(next).Round(time.Millisecond)
			if sleep < 0 {
				sleep = 0
			}
			gotState := c.State()
			if gotState == StateShutdown {
				return
			}
			if gotState != StateConnecting {
				// Don't print error on first attempt,
				// and after that only once per hour.
				logger.LogOnceIf(c.ctx, fmt.Errorf("grid: %s connecting to %s: %w (%T) Sleeping %v (%v)", c.Local, toDial, err, err, sleep, gotState), toDial)
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
		}
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

	// Read goroutine
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
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
		readDataInto := func(dst []byte, rw io.ReadWriter, s ws.State, want ws.OpCode) ([]byte, error) {
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
				return readAllInto(dst[:0], &wsReader)
			}
		}

		// Keep reusing the same buffer.
		var msg []byte
		for {
			if atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
				cancel(ErrDisconnected)
				return
			}
			if cap(msg) > readBufferSize*8 {
				// Don't keep too much memory around.
				msg = nil
			}

			var err error
			msg, err = readDataInto(msg, conn, c.side, ws.OpBinary)
			if err != nil {
				cancel(ErrDisconnected)
				logger.LogIfNot(ctx, fmt.Errorf("ws read: %w", err), net.ErrClosed, io.EOF)
				return
			}
			if c.incomingBytes != nil {
				c.incomingBytes(int64(len(msg)))
			}

			// Parse the received message
			var m message
			subID, remain, err := m.parse(msg)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("ws parse package: %w", err))
				cancel(ErrDisconnected)
				return
			}
			if debugPrint {
				fmt.Printf("%s Got msg: %v\n", c.Local, m)
			}
			if m.Op != OpMerged {
				c.handleMsg(ctx, m, subID)
				continue
			}
			// Handle merged messages.
			messages := int(m.Seq)
			for i := 0; i < messages; i++ {
				if atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
					cancel(ErrDisconnected)
					return
				}
				var next []byte
				next, remain, err = msgp.ReadBytesZC(remain)
				if err != nil {
					logger.LogIf(ctx, fmt.Errorf("ws read merged: %w", err))
					cancel(ErrDisconnected)
					return
				}

				m.Payload = nil
				subID, _, err = m.parse(next)
				if err != nil {
					logger.LogIf(ctx, fmt.Errorf("ws parse merged: %w", err))
					cancel(ErrDisconnected)
					return
				}
				c.handleMsg(ctx, m, subID)
			}
		}
	}()

	// Write function.
	defer func() {
		if rec := recover(); rec != nil {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
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
		DeadlineMS: 5000,
	}

	defer ping.Stop()
	queue := make([][]byte, 0, maxMergeMessages)
	merged := make([]byte, 0, writeBufferSize)
	var queueSize int
	var buf bytes.Buffer
	var wsw wsWriter
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
				lastPongTime := time.Unix(lastPong, 0)
				if d := time.Since(lastPongTime); d > connPingInterval*2 {
					logger.LogIf(ctx, fmt.Errorf("host %s last pong too old (%v); disconnecting", c.Remote, d.Round(time.Millisecond)))
					return
				}
			}
			var err error
			toSend, err = pingFrame.MarshalMsg(GetByteBuffer()[:0])
			if err != nil {
				logger.LogIf(ctx, err)
				// Fake it...
				atomic.StoreInt64(&c.LastPong, time.Now().Unix())
				continue
			}
		case toSend = <-c.outQueue:
			if len(toSend) == 0 {
				continue
			}
		}
		if len(queue) < maxMergeMessages && queueSize+len(toSend) < writeBufferSize-1024 && len(c.outQueue) > 0 {
			queue = append(queue, toSend)
			queueSize += len(toSend)
			continue
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
			// Combine writes.
			buf.Reset()
			err := wsw.writeMessage(&buf, c.side, ws.OpBinary, toSend)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("ws writeMessage: %w", err))
				return
			}
			PutByteBuffer(toSend)
			err = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("conn.SetWriteDeadline: %w", err))
				return
			}
			_, err = buf.WriteTo(conn)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("ws write: %w", err))
				return
			}
			continue
		}

		// Merge entries and send
		queue = append(queue, toSend)
		if debugPrint {
			fmt.Println("Merging", len(queue), "messages")
		}

		toSend = merged[:0]
		m := message{Op: OpMerged, Seq: uint32(len(queue))}
		var err error
		toSend, err = m.MarshalMsg(toSend)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("msg.MarshalMsg: %w", err))
			return
		}
		// Append as byte slices.
		for _, q := range queue {
			toSend = msgp.AppendBytes(toSend, q)
			PutByteBuffer(q)
		}
		queue = queue[:0]
		queueSize = 0

		// Combine writes.
		// Consider avoiding buffer copy.
		buf.Reset()
		err = wsw.writeMessage(&buf, c.side, ws.OpBinary, toSend)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("ws writeMessage: %w", err))
			return
		}
		// buf is our local buffer, so we can reuse it.
		err = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("conn.SetWriteDeadline: %w", err))
			return
		}
		_, err = buf.WriteTo(conn)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("ws write: %w", err))
			return
		}

		if buf.Cap() > writeBufferSize*4 {
			// Reset buffer if it gets too big, so we don't keep it around.
			buf = bytes.Buffer{}
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
		logger.LogIf(ctx, fmt.Errorf("unknown message type: %v", m.Op))
	}
}

func (c *Connection) handleConnectMux(ctx context.Context, m message, subID *subHandlerID) {
	// Stateless stream:
	if m.Flags&FlagStateless != 0 {
		// Reject for now, so we can safely add it later.
		if true {
			logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Stateless streams not supported"}))
			return
		}

		var handler *StatelessHandler
		if subID == nil {
			handler = c.handlers.stateless[m.Handler]
		} else {
			handler = c.handlers.subStateless[*subID]
		}
		if handler == nil {
			logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
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
				logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
				return
			}
			handler = c.handlers.streams[m.Handler]
		} else {
			handler = c.handlers.subStreams[*subID]
		}
		if handler == nil {
			logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
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
		logger.LogIf(ctx, err)
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
			logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
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
		logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
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
		logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
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
					logger.LogIf(ctx, err)
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
		logger.LogIf(ctx, c.queueMsg(m, nil))
	}(m)
}

func (c *Connection) handlePong(ctx context.Context, m message) {
	var pong pongMsg
	_, err := pong.UnmarshalMsg(m.Payload)
	PutByteBuffer(m.Payload)
	logger.LogIf(ctx, err)
	if m.MuxID == 0 {
		atomic.StoreInt64(&c.LastPong, time.Now().Unix())
		return
	}
	if v, ok := c.outgoing.Load(m.MuxID); ok {
		v.pong(pong)
	} else {
		// We don't care if the client was removed in the meantime,
		// but we send a disconnect message to the server just in case.
		logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
	}
}

func (c *Connection) handlePing(ctx context.Context, m message) {
	if m.MuxID == 0 {
		logger.LogIf(ctx, c.queueMsg(m, &pongMsg{}))
		return
	}
	// Single calls do not support pinging.
	if v, ok := c.inStream.Load(m.MuxID); ok {
		pong := v.ping(m.Seq)
		logger.LogIf(ctx, c.queueMsg(m, &pong))
	} else {
		pong := pongMsg{NotFound: true}
		logger.LogIf(ctx, c.queueMsg(m, &pong))
	}
	return
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
		logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
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
			logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
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
	} else if m.Payload != nil {
		v.response(m.Seq, Response{
			Msg: m.Payload,
			Err: nil,
		})
	}
	if m.Flags&FlagEOF != 0 {
		v.close()
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
			logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: muxID}, nil))
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
			logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectServerMux, MuxID: muxID}, nil))
		}
	}
}

// State returns the current connection status.
func (c *Connection) State() State {
	return State(atomic.LoadUint32((*uint32)(&c.state)))
}

// Stats returns the current connection stats.
func (c *Connection) Stats() ConnectionStats {
	return ConnectionStats{
		IncomingStreams: c.inStream.Size(),
		OutgoingStreams: c.outgoing.Size(),
	}
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
		c.connPingInterval = args[0].(time.Duration)
	case debugSetClientPingDuration:
		c.clientPingInterval = args[0].(time.Duration)
	case debugAddToDeadline:
		c.addDeadline = args[0].(time.Duration)
	case debugIsOutgoingClosed:
		// params: muxID uint64, isClosed func(bool)
		muxID := args[0].(uint64)
		resp := args[1].(func(b bool))
		mid, ok := c.outgoing.Load(muxID)
		if !ok || mid == nil {
			resp(true)
			return
		}
		mid.respMu.Lock()
		resp(mid.closed)
		mid.respMu.Unlock()
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
