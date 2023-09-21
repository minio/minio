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
	"github.com/minio/minio/internal/logger"
	"github.com/puzpuzpuz/xsync/v2"
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
	remoteID *uuid.UUID

	// Context for the server.
	ctx context.Context

	// Active mux connections.
	outgoing *xsync.MapOf[uint64, *MuxClient]

	// Incoming streams
	inStream *xsync.MapOf[uint64, *muxServer]

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State

	// Transport for outgoing connections.
	dialer ContextDialer
	header http.Header

	connWg sync.WaitGroup

	// connChange will be signaled whenever State has been updated, or at regular intervals.
	// Holding the lock allows safe reads of State, and guarantees that changes will be detected.
	connChange *sync.Cond
	handlers   *handlers

	remote             *RemoteClient
	auth               AuthFn
	clientPingInterval time.Duration
	connPingInterval   time.Duration
	tlsConfig          *tls.Config

	// For testing only
	debugInConn  net.Conn
	debugOutConn net.Conn
	connMu       sync.Mutex
}

type Subroute struct {
	*Connection
	route string
	subID subHandlerID
}

// State is a connection state.
type State uint32

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

func (c ContextDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return c(ctx, network, address)
}

const (
	defaultOutQueue    = 10000
	readBufferSize     = 4 << 10
	writeBufferSize    = 4 << 10
	defaultDialTimeout = time.Second
	connPingInterval   = 5 * time.Second
)

type connectionParams struct {
	ctx           context.Context
	id            uuid.UUID
	local, remote string
	dial          ContextDialer
	handlers      *handlers
	auth          AuthFn
	tlsConfig     *tls.Config

	debugBlockConnect chan struct{}
}

// newConnection will create an unconnected connection to a remote.
func newConnection(o connectionParams) *Connection {
	c := &Connection{
		state:              StateUnconnected,
		Remote:             o.remote,
		Local:              o.local,
		id:                 o.id,
		ctx:                o.ctx,
		outgoing:           xsync.NewIntegerMapOfPresized[uint64, *MuxClient](1000),
		inStream:           xsync.NewIntegerMapOfPresized[uint64, *muxServer](1000),
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
	}

	if o.local == o.remote {
		panic("equal hosts")
	}
	c.header.Set("Authorization", "Bearer "+o.auth(o.remote+RoutePath))
	if c.shouldConnect() {
		c.side = ws.StateClientSide

		go func() {
			if o.debugBlockConnect != nil {
				<-o.debugBlockConnect
			}
			c.connect()
		}()
	}
	if debugPrint {
		fmt.Println(c.Local, "->", c.Remote, "Should local connect:", c.shouldConnect(), "side:", c.side)
	}
	return c
}

func (c *Connection) Subroute(s string) *Subroute {
	return &Subroute{
		Connection: c,
		route:      s,
		subID:      makeSubHandlerID(0, s),
	}
}

func (c *Subroute) Subroute(_ string) *Subroute {
	panic("grid: subroutes cannot be nested")
}

// NewMuxClient returns a mux client for manual use.
func (c *Connection) NewMuxClient(ctx context.Context) (*MuxClient, error) {
	client := newMuxClient(ctx, atomic.AddUint64(&c.NextID, 1), c)
	if dl, ok := ctx.Deadline(); ok {
		client.deadline = getDeadline(time.Until(dl))
		if client.deadline == 0 {
			return nil, context.DeadlineExceeded
		}
	}
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, ok := c.outgoing.LoadOrStore(client.MuxID, client); client.MuxID == 0 || !ok {
			break
		}
		client.MuxID = atomic.AddUint64(&c.NextID, 1)
	}
	return client, nil
}

// NewMuxClient returns a mux client for manual use.
func (c *Subroute) NewMuxClient(ctx context.Context) (*MuxClient, error) {
	cl, err := c.Connection.NewMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	cl.subroute = &c.subID
	return cl, nil
}

// Request allows to do a single remote request.
func (c *Connection) Request(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	handler := c.handlers.single[h]
	if handler == nil {
		return nil, ErrUnknownHandler
	}
	client, err := c.NewMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	defer c.outgoing.Delete(client.MuxID)
	return client.roundtrip(h, req)
}

// Request allows to do a single remote request.
func (c *Subroute) Request(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	handler := c.handlers.subSingle[makeZeroSubHandlerID(h)]
	if handler == nil {
		return nil, ErrUnknownHandler
	}
	client, err := c.NewMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	client.subroute = &c.subID
	defer c.outgoing.Delete(client.MuxID)
	return client.roundtrip(h, req)
}

// NewStream creates a new stream
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

	cl, err := c.NewMuxClient(ctx)
	if err != nil {
		return nil, err
	}

	return cl.RequestStream(h, payload, requests, responses)
}

func (c *Subroute) NewStream(ctx context.Context, h HandlerID, payload []byte) (st *Stream, err error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
	}
	if c.State() != StateConnected {
		return nil, ErrDisconnected
	}
	handler := c.handlers.subStreams[makeZeroSubHandlerID(h)]
	if handler == nil {
		fmt.Println("want", makeZeroSubHandlerID(h), c.route, "got", c.handlers.subStreams)
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

	cl, err := c.NewMuxClient(ctx)
	if err != nil {
		return nil, err
	}
	cl.subroute = &c.subID

	return cl.RequestStream(h, payload, requests, responses)
}

// WaitForConnect will block until a connection has been established or
// the context is canceled, in which case the context error is returned.
func (c *Connection) WaitForConnect(ctx context.Context) error {
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
		defer close(changed)
		for {
			c.connChange.Wait()
			newState := c.state
			select {
			case changed <- newState:
				if newState == StateConnected {
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
			return ctx.Err()
		case newState := <-changed:
			if newState == StateConnected {
				return nil
			}
		}
	}
}

// ErrDisconnected is returned when the connection to the remote has been lost during the call.
var ErrDisconnected = errors.New("remote disconnected")

/*
var ErrDone = errors.New("done for now")

var ErrRemoteRestart = errors.New("remote restarted")


// Stateless connects to the remote handler and return all packets sent back.
// If the remote is restarted will return ErrRemoteRestart.
// If nil will be returned remote call sent EOF or ErrDone is returned by the callback.
// If ErrDone is returned on cb nil will be returned.
func (c *Connection) Stateless(ctx context.Context, h HandlerID, req []byte, cb func([]byte) error) error {
	client, err := c.NewMuxClient(ctx)
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
func (c *Connection) shouldConnect() bool {
	// We xor the two hashes, so result isn't order dependent.
	h0 := xxh3.HashString(c.Local)
	h1 := xxh3.HashString(c.Remote)
	if h0 == h1 {
		// Tiebreak on unlikely tie.
		return c.Local < c.Remote
	}
	return h0 < h1
}

func (c *Connection) send(msg []byte) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case c.outQueue <- msg:
		return nil
	}
}

// queueMsg queues a message, with an optional payload.
// sender should not reference msg.Payload
func (c *Connection) queueMsg(msg message, payload sender) error {
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
	return wsutil.WriteMessage(conn, c.side, ws.OpBinary, dst)
}

func (c *Connection) connect() {
	c.updateState(StateConnecting)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Runs until the server is shut down.
	for {
		if atomic.LoadUint32((*uint32)(&c.state)) == StateShutdown {
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
			gotState := atomic.LoadUint32((*uint32)(&c.state))
			if gotState == StateShutdown {
				return
			}
			if gotState != StateConnecting {
				// Don't print error on first attempt.
				logger.LogIf(c.ctx, fmt.Errorf("grid: connecting to %s: %w (%T) Sleeping %v (%v)", toDial, err, err, sleep, gotState))
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
		remoteUUID := uuid.UUID(r.ID)
		if c.remoteID != nil && remoteUUID != *c.remoteID {
			c.outgoing.Range(func(key uint64, client *MuxClient) bool {
				client.close()
				return true
			})
			c.inStream.Range(func(key uint64, value *muxServer) bool {
				value.close()
				return true
			})
			c.inStream.Clear()
			c.outgoing.Clear()
		}
		c.remoteID = &remoteUUID
		if debugPrint {
			fmt.Println(c.Local, "Connected Waiting for Messages")
		}
		c.updateState(StateConnected)
		go c.handleMessages(c.ctx, conn)
		c.connChange.L.Lock()
		for {
			c.connChange.Wait()
			newState := atomic.LoadUint32((*uint32)(&c.state))
			if newState != StateConnected {
				c.connChange.L.Unlock()
				if newState == StateShutdown {
					conn.Close()
					return
				}
				fmt.Println(c.Local, "Disconnected")
				// Reconnect
				break
			}
		}
	}
}

func (c *Connection) disconnected() {
	c.outgoing.Range(func(key uint64, client *MuxClient) bool {
		if !client.stateless {
			client.cancelFn(ErrDisconnected)
		}
		return true
	})
}

func (c *Connection) receive(conn net.Conn, r receiver) error {
	b, op, err := wsutil.ReadData(conn, c.side)
	if err != nil {
		return err
	}
	if op != ws.OpBinary {
		return fmt.Errorf("unexpected connect response type %v", op)
	}
	var m message
	_, err = m.parse(b)
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
		return errors.New("expected to be client side, not server side")
	}
	msg := message{
		Op: OpConnectResponse,
	}

	if c.remoteID != nil && *c.remoteID != req.ID {
		c.updateState(StateConnectionError)
		// Remote ID changed.
		// Close all active requests.
		c.outgoing.Range(func(key uint64, client *MuxClient) bool {
			client.close()
			return true
		})
	}
	rid := uuid.UUID(req.ID)
	c.remoteID = &rid
	resp := connectResp{
		ID:       c.id,
		Accepted: true,
	}
	err := c.sendMsg(conn, msg, &resp)
	if debugPrint {
		fmt.Printf("grid: Queued Response %+v Side: %v\n", resp, c.side)
	}
	if err == nil {
		c.connWg.Wait()
		c.updateState(StateConnected)
		c.handleMessages(ctx, conn)
	}
	return err
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

func (c *Connection) handleMessages(ctx context.Context, conn net.Conn) {
	// Read goroutine
	c.connWg.Add(2)
	ctx, cancel := context.WithCancelCause(ctx)
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
			c.connWg.Done()
		}()

		controlHandler := wsutil.ControlFrameHandler(conn, c.side)
		readDataInto := func(dst []byte, rw io.ReadWriter, s ws.State, want ws.OpCode) ([]byte, error) {
			dst = dst[:0]
			rd := wsutil.Reader{
				Source:          conn,
				State:           c.side,
				CheckUTF8:       true,
				SkipHeaderCheck: false,
				OnIntermediate:  controlHandler,
			}
			for {
				hdr, err := rd.NextFrame()
				if err != nil {
					return nil, err
				}
				if hdr.OpCode.IsControl() {
					if err := controlHandler(hdr, &rd); err != nil {
						return nil, err
					}
					continue
				}
				if hdr.OpCode&want == 0 {
					if err := rd.Discard(); err != nil {
						return nil, err
					}
					continue
				}

				if int64(cap(dst)) < hdr.Length+1 {
					dst = make([]byte, 0, hdr.Length+hdr.Length>>3)
				}
				return readAllInto(dst[:0], &rd)
			}
		}

		// Keep reusing the same buffer.
		var msg []byte
		for {
			if atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
				cancel(ErrDisconnected)
				return
			}

			var m message
			var err error
			msg, err = readDataInto(msg, conn, c.side, ws.OpBinary)
			if err != nil {
				cancel(ErrDisconnected)
				logger.LogIfNot(ctx, fmt.Errorf("ws read: %w", err), net.ErrClosed, io.EOF)
				return
			}

			// Parse the received message
			subID, err := m.parse(msg)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("ws parse package: %w", err))
				cancel(ErrDisconnected)
				return
			}
			if debugPrint {
				fmt.Printf("%s Got msg: %+v\n", c.Local, m)
			}
			switch m.Op {
			case OpMuxServerMsg:
				if debugPrint {
					fmt.Printf("%s Got mux msg: %+v\n", c.Local, m)
				}
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					if m.Flags&FlagEOF == 0 {
						logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
					}
					PutByteBuffer(m.Payload)
					continue
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
					c.outgoing.Delete(m.MuxID)
				}
			case OpResponse:
				if debugPrint {
					fmt.Printf("%s Got mux response: %+v\n", c.Local, m)
				}
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					PutByteBuffer(m.Payload)
					continue
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
				c.outgoing.Delete(m.MuxID)
			case OpMuxClientMsg:
				v, ok := c.inStream.Load(m.MuxID)
				if !ok {
					if debugPrint {
						fmt.Println(c.Local, "OpMuxClientMsg: Unknown Mux:", m.MuxID)
					}
					logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
					PutByteBuffer(m.Payload)
					continue
				}
				v.message(m)
			case OpUnblockSrvMux:
				PutByteBuffer(m.Payload)
				m.Payload = nil
				if v, ok := c.inStream.Load(m.MuxID); ok {
					v.unblockSend(m.Seq)
					continue
				}
				if debugPrint {
					fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
				}
				// We can expect to receive unblocks for closed muxes
				continue
			case OpUnblockClMux:
				PutByteBuffer(m.Payload)
				m.Payload = nil
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					if debugPrint {
						fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
					}
					// We can expect to receive unblocks for closed muxes
					continue
				}
				v.unblockSend(m.Seq)
				continue
			case OpDisconnectServerMux:
				if debugPrint {
					fmt.Println(c.Local, "Disconnect server mux:", m.MuxID)
				}
				PutByteBuffer(m.Payload)
				m.Payload = nil
				if v, ok := c.inStream.Load(m.MuxID); ok {
					v.close()
					continue
				}
			case OpDisconnectClientMux:
				if v, ok := c.outgoing.Load(m.MuxID); ok {
					if m.Flags&FlagPayloadIsErr != 0 {
						v.error(RemoteErr(m.Payload))
					} else {
						v.error("remote disconnected")
					}
					continue
				}
				PutByteBuffer(m.Payload)
			case OpPing:
				PutByteBuffer(m.Payload)
				if m.MuxID == 0 {
					logger.LogIf(ctx, c.queueMsg(m, &pongMsg{}))
					continue
				}
				// Single calls do not support pinging.
				if v, ok := c.inStream.Load(m.MuxID); ok {
					pong := v.ping()
					logger.LogIf(ctx, c.queueMsg(m, &pong))
				} else {
					pong := pongMsg{NotFound: true}
					logger.LogIf(ctx, c.queueMsg(m, &pong))
				}
				continue
			case OpPong:
				var pong pongMsg
				_, err := pong.UnmarshalMsg(m.Payload)
				PutByteBuffer(m.Payload)
				logger.LogIf(ctx, err)
				if m.MuxID == 0 {
					atomic.StoreInt64(&c.LastPong, time.Now().Unix())
				} else {
					if v, ok := c.outgoing.Load(m.MuxID); ok {
						v.pong(pong)
					} else {
						// We don't care if the client was removed in the meantime,
						// but we send a disconnect message to the server just in case.
						logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
					}
				}
			case OpRequest:
				if !m.Handler.valid() {
					logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
					continue
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
					continue
				}
				go func(m message) {
					var start time.Time
					if m.DeadlineMS > 0 {
						start = time.Now()
					}
					b, err := handler(m.Payload)
					// TODO: Maybe recycle m.Payload - should be free here.
					if m.DeadlineMS > 0 && time.Since(start).Milliseconds() > int64(m.DeadlineMS) {
						// No need to return result
						PutByteBuffer(b)
						return
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

						if debugPrint {
							fmt.Println("returning payload:", string(b))
						}
					}
					m.Flags = 0
					logger.LogIf(ctx, c.queueMsg(m, nil))
				}(m)
				continue
			case OpAckMux:
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					if m.Flags&FlagEOF == 0 {
						logger.LogIf(ctx, c.queueMsg(message{Op: OpDisconnectClientMux, MuxID: m.MuxID}, nil))
					}
					PutByteBuffer(m.Payload)
					continue
				}
				if debugPrint {
					fmt.Println(c.Local, "Mux", m.MuxID, "Acknowledged")
				}
				v.ack(m.Seq)
			case OpConnectMux:
				// Stateless stream:
				if m.Flags&FlagStateless != 0 {
					var handler *StatelessHandler
					if subID == nil {
						handler = c.handlers.stateless[m.Handler]
					} else {
						handler = c.handlers.subStateless[*subID]
					}
					if handler == nil {
						logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
						continue
					}
					_, _ = c.inStream.LoadOrCompute(m.MuxID, func() *muxServer {
						return newMuxStateless(ctx, m, c, *handler)
					})
				} else {
					// Stream:
					var handler *StreamHandler
					if subID == nil {
						handler = c.handlers.streams[m.Handler]
					} else {
						handler = c.handlers.subStreams[*subID]
					}
					if handler == nil {
						logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
						continue
					}

					// Start a new server handler if none exists.
					_, _ = c.inStream.LoadOrCompute(m.MuxID, func() *muxServer {
						return newMuxStream(ctx, m, c, *handler)
					})
					if debugPrint {
						fmt.Println("connected stream mux:", m.MuxID)
					}
				}
				// Acknowledge Mux created.
				m.Op = OpAckMux
				m.Flags = 0
				m.Payload = nil
				logger.LogIf(ctx, c.queueMsg(m, nil))
				continue
			}
		}
	}()

	// Write goroutine.
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
		c.connChange.L.Unlock()
		c.disconnected()

		conn.Close()
		c.connWg.Done()
	}()

	c.connMu.Lock()
	connPingInterval := c.connPingInterval
	c.connMu.Unlock()
	ping := time.NewTicker(connPingInterval)
	pingFrame := message{
		Op:         OpPing,
		DeadlineMS: 1000,
	}

	defer ping.Stop()
	for {
		var toSend []byte
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
				continue
			}
			lastPong := atomic.LoadInt64(&c.LastPong)
			if lastPong > 0 {
				lastPongTime := time.Unix(lastPong, 0)
				if time.Since(lastPongTime) > connPingInterval*2 {
					if debugPrint {
						fmt.Println(c.Local, "Last pong too old. Disconnecting")
					}
					cancel(ErrDisconnected)
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
		c.connChange.L.Lock()
		for atomic.LoadUint32((*uint32)(&c.state)) != StateConnected {
			if debugPrint {
				fmt.Println("Waiting for connection")
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

		err := wsutil.WriteMessage(conn, c.side, ws.OpBinary, toSend)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("ws write: %v", err))
			cancel(ErrDisconnected)
			return
		}
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
		c.connWg.Wait()
	case debugSetConnPingDuration:
		c.connMu.Lock()
		defer c.connMu.Unlock()
		c.connPingInterval = args[0].(time.Duration)
	case debugSetClientPingDuration:
		c.clientPingInterval = args[0].(time.Duration)
	}
}
