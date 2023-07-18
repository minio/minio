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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	State State

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

	handlers *handlers

	auth AuthFn
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
)

type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

const (
	defaultOutQueue    = 10000
	readBufferSize     = 4 << 10
	writeBufferSize    = 4 << 10
	defaultDialTimeout = time.Second

	// GridRoutePath is the remote path to connect to.
	GridRoutePath = "/minio/grid/v1"
)

// NewConnection will create an unconnected connection to a remote.
func NewConnection(id uuid.UUID, local, remote string, dial ContextDialer, handlers *handlers, auth AuthFn) *Connection {
	c := &Connection{
		State:      StateUnconnected,
		Remote:     remote,
		Local:      local,
		id:         id,
		ctx:        context.Background(),
		outgoing:   xsync.NewIntegerMapOfPresized[uint64, *MuxClient](1000),
		inStream:   xsync.NewIntegerMapOfPresized[uint64, *muxServer](1000),
		outQueue:   make(chan []byte, defaultOutQueue),
		dialer:     dial,
		side:       ws.StateServerSide,
		connChange: &sync.Cond{L: &sync.Mutex{}},
		handlers:   handlers,
		auth:       auth,
		header:     make(http.Header, 1),
	}

	if local == remote {
		panic("equal hosts")
	}
	c.header.Set("Authorization", "Bearer "+auth(remote+GridRoutePath))
	if c.shouldConnect() {
		c.side = ws.StateClientSide
		go c.connect()
	}
	if debugPrint {
		fmt.Println(c.Local, "->", c.Remote, "Should local connect:", c.shouldConnect(), "side:", c.side)
	}
	return c
}

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

func (c *Connection) NewMuxClient(ctx context.Context) *MuxClient {
	client := newMuxClient(ctx, atomic.AddUint64(&c.NextID, 1), c)
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, ok := c.outgoing.LoadOrStore(client.MuxID, client); client.MuxID == 0 || !ok {
			break
		}
		client.MuxID = atomic.AddUint64(&c.NextID, 1)
	}
	return client
}

func (c *Connection) Single(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	client := c.NewMuxClient(ctx)
	defer c.outgoing.Delete(client.MuxID)
	return client.roundtrip(h, req)
}

var ErrDone = errors.New("done for now")

var ErrRemoteRestart = errors.New("remote restarted")

// Stateless connects to the remote handler and return all packets sent back.
// If the remote is restarted will return ErrRemoteRestart.
// If nil will be returned remote call sent EOF or ErrDone is returned by the callback.
// If ErrDone is returned on cb nil will be returned.
func (c *Connection) Stateless(ctx context.Context, h HandlerID, req []byte, cb func([]byte) error) error {
	client := c.NewMuxClient(ctx)
	resp := make(chan Response, 10)
	client.RequestBytes(h, req, resp)

	defer c.outgoing.Delete(client.MuxID)
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

func (c *Connection) send(ctx context.Context, msg []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
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
	return c.send(context.Background(), dst)
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
		fmt.Println(c.Local, "sendMsg: Sending", len(dst), "bytes. Side:", c.side)
	}
	return wsutil.WriteMessage(conn, c.side, ws.OpBinary, dst)
}

func (c *Connection) connect() {
	c.updateState(StateConnecting)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Runs until the server is shut down.
	for {
		toDial := strings.Replace(c.Remote, "http://", "ws://", 1)
		toDial = strings.Replace(toDial, "https://", "wss://", 1)
		toDial = toDial + GridRoutePath

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
		dialStarted := time.Now()
		if debugPrint {
			fmt.Println(c.Local, "Connecting to ", toDial)
		}
		conn, br, _, err := dialer.Dial(c.ctx, toDial)
		if br != nil {
			ws.PutReader(br)
		}
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
			logger.LogIf(c.ctx, fmt.Errorf("grid: connecting to %s: %v Sleeping %v", toDial, err, sleep))
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
		go c.handleMessages(c.ctx, conn)
		c.updateState(StateConnected)
		for {
			c.connChange.L.Lock()
			c.connChange.Wait()
			newState := c.State
			c.connChange.L.Unlock()
			if newState != StateConnected {
				fmt.Println(c.Local, "Disconnected")
				// Reconnect
				break
			}
		}
	}
}

type Stream struct {
	// Responses from the remote server.
	// Channel will be closed
	Responses <-chan Response

	// Requests sent to the server.
	// If the handler is defined with 0 incoming capacity this will be nil.
	Requests chan<- []byte
}

// NewStream creates a
func (c *Connection) NewStream(ctx context.Context, h HandlerID, payload []byte) (st *Stream, err error) {
	if !h.valid() {
		return nil, ErrUnknownHandler
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

	cl := c.NewMuxClient(ctx)
	if err := cl.RequestStream(h, payload, requests, responses); err != nil {
		return nil, err
	}
	return &Stream{Responses: responses, Requests: requests}, nil
}

// WaitForConnect will block until a connection has been established or
// the context is canceled, in which case the context error is returned.
func (c *Connection) WaitForConnect(ctx context.Context) error {
	if atomic.LoadUint32((*uint32)(&c.State)) == StateConnected {
		// Happy path.
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	changed := make(chan struct{}, 1)
	go func() {
		defer close(changed)
		c.connChange.L.Lock()
		c.connChange.Wait()
		c.connChange.L.Unlock()
		select {
		case changed <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			if atomic.LoadUint32((*uint32)(&c.State)) == StateConnected {
				return nil
			}
		}
	}
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
	err = m.parse(b)
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
	c.connChange.L.Unlock()
	// We may have reads that aren't locked, so update atomically.
	atomic.StoreUint32((*uint32)(&c.State), uint32(s))
	c.connChange.Broadcast()
}

func (c *Connection) handleMessages(ctx context.Context, conn net.Conn) {
	// Read goroutine
	c.connWg.Add(2)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(c.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
			c.connChange.L.Lock()
			if atomic.CompareAndSwapUint32((*uint32)(&c.State), StateConnected, StateConnectionError) {
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
			if atomic.LoadUint32((*uint32)(&c.State)) != StateConnected {
				return
			}

			var m message
			var err error
			msg, err = readDataInto(msg, conn, c.side, ws.OpBinary)
			if err != nil {
				logger.LogIf(c.ctx, fmt.Errorf("ws read: %w", err))
				return
			}

			// Parse the received message
			err = m.parse(msg)
			if err != nil {
				logger.LogIf(c.ctx, fmt.Errorf("ws parse package: %w", err))
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
						logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: m.MuxID}, nil))
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
				} else {
					v.response(m.Seq, Response{
						Msg: m.Payload,
						Err: nil,
					})
				}
				if m.Flags&FlagEOF != 0 {
					v.close()
					c.outgoing.Delete(m.MuxID)
				}
			case OpMuxClientMsg:
				v, ok := c.inStream.Load(m.MuxID)
				if !ok {
					if debugPrint {
						fmt.Println(c.Local, "OpMuxClientMsg: Unknown Mux:", m.MuxID)
					}
					logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: m.MuxID}, nil))
					PutByteBuffer(m.Payload)
					continue
				}
				v.message(m)
				if m.Flags&FlagEOF != 0 {
					v.close()
					c.inStream.Delete(m.MuxID)
				}
			case OpUnblockSrvMux:
				PutByteBuffer(m.Payload)
				m.Payload = nil
				if v, ok := c.inStream.Load(m.MuxID); ok {
					v.unblockSend()
					continue
				}
				if debugPrint {
					fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
				}
				logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: m.MuxID}, nil))
				continue
			case OpUnblockClMux:
				PutByteBuffer(m.Payload)
				m.Payload = nil
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					if debugPrint {
						fmt.Println(c.Local, "Unblock: Unknown Mux:", m.MuxID)
					}
					logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: m.MuxID}, nil))
					continue
				}
				v.unblockSend()
				continue
			case OpDisconnectMux:
				PutByteBuffer(m.Payload)
				m.Payload = nil
				if v, ok := c.inStream.Load(m.MuxID); ok {
					v.close()
					continue
				}
			case OpPing:
				if m.MuxID == 0 {
					logger.LogIf(ctx, c.queueMsg(m, &pongMsg{}))
					continue
				}
				if v, ok := c.inStream.Load(m.MuxID); ok {
					pong := v.ping()
					logger.LogIf(ctx, c.queueMsg(m, &pong))
				} else {
					pong := pongMsg{NotFound: true}
					logger.LogIf(ctx, c.queueMsg(m, &pong))
				}
				continue
			case OpPong:
				// Broadcast when we get a pong.
				// TODO: Add automatic pings.
				c.connChange.Broadcast()
				atomic.StoreInt64(&c.LastPong, time.Now().Unix())

			case OpRequest:
				if !m.Handler.valid() {
					logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
					continue
				}
				// Singleshot message
				handler := c.handlers.single[m.Handler]
				if handler == nil {
					logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
					continue
				}
				go func(m message) {
					m.Op = OpMuxServerMsg
					b, err := handler(m.Payload)
					if err != nil {
						m.Flags |= FlagPayloadIsErr
						m.Payload = []byte(*err)
					} else {
						m.Payload = b
						if debugPrint {
							fmt.Println("returning payload:", string(b))
						}
					}
					logger.LogIf(c.ctx, c.queueMsg(m, nil))
				}(m)
				continue
			case OpAckMux:
				if debugPrint {
					fmt.Println(c.Local, "Mux", m.MuxID, "Acknowledged")
				}
				// TODO: Unblock now.
			case OpConnectMux:
				if !m.Handler.valid() {
					logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler"}))
					continue
				}
				// Stateless stream:
				if m.Flags&FlagStateless != 0 {
					handler := c.handlers.stateless[m.Handler]
					if handler == nil {
						logger.LogIf(ctx, c.queueMsg(m, muxConnectError{Error: "Invalid Handler for type"}))
						continue
					}
					_, _ = c.inStream.LoadOrCompute(m.MuxID, func() *muxServer {
						return newMuxStateless(ctx, m, c, *handler)
					})
				} else {
					// Stream:
					handler := c.handlers.streams[m.Handler]
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
				PutByteBuffer(m.Payload)
				m.Payload = nil
				logger.LogIf(ctx, c.queueMsg(m, nil))
				continue
			}
		}
	}()

	// Write goroutine.
	defer func() {
		if rec := recover(); rec != nil {
			logger.LogIf(c.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
			debug.PrintStack()
		}
		atomic.CompareAndSwapUint32((*uint32)(&c.State), StateConnected, StateConnectionError)
		conn.Close()
		c.connWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case toSend := <-c.outQueue:
			if debugPrint {
				fmt.Println("Sending", len(toSend), "bytes. Side", c.side)
			}
			if atomic.LoadUint32((*uint32)(&c.State)) != StateConnected {
				return
			}

			err := wsutil.WriteMessage(conn, c.side, ws.OpBinary, toSend)
			if err != nil {
				logger.LogIf(c.ctx, fmt.Errorf("ws write: %v", err))
				return
			}
		}
	}
}

func (c *Connection) deleteMux(incoming bool, muxID uint64) {
	if incoming {
		v, loaded := c.inStream.LoadAndDelete(muxID)
		if loaded && v != nil {
			logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: muxID}, nil))
			v.close()
		}
	} else {
		v, loaded := c.outgoing.LoadAndDelete(muxID)
		if loaded && v != nil {
			v.close()
			logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: muxID}, nil))
		}
	}
}

// readAllInto reads from r and appends to b until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because readAllInto is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func readAllInto(b []byte, r *wsutil.Reader) ([]byte, error) {
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return b, err
		}
	}
}
