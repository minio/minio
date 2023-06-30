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
	// State of the connection (atomic)
	State State

	// NextID is the next ID that can be used (atomic).
	NextID uint64

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

	// Incoming stateless requests
	statefulCtx *xsync.MapOf[uint64, context.CancelFunc]
	// Incoming streams
	streamCtx *xsync.MapOf[uint64, context.CancelFunc]

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State

	// Transport for outgoing connections.
	dialer ContextDialer
	header http.Header

	connWg sync.WaitGroup

	// connChange will be signaled whenever
	connChange *sync.Cond
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
)

// NewConnection will create an unconnected connection to a remote.
func NewConnection(id uuid.UUID, local, remote string, dial ContextDialer) *Connection {
	c := Connection{
		State:      StateUnconnected,
		Remote:     remote,
		Local:      local,
		id:         id,
		ctx:        context.Background(),
		outgoing:   xsync.NewIntegerMapOfPresized[uint64, *MuxClient](1000),
		statefulCtx:   xsync.NewIntegerMapOfPresized[uint64, context.CancelFunc](1000),
		streamCtx:   xsync.NewIntegerMapOfPresized[uint64, context.CancelFunc](1000),
		outQueue:   make(chan []byte, defaultOutQueue),
		dialer:     dial,
		side:       ws.StateServerSide,
		connChange: &sync.Cond{L: &sync.Mutex{}},
	}
	if c.shouldConnect() {
		c.side = ws.StateClientSide
		go c.connect()
	}
	return &c
}

// shouldConnect returns a deterministic bool whether the local should initiate the connection.
func (c *Connection) shouldConnect() bool {
	// We xor the two hashes, so result isn't order dependent.
	return xxh3.HashString(c.Local)^xxh3.HashString(c.Remote)&1 == 1
}

func (c *Connection) NewMuxClient(ctx context.Context) *MuxClient {
	client := newMuxClient(ctx, atomic.AddUint64(&c.NextID, 1), c)
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, ok := c.outgoing.LoadOrStore(client.MuxID, client); !ok {
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
// If the remote is restarted or reports EOF the function will return ErrRemoteRestart and io.EOF respectively.
// If cb returns an error it is returned. If ErrDone is returned on cb nil will be returned.
func (c *Connection) Stateless(ctx context.Context, h HandlerID, req []byte, cb func([]byte) error) error {
	client := c.NewMuxClient(ctx)
	resp := make(chan Response, 10)
	client.RequestBytes(h, req, resp)

	defer c.outgoing.Delete(client.MuxID)
	for r := range resp {
		if r.Err != nil && !errors.Is(r.Err, io.EOF) {
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
		if errors.Is(r.Err, io.EOF) {
			break
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

func (c *Connection) queueMsg(msg message, payload msgp.MarshalSizer) error {
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
	return wsutil.WriteMessage(conn, c.side, ws.OpBinary, dst)
}

func (c *Connection) connect() {
	atomic.StoreUint32((*uint32)(&c.State), StateConnecting)
	c.connChange.Signal()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Runs until the server is shut down.
	for {
		toDial := strings.Replace(c.Remote, "http://", "ws://", 1)
		toDial = strings.Replace(toDial, "https://", "wss://", 1)
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
		conn, br, _, err := dialer.Dial(c.ctx, toDial)
		if br != nil {
			ws.PutReader(br)
		}
		retry := func(err error) {
			sleep := defaultDialTimeout + time.Duration(rng.Int63n(int64(defaultDialTimeout)))
			next := dialStarted.Add(sleep)
			sleep = time.Until(next).Round(time.Millisecond)
			if sleep < 0 {
				sleep = 0
			}
			logger.LogIf(c.ctx, fmt.Errorf("grid: connecting to %s: %v Sleeping %v", toDial, err, sleep))
			atomic.StoreUint32((*uint32)(&c.State), StateConnectionError)
			c.connChange.Signal()
			time.Sleep(sleep)
		}
		if err != nil {
			retry(err)
			continue
		}
		m := message{
			Op: OpConnect,
		}
		req := connectReq{
			Host: c.Remote,
			ID:   c.id,
		}
		err = c.sendMsg(conn, m, &req)
		if err != nil {
			retry(err)
			continue
		}
		var r connectResp
		err = c.receive(conn, &r)
		if err != nil {
			retry(err)
			continue
		}
		if !r.Accepted {
			retry(fmt.Errorf("connection rejected: %s", r.RejectedReason))
			continue
		}
		remoteUUID := uuid.UUID(r.ID)
		if c.remoteID != nil && remoteUUID != *c.remoteID {
			// TODO: Only disconnect stateful clients.
			c.outgoing.Range(func(key uint64, client *MuxClient) bool {
				client.close()
				return true
			})
		}
		c.remoteID = &remoteUUID
		c.handleMessages(c.ctx, conn)
		atomic.StoreUint32((*uint32)(&c.State), StateConnected)
		c.connChange.Signal()
		for {
			c.connChange.Wait()
			if atomic.LoadUint32((*uint32)(&c.State)) != StateConnected {
				// Reconnect
				break
			}
		}
	}
}

func (c *Connection) receive(conn net.Conn, r receiver) error {
	b, op, err := wsutil.ReadData(conn, ws.StateClientSide)
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
	if c.shouldConnect() {
		return errors.New("expected to be client side, not server side")
	}
	msg := message{
		Op: OpConnectResponse,
	}

	if c.remoteID != nil && *c.remoteID != req.ID {
		atomic.StoreUint32((*uint32)(&c.State), StateConnectionError)
		c.connChange.Signal()
		// Remote ID changed.
		// Close all active requests.
		c.outgoing.Range(func(key uint64, client *MuxClient) bool {
			client.close()
			return true
		})
	}

	err := c.sendMsg(conn, msg, &connectResp{
		ID:       c.id,
		Accepted: true,
	})
	if err == nil {
		c.connWg.Wait()
		atomic.StoreUint32((*uint32)(&c.State), StateConnected)
		c.connChange.Signal()
		c.handleMessages(ctx, conn)
	}
	return err
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
			atomic.CompareAndSwapUint32((*uint32)(&c.State), StateConnected, StateConnectionError)
			c.connChange.Signal()
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
			switch m.Op {
			case OpMuxMsg:
				v, ok := c.outgoing.Load(m.MuxID)
				if !ok {
					logger.LogIf(c.ctx, c.queueMsg(message{Op: OpDisconnectMux, MuxID: m.MuxID}, nil))
					PutByteBuffer(m.Payload)
					continue
				}
				var err error
				if m.Flags&FlagEOF != 0 {
					err = io.EOF
				}
				v.response(m.Seq, Response{
					Msg: m.Payload,
					Err: err,
				})
			case OpConnectMux:
				c.
			}
			// TODO: Do something with the msg.
		}
	}()

	// Write goroutine.
	go func() {
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
	}()
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
