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
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"

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
	active *xsync.MapOf[uint64, *MuxClient]

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State

	// Transport for outgoing connections.
	tr http.RoundTripper

	// connChange will be signaled whenever
	connChange     *sync.Cond
	connChangeLock sync.Mutex
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

const defaultOutQueue = 10000

// NewConnection will create an unconnected connection to a remote.
func NewConnection(id uuid.UUID, local, remote string, tr http.RoundTripper) *Connection {
	c := Connection{
		State:    StateUnconnected,
		Remote:   remote,
		Local:    local,
		id:       id,
		ctx:      context.Background(),
		active:   xsync.NewIntegerMapOfPresized[uint64, *MuxClient](1000),
		outQueue: make(chan []byte, defaultOutQueue),
		tr:       tr,
		side:     ws.StateServerSide,
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
		if _, ok := c.active.LoadOrStore(client.MuxID, client); !ok {
			break
		}
		client.MuxID = atomic.AddUint64(&c.NextID, 1)
	}
	return client
}

func (c *Connection) Single(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	client := c.NewMuxClient(ctx)
	defer c.active.Delete(client.MuxID)
	return client.roundtrip(h, req)
}

var ErrDone = errors.New("done for now")

// Stateless
func (c *Connection) Stateless(ctx context.Context, h HandlerID, req []byte, cb func([]byte) error) error {
	client := c.NewMuxClient(ctx)
	client.RequestBytes()
	defer c.active.Delete(client.MuxID)
	for {
	}
}

func (c *Connection) connect() {
	if atomic.CompareAndSwapUint32((*uint32)(&c.State), StateUnconnected, StateConnecting) {
	}
}

func (c *Connection) send(ctx context.Context, msg []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.outQueue <- msg:
		return nil
	}
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

func (c *Connection) handleIncoming(ctx context.Context, conn net.Conn, req connectReq) error {
	if c.shouldConnect() {
		return errors.New("expected to be client side, not server side")
	}
	msg := message{
		Op: OpConnectResponse,
	}
	if c.remoteID != nil && *c.remoteID != req.ID {
		atomic.StoreUint32((*uint32)(&c.State), StateConnectionError)
		// TODO: Only disconnect stateful clients.
		c.active.Range(func(key uint64, client *MuxClient) bool {
			client.close()
			return true
		})
	}

	err := c.sendMsg(conn, msg, &connectResp{
		ID:       c.id,
		Accepted: true,
	})
	if err == nil {
		atomic.StoreUint32((*uint32)(&c.State), StateConnected)
		go c.handleMessages(ctx, conn)
	}
	return err
}

func (c *Connection) handleMessages(ctx context.Context, conn net.Conn) {
	// Read goroutine
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(c.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
			conn.Close()
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
		}
	}()

	// Write goroutine.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(c.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
			conn.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case toSend := <-c.outQueue:
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
