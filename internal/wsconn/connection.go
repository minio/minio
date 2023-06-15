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

package wsconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime/debug"
	"sync/atomic"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/minio/minio/internal/logger"
	"github.com/puzpuzpuz/xsync/v2"
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

	// ID of this server instance.
	id uuid.UUID

	// Context for the server.
	ctx context.Context

	// Active mux connections.
	active *xsync.MapOf[uint64, *MuxClient]

	// outQueue is the output queue
	outQueue chan []byte

	// Client or serverside.
	side ws.State
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
func NewConnection(id uuid.UUID, remote string) *Connection {
	return &Connection{
		State:    StateUnconnected,
		Remote:   remote,
		id:       id,
		ctx:      context.TODO(),
		active:   xsync.NewIntegerMapOfPresized[uint64, *MuxClient](1000),
		outQueue: make(chan []byte, defaultOutQueue),
	}
}

func (r *Connection) NewMuxClient(ctx context.Context) *MuxClient {
	id := atomic.AddUint64(&r.NextID, 1)
	c := newMuxClient(ctx, id, r)
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, ok := r.active.LoadOrStore(id, c); !ok {
			break
		}
		c.MuxID = atomic.AddUint64(&r.NextID, 1)
	}
	return c
}

func (r *Connection) Single(ctx context.Context, h HandlerID, req []byte) ([]byte, error) {
	id := atomic.AddUint64(&r.NextID, 1)
	c := newMuxClient(ctx, id, r)
	for {
		// Handle the extremely unlikely scenario that we wrapped.
		if _, ok := r.active.LoadOrStore(id, c); !ok {
			break
		}
		c.MuxID = atomic.AddUint64(&r.NextID, 1)
	}
	defer r.active.Delete(c.MuxID)
	return c.roundtrip(h, req)
}

func (r *Connection) connect() {
	if atomic.CompareAndSwapUint32((*uint32)(&r.State), StateUnconnected, StateConnecting) {
	}
}

func (r *Connection) send(ctx context.Context, msg []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.outQueue <- msg:
		return nil
	}
}

func (r *Connection) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		msg, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			logger.LogIf(r.ctx, fmt.Errorf("handleMessages: reading connect: %w", err))
			return
		}
		var m message
		err = m.parse(msg)
		if err != nil {
			logger.LogIf(r.ctx, fmt.Errorf("handleMessages: parsing connect: %w", err))
			return
		}
		if m.Op != OpConnect {
			logger.LogIf(r.ctx, fmt.Errorf("handleMessages: unexpected op: %v", m.Op))
			return
		}
		var cReq ConnectReq
		_, err = cReq.UnmarshalMsg(msg)
		if err != nil {
			logger.LogIf(r.ctx, fmt.Errorf("handleMessages: parsing ConnectReq: %w", err))
			return
		}
		if !atomic.CompareAndSwapUint32((*uint32)(&r.State), StateUnconnected, StateConnected) {
			// Handle
		}
		if r.

		go r.handleMessages(conn)
	}
}

func (r *Connection) handleMessages(conn net.Conn) {
	// Read goroutine
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(r.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
			conn.Close()
		}()

		controlHandler := wsutil.ControlFrameHandler(conn, r.side)
		readDataInto := func(dst []byte, rw io.ReadWriter, s ws.State, want ws.OpCode) ([]byte, error) {
			dst = dst[:0]
			rd := wsutil.Reader{
				Source:          conn,
				State:           r.side,
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
			msg, err = readDataInto(msg, conn, r.side, ws.OpBinary)
			if err != nil {
				logger.LogIf(r.ctx, fmt.Errorf("ws read: %w", err))
				return
			}

			// Parse the received message
			err = m.parse(msg)
			if err != nil {
				logger.LogIf(r.ctx, fmt.Errorf("ws parse package: %w", err))
				return
			}
		}
	}()

	// Write goroutine.
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogIf(r.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
				debug.PrintStack()
			}
			conn.Close()
		}()
		for toSend := range r.outQueue {
			err := wsutil.WriteMessage(conn, r.side, ws.OpBinary, toSend)
			if err != nil {
				logger.LogIf(r.ctx, fmt.Errorf("ws write: %v", err))
				return
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
