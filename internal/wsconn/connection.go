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
	"fmt"
	"net"
	"net/http"
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
}

// State is a connection state.
type State uint32

const (
	// StateUnconnected is the initial state of a connection.
	// When the first message is sent it will attempt to connect.
	StateUnconnected State = iota

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

func (r *Connection) send(msg []byte) {
	r.outQueue <- msg
}

func (r *Connection) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		go r.handleMessages(conn)
	}
}

func (r *Connection) handleMessages(conn net.Conn) {
	defer func() {
		if rec := recover(); rec != nil {
			logger.LogIf(r.ctx, fmt.Errorf("handleMessages: panic recovered: %v", rec))
		}
		conn.Close()
	}()
	for {
		msg, op, err := wsutil.ReadClientData(conn)
		if err != nil {
			// handle error
		}
		err = wsutil.WriteServerMessage(conn, op, msg)
		if err != nil {
			// handle error
		}
	}
}
