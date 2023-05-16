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

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/minio/minio/internal/logger"
)

// A Connection is a remote connection.
// There is no distinction externally whether this was
type Connection struct {
	// State of the connection (atomic)
	State State

	// Non-atomic
	Remote string

	// Context for the server.
	ctx context.Context

	// Active mux connections.
	active map[uint32]*MuxClient
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

func (r *Connection) connect() error {
	return nil
}

func (r *Connection) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			// handle error
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

type MuxClient struct {
	ID       uint32
	Seq      uint32
	Resp     chan []byte
	Stateful bool
}
