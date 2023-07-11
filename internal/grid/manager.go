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
	"fmt"
	"net/http"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/minio/minio/internal/logger"
)

// apiVersion is a major version of the entire api.
// Bumping this should only be done when overall,
// incompatible changes are made, not when adding a new handler
// or changing an existin handler.
const apiVersion = 1

// Manager will
type Manager struct {
	// ID is an instance ID, that will change whenever the server restarts.
	// This allows remotes to keep track of whether state is preserved.
	ID uuid.UUID

	// Immutable after creation, so no locks.
	targets map[string]*Connection

	// serverside handlers.
	handlers handlers
}

// NewManager creates a new grid manager
func NewManager(dialer ContextDialer, local string, hosts []string, auth AuthFn) (*Manager, error) {
	found := false
	m := Manager{
		ID:      uuid.New(),
		targets: make(map[string]*Connection, len(hosts)),
	}
	for _, host := range hosts {
		if host == local {
			if found {
				return nil, fmt.Errorf("grid: local host found multiple times")
			}
			found = true
		}
		m.targets[host] = NewConnection(m.ID, local, host, dialer, &m.handlers, auth)
	}
	if found {
		return nil, fmt.Errorf("grid: local host not found")
	}

	return &m, nil
}

func (c *Manager) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		msg, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: reading connect: %w", err))
			return
		}
		var m message
		err = m.parse(msg)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: parsing connect: %w", err))
			return
		}
		if m.Op != OpConnect {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: unexpected op: %v", m.Op))
			return
		}
		var cReq connectReq
		_, err = cReq.UnmarshalMsg(msg)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: parsing ConnectReq: %w", err))
			return
		}
		remote := c.targets[cReq.Host]
		if remote == nil {
			logger.LogIf(ctx, fmt.Errorf("handleMessages: unknown host: %v", cReq.Host))
			return
		}
		logger.LogIf(ctx, remote.handleIncoming(ctx, conn, cReq))
	}
}

// AuthFn should provide an authentication string for the given aud.
type AuthFn func(aud string) string

// Connection will return the connection for the specified host.
// If the host does not exist nil will be returned.
func (m *Manager) Connection(host string) *Connection {
	return m.targets[host]
}

// RegisterSingle will register a stateless handler that serves
// []byte -> ([]byte, error) requests.
func (m *Manager) RegisterSingle(id HandlerID, h SingleHandlerFn) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	if m.handlers.hasAny(id) {
		return ErrHandlerAlreadyExists
	}

	m.handlers.single[id] = h
	return nil
}

// RegisterStateless will register a stateless handler that serves
// []byte -> stream of ([]byte, error) requests.
func (m *Manager) RegisterStateless(id HandlerID, h StatelessHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	if m.handlers.hasAny(id) {
		return ErrHandlerAlreadyExists
	}

	m.handlers.stateless[id] = &h
	return nil
}

// RegisterStreamingHandler will register a stateless handler that serves
// two-way streaming requests.
func (m *Manager) RegisterStreamingHandler(id HandlerID, h StatefulHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	if m.handlers.hasAny(id) {
		return ErrHandlerAlreadyExists
	}
	m.handlers.streams[id] = &h
	return nil
}
