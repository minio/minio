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
	"github.com/google/uuid"
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

	// Handlers
	stateless [handlerLast]StatelessHandler

	// Handlers
	streams [handlerLast]*StatelessHandler

	// versions of each handler
	version [handlerLast]uint8
}

func NewManager(local string, hosts []string) *Manager {
	m := Manager{
		ID:      uuid.New(),
		targets: make(map[string]*Connection, len(hosts)),
	}
	for _, host := range hosts {
		m.targets[host] = NewConnection(m.ID, host)
	}
	return &m
}

// Connection will return the connection for the specified host.
// If the host does not exist nil will be returned.
func (m *Manager) Connection(host string) *Connection {
	return m.targets[host]
}

// RegisterStateless will register a stateless handler that serves
// []byte -> ([]byte, error) requests.
func (m *Manager) RegisterStateless(id HandlerID, h StatelessHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	idx := id.ID()
	if m.stateless[idx] != nil || m.streams[idx] != nil {
		return ErrHandlerAlreadyExists
	}
	m.stateless[idx] = h
	m.version[idx] = id.Version()
	return nil
}

// RegisterStreamingHandler will register a stateless handler that serves
// two-way streaming requests.
func (m *Manager) RegisterStreamingHandler(id HandlerID, h StatefulHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	idx := id.ID()
	if m.stateless[idx] != nil || m.streams[idx] != nil {
		return ErrHandlerAlreadyExists
	}
	m.stateless[idx] = h
	m.version[idx] = id.Version()
	return nil
}
