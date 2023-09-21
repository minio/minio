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
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
)

const (
	// apiVersion is a major version of the entire api.
	// Bumping this should only be done when overall,
	// incompatible changes are made, not when adding a new handler
	// or changing an existing handler.
	apiVersion = "v1"

	// RoutePath is the remote path to connect to.
	RoutePath = "/minio/grid/" + apiVersion
)

// Manager will
type Manager struct {
	// ID is an instance ID, that will change whenever the server restarts.
	// This allows remotes to keep track of whether state is preserved.
	ID uuid.UUID

	// Immutable after creation, so no locks.
	targets map[string]*Connection

	// serverside handlers.
	handlers handlers

	local string
}

// ManagerOptions are options for creating a new grid manager.
type ManagerOptions struct {
	Dialer    ContextDialer
	Local     string
	Hosts     []string
	Auth      AuthFn
	TLSConfig *tls.Config

	debugBlockConnect chan struct{}
}

// NewManager creates a new grid manager
func NewManager(ctx context.Context, o ManagerOptions) (*Manager, error) {
	found := false
	m := &Manager{
		ID:      uuid.New(),
		targets: make(map[string]*Connection, len(o.Hosts)),
		local:   o.Local,
	}
	m.handlers.init()
	if ctx == nil {
		ctx = context.Background()
	}
	for _, host := range o.Hosts {
		if host == o.Local {
			if found {
				return nil, fmt.Errorf("grid: local host found multiple times")
			}
			found = true
			// No connection to local.
			continue
		}
		m.targets[host] = newConnection(connectionParams{
			ctx:               ctx,
			id:                m.ID,
			local:             o.Local,
			remote:            host,
			dial:              o.Dialer,
			handlers:          &m.handlers,
			auth:              o.Auth,
			debugBlockConnect: o.debugBlockConnect,
		})
	}
	if !found {
		return nil, fmt.Errorf("grid: local host not found")
	}

	return m, nil
}

// AddToMux will add the grid manager to the given mux.
func (m *Manager) AddToMux(router *mux.Router) {
	router.Handle(RoutePath, m.Handler())
}

// Handler returns a handler that can be used to serve grid requests.
// This should be connected on RoutePath to the main server.
func (m *Manager) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if debugPrint {
				fmt.Printf("grid: Handler returning from: %v %v\n", req.Method, req.URL)
			}
			if r := recover(); r != nil {
				debug.PrintStack()
				fmt.Printf("grid: panic: %v\n", r)
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		if debugPrint {
			fmt.Printf("grid: Got a %s request for: %v\n", req.Method, req.URL)
		}
		ctx := req.Context()
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			if debugPrint {
				fmt.Printf("grid: Unable to upgrade: %v. http.ResponseWriter is type %T\n", err, w)
			}
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		defer conn.Close()
		if debugPrint {
			fmt.Printf("grid: Upgraded request: %v\n", req.URL)
		}

		msg, _, err := wsutil.ReadClientData(conn)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("grid: reading connect: %w", err))
			return
		}
		if debugPrint {
			fmt.Printf("%s handler: Got message, length %v\n", m.local, len(msg))
		}

		var message message
		_, err = message.parse(msg)
		if err != nil {
			if debugPrint {
				fmt.Println("parse err:", err)
			}
			logger.LogIf(ctx, fmt.Errorf("handleMessages: parsing connect: %w", err))
			return
		}
		if message.Op != OpConnect {
			if debugPrint {
				fmt.Println("op err:", message.Op)
			}
			logger.LogIf(ctx, fmt.Errorf("handler: unexpected op: %v", message.Op))
			return
		}
		var cReq connectReq
		_, err = cReq.UnmarshalMsg(message.Payload)
		if err != nil {
			if debugPrint {
				fmt.Println("handler: creq err:", err)
			}
			logger.LogIf(ctx, fmt.Errorf("handleMessages: parsing ConnectReq: %w", err))
			return
		}
		remote := m.targets[cReq.Host]
		if remote == nil {
			if debugPrint {
				fmt.Printf("%s: handler: unknown host: %v. Have %v\n", m.local, cReq.Host, m.targets)
			}
			logger.LogIf(ctx, fmt.Errorf("handler: unknown host: %v", cReq.Host))
			return
		}
		if debugPrint {
			fmt.Printf("handler: Got Connect Req %+v\n", cReq)
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
	if m.handlers.hasAny(id) && !id.isTestHandler() {
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
	if m.handlers.hasAny(id) && !id.isTestHandler() {
		return ErrHandlerAlreadyExists
	}

	m.handlers.stateless[id] = &h
	return nil
}

// RegisterStreamingHandler will register a stateless handler that serves
// two-way streaming requests.
func (m *Manager) RegisterStreamingHandler(id HandlerID, h StreamHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	if h.SubRoute == "" {
		if m.handlers.hasAny(id) && !id.isTestHandler() {
			return ErrHandlerAlreadyExists
		}
		m.handlers.streams[id] = &h
		return nil
	}
	if debugPrint {
		fmt.Println("RegisterStreamingHandler: subroute:", h.SubRoute)
	}
	subID := makeSubHandlerID(id, h.SubRoute)
	if m.handlers.hasSubhandler(subID) && !id.isTestHandler() {
		return ErrHandlerAlreadyExists
	}
	m.handlers.subStreams[subID] = &h
	// Copy so clients can also pick it up for other subpaths.
	m.handlers.subStreams[makeZeroSubHandlerID(id)] = &h
	return nil
}

// HostName returns the name of the local host.
func (m *Manager) HostName() string {
	return m.local
}

// TestingShutDown will shut down all connections.
// This should *only* be used by tests.
//
//lint:ignore U1000 This is used by tests.
func (m *Manager) debugMsg(d debugMsg, args ...any) {
	for _, c := range m.targets {
		c.debugMsg(d, args...)
	}
}
