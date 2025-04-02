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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/deadlineconn"
	"github.com/minio/minio/internal/pubsub"
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

	// RouteLockPath is the remote lock path to connect to.
	RouteLockPath = "/minio/grid/lock/" + apiVersion
)

// Manager will contain all the connections to the grid.
// It also handles incoming requests and routes them to the appropriate connection.
type Manager struct {
	// ID is an instance ID, that will change whenever the server restarts.
	// This allows remotes to keep track of whether state is preserved.
	ID uuid.UUID

	// Immutable after creation, so no locks.
	targets map[string]*Connection

	// serverside handlers.
	handlers handlers

	// local host name.
	local string

	// authToken is a function that will validate a token.
	authToken ValidateTokenFn

	// routePath indicates the dial route path
	routePath string
}

// ManagerOptions are options for creating a new grid manager.
type ManagerOptions struct {
	Local        string        // Local host name.
	Hosts        []string      // All hosts, including local in the grid.
	Incoming     func(n int64) // Record incoming bytes.
	Outgoing     func(n int64) // Record outgoing bytes.
	BlockConnect chan struct{} // If set, incoming and outgoing connections will be blocked until closed.
	RoutePath    string
	TraceTo      *pubsub.PubSub[madmin.TraceInfo, madmin.TraceType]
	Dialer       ConnDialer
	// Sign a token for the given audience.
	AuthFn AuthFn
	// Callbacks to validate incoming connections.
	AuthToken ValidateTokenFn
}

// NewManager creates a new grid manager
func NewManager(ctx context.Context, o ManagerOptions) (*Manager, error) {
	found := false
	if o.AuthToken == nil {
		return nil, fmt.Errorf("grid: AuthToken not set")
	}
	if o.Dialer == nil {
		return nil, fmt.Errorf("grid: Dialer not set")
	}
	if o.AuthFn == nil {
		return nil, fmt.Errorf("grid: AuthFn not set")
	}
	m := &Manager{
		ID:        uuid.New(),
		targets:   make(map[string]*Connection, len(o.Hosts)),
		local:     o.Local,
		authToken: o.AuthToken,
		routePath: o.RoutePath,
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
			ctx:           ctx,
			id:            m.ID,
			local:         o.Local,
			remote:        host,
			handlers:      &m.handlers,
			blockConnect:  o.BlockConnect,
			publisher:     o.TraceTo,
			incomingBytes: o.Incoming,
			outgoingBytes: o.Outgoing,
			dialer:        o.Dialer,
			authFn:        o.AuthFn,
		})
	}
	if !found {
		return nil, fmt.Errorf("grid: local host (%s) not found in cluster setup", o.Local)
	}

	return m, nil
}

// AddToMux will add the grid manager to the given mux.
func (m *Manager) AddToMux(router *mux.Router, authReq func(r *http.Request) error) {
	router.Handle(m.routePath, m.Handler(authReq))
}

// Handler returns a handler that can be used to serve grid requests.
// This should be connected on RoutePath to the main server.
func (m *Manager) Handler(authReq func(r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if debugPrint {
				fmt.Printf("grid: Handler returning from: %v %v\n", req.Method, req.URL)
			}
			if r := recover(); r != nil {
				debug.PrintStack()
				err := fmt.Errorf("grid: panic: %v\n", r)
				gridLogIf(context.Background(), err, err.Error())
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		if debugPrint {
			fmt.Printf("grid: Got a %s request for: %v\n", req.Method, req.URL)
		}
		ctx := req.Context()
		if err := authReq(req); err != nil {
			gridLogOnceIf(ctx, fmt.Errorf("auth %s: %w", req.RemoteAddr, err), req.RemoteAddr)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		conn, _, _, err := ws.UpgradeHTTP(req, w)
		if err != nil {
			if debugPrint {
				fmt.Printf("grid: Unable to upgrade: %v. http.ResponseWriter is type %T\n", err, w)
			}
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		m.IncomingConn(ctx, conn)
	}
}

// IncomingConn will handle an incoming connection.
// This should be called with the incoming connection after accept.
// Auth is handled internally, as well as disconnecting any connections from the same host.
func (m *Manager) IncomingConn(ctx context.Context, conn net.Conn) {
	// We manage our own deadlines.
	conn = deadlineconn.Unwrap(conn)
	remoteAddr := conn.RemoteAddr().String()
	// will write an OpConnectResponse message to the remote and log it once locally.
	defer conn.Close()
	writeErr := func(err error) {
		if err == nil {
			return
		}
		if errors.Is(err, io.EOF) {
			return
		}
		gridLogOnceIf(ctx, err, remoteAddr)
		resp := connectResp{
			ID:             m.ID,
			Accepted:       false,
			RejectedReason: err.Error(),
		}
		if b, err := resp.MarshalMsg(nil); err == nil {
			msg := message{
				Op:      OpConnectResponse,
				Payload: b,
			}
			if b, err := msg.MarshalMsg(nil); err == nil {
				wsutil.WriteMessage(conn, ws.StateServerSide, ws.OpBinary, b)
			}
		}
	}
	defer conn.Close()
	if debugPrint {
		fmt.Printf("grid: Upgraded request: %v\n", remoteAddr)
	}

	msg, _, err := wsutil.ReadClientData(conn)
	if err != nil {
		writeErr(fmt.Errorf("reading connect: %w", err))
		return
	}
	if debugPrint {
		fmt.Printf("%s handler: Got message, length %v\n", m.local, len(msg))
	}

	var message message
	_, _, err = message.parse(msg)
	if err != nil {
		writeErr(fmt.Errorf("error parsing grid connect: %w", err))
		return
	}
	if message.Op != OpConnect {
		writeErr(fmt.Errorf("unexpected connect op: %v", message.Op))
		return
	}
	var cReq connectReq
	_, err = cReq.UnmarshalMsg(message.Payload)
	if err != nil {
		writeErr(fmt.Errorf("error parsing connectReq: %w", err))
		return
	}
	remote := m.targets[cReq.Host]
	if remote == nil {
		writeErr(fmt.Errorf("unknown incoming host: %v", cReq.Host))
		return
	}
	if time.Since(cReq.Time).Abs() > 5*time.Minute {
		writeErr(fmt.Errorf("time difference too large between servers: %v", time.Since(cReq.Time).Abs()))
		return
	}
	if err := m.authToken(cReq.Token); err != nil {
		writeErr(fmt.Errorf("auth token: %w", err))
		return
	}

	if debugPrint {
		fmt.Printf("handler: Got Connect Req %+v\n", cReq)
	}
	writeErr(remote.handleIncoming(ctx, conn, cReq))
}

// AuthFn should provide an authentication string for the given aud.
type AuthFn func() string

// ValidateAuthFn should check authentication for the given aud.
type ValidateAuthFn func(auth string) string

// Connection will return the connection for the specified host.
// If the host does not exist nil will be returned.
func (m *Manager) Connection(host string) *Connection {
	return m.targets[host]
}

// RegisterSingleHandler will register a stateless handler that serves
// []byte -> ([]byte, error) requests.
// subroutes are joined with "/" to a single subroute.
func (m *Manager) RegisterSingleHandler(id HandlerID, h SingleHandlerFn, subroute ...string) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	s := strings.Join(subroute, "/")
	if debugPrint {
		fmt.Println("RegisterSingleHandler: ", id.String(), "subroute:", s)
	}

	if len(subroute) == 0 {
		if m.handlers.hasAny(id) && !id.isTestHandler() {
			return fmt.Errorf("handler %v: %w", id.String(), ErrHandlerAlreadyExists)
		}

		m.handlers.single[id] = h
		return nil
	}
	subID := makeSubHandlerID(id, s)
	if m.handlers.hasSubhandler(subID) && !id.isTestHandler() {
		return fmt.Errorf("handler %v, subroute:%v: %w", id.String(), s, ErrHandlerAlreadyExists)
	}
	m.handlers.subSingle[subID] = h
	// Copy so clients can also pick it up for other subpaths.
	m.handlers.subSingle[makeZeroSubHandlerID(id)] = h
	return nil
}

/*
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
*/

// RegisterStreamingHandler will register a stateless handler that serves
// two-way streaming requests.
func (m *Manager) RegisterStreamingHandler(id HandlerID, h StreamHandler) error {
	if !id.valid() {
		return ErrUnknownHandler
	}
	if debugPrint {
		fmt.Println("RegisterStreamingHandler: subroute:", h.Subroute)
	}
	if h.Subroute == "" {
		if m.handlers.hasAny(id) && !id.isTestHandler() {
			return ErrHandlerAlreadyExists
		}
		m.handlers.streams[id] = &h
		return nil
	}
	subID := makeSubHandlerID(id, h.Subroute)
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

// Targets returns the names of all remote targets.
func (m *Manager) Targets() []string {
	var res []string
	for k := range m.targets {
		res = append(res, k)
	}
	return res
}

// debugMsg should *only* be used by tests.
//
//lint:ignore U1000 This is used by tests.
func (m *Manager) debugMsg(d debugMsg, args ...any) {
	for _, c := range m.targets {
		c.debugMsg(d, args...)
	}
}

// ConnStats returns the connection statistics for all connections.
func (m *Manager) ConnStats() madmin.RPCMetrics {
	var res madmin.RPCMetrics
	for _, c := range m.targets {
		t := c.Stats()
		res.Merge(&t)
	}
	return res
}
