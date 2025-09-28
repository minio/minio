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
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/minio/mux"
)

//go:generate stringer -type=debugMsg $GOFILE

// debugMsg is a debug message for testing purposes.
// may only be used for tests.
type debugMsg int

const (
	debugPrint = false
	debugReqs  = false
)

const (
	debugShutdown debugMsg = iota
	debugKillInbound
	debugKillOutbound
	debugWaitForExit
	debugSetConnPingDuration
	debugSetClientPingDuration
	debugAddToDeadline
	debugIsOutgoingClosed
	debugBlockInboundMessages
)

// TestGrid contains a grid of servers for testing purposes.
type TestGrid struct {
	Servers     []*httptest.Server
	Listeners   []net.Listener
	Managers    []*Manager
	Mux         []*mux.Router
	Hosts       []string
	cleanupOnce sync.Once
	cancel      context.CancelFunc
}

// SetupTestGrid creates a new grid for testing purposes.
// Select the number of hosts to create.
// Call (TestGrid).Cleanup() when done.
func SetupTestGrid(n int) (*TestGrid, error) {
	hosts, listeners, err := getHosts(n)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}
	var res TestGrid
	res.Hosts = hosts
	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	res.cancel = cancel
	for i, host := range hosts {
		manager, err := NewManager(ctx, ManagerOptions{
			Dialer: ConnectWS(dialer.DialContext,
				dummyNewToken,
				nil),
			Local:        host,
			Hosts:        hosts,
			AuthFn:       dummyNewToken,
			AuthToken:    dummyTokenValidate,
			BlockConnect: ready,
			RoutePath:    RoutePath,
		})
		if err != nil {
			return nil, err
		}
		m := mux.NewRouter()
		m.Handle(RoutePath, manager.Handler(dummyRequestValidate))
		res.Managers = append(res.Managers, manager)
		res.Servers = append(res.Servers, startHTTPServer(listeners[i], m))
		res.Listeners = append(res.Listeners, listeners[i])
		res.Mux = append(res.Mux, m)
	}
	close(ready)
	for _, m := range res.Managers {
		for _, remote := range m.Targets() {
			if err := m.Connection(remote).WaitForConnect(ctx); err != nil {
				return nil, err
			}
		}
	}
	return &res, nil
}

// Cleanup will clean up the test grid.
func (t *TestGrid) Cleanup() {
	t.cancel()
	t.cleanupOnce.Do(func() {
		for _, manager := range t.Managers {
			manager.debugMsg(debugShutdown)
		}
		for _, server := range t.Servers {
			server.Close()
		}
		for _, listener := range t.Listeners {
			listener.Close()
		}
	})
}

// WaitAllConnect will wait for all connections to be established.
func (t *TestGrid) WaitAllConnect(ctx context.Context) {
	for _, manager := range t.Managers {
		for _, remote := range manager.Targets() {
			if manager.HostName() == remote {
				continue
			}
			if err := manager.Connection(remote).WaitForConnect(ctx); err != nil {
				panic(err)
			}
		}
	}
}

func getHosts(n int) (hosts []string, listeners []net.Listener, err error) {
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
				return nil, nil, fmt.Errorf("httptest: failed to listen on a port: %v", err)
			}
		}
		addr := l.Addr()
		hosts = append(hosts, "http://"+addr.String())
		listeners = append(listeners, l)
	}
	return hosts, listeners, err
}

func startHTTPServer(listener net.Listener, handler http.Handler) (server *httptest.Server) {
	server = httptest.NewUnstartedServer(handler)
	server.Config.Addr = listener.Addr().String()
	server.Listener = listener
	server.Start()
	return server
}

func dummyRequestValidate(r *http.Request) error {
	return nil
}

func dummyTokenValidate(token string) error {
	if token == "debug" {
		return nil
	}
	return fmt.Errorf("invalid token. want empty, got %s", token)
}

func dummyNewToken() string {
	return "debug"
}
