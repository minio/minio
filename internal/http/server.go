// Copyright (c) 2015-2021 MinIO, Inc.
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

package http

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

var (
	// GlobalMinIOVersion - is sent in the header to all http targets
	GlobalMinIOVersion string

	// GlobalDeploymentID - is sent in the header to all http targets
	GlobalDeploymentID string
)

const (
	serverShutdownPoll = 500 * time.Millisecond

	// DefaultShutdownTimeout - default shutdown timeout to gracefully shutdown server.
	DefaultShutdownTimeout = 5 * time.Second

	// DefaultIdleTimeout for idle inactive connections
	DefaultIdleTimeout = 30 * time.Second

	// DefaultReadHeaderTimeout for very slow inactive connections
	DefaultReadHeaderTimeout = 30 * time.Second

	// DefaultMaxHeaderBytes - default maximum HTTP header size in bytes.
	DefaultMaxHeaderBytes = 1 * humanize.MiByte
)

// Server - extended http.Server supports multiple addresses to serve and enhanced connection handling.
type Server struct {
	http.Server
	Addrs           []string      // addresses on which the server listens for new connection.
	ShutdownTimeout time.Duration // timeout used for graceful server shutdown.
	listenerMutex   sync.Mutex    // to guard 'listener' field.
	listener        *httpListener // HTTP listener for all 'Addrs' field.
	inShutdown      uint32        // indicates whether the server is in shutdown or not
	requestCount    int32         // counter holds no. of request in progress.
}

// GetRequestCount - returns number of request in progress.
func (srv *Server) GetRequestCount() int {
	return int(atomic.LoadInt32(&srv.requestCount))
}

// Start - start HTTP server
func (srv *Server) Start(ctx context.Context) (err error) {
	// Take a copy of server fields.
	var tlsConfig *tls.Config
	if srv.TLSConfig != nil {
		tlsConfig = srv.TLSConfig.Clone()
	}
	handler := srv.Handler // if srv.Handler holds non-synced state -> possible data race

	// Create new HTTP listener.
	var listener *httpListener
	listener, err = newHTTPListener(
		ctx,
		srv.Addrs,
	)
	if err != nil {
		return err
	}

	// Wrap given handler to do additional
	// * return 503 (service unavailable) if the server in shutdown.
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If server is in shutdown.
		if atomic.LoadUint32(&srv.inShutdown) != 0 {
			// To indicate disable keep-alives
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(http.ErrServerClosed.Error()))
			return
		}

		atomic.AddInt32(&srv.requestCount, 1)
		defer atomic.AddInt32(&srv.requestCount, -1)

		// Handle request using passed handler.
		handler.ServeHTTP(w, r)
	})

	srv.listenerMutex.Lock()
	srv.Handler = wrappedHandler
	srv.listener = listener
	srv.listenerMutex.Unlock()

	// Start servicing with listener.
	if tlsConfig != nil {
		return srv.Server.Serve(tls.NewListener(listener, tlsConfig))
	}
	return srv.Server.Serve(listener)
}

// Shutdown - shuts down HTTP server.
func (srv *Server) Shutdown() error {
	srv.listenerMutex.Lock()
	if srv.listener == nil {
		srv.listenerMutex.Unlock()
		return http.ErrServerClosed
	}
	srv.listenerMutex.Unlock()

	if atomic.AddUint32(&srv.inShutdown, 1) > 1 {
		// shutdown in progress
		return http.ErrServerClosed
	}

	// Close underneath HTTP listener.
	srv.listenerMutex.Lock()
	err := srv.listener.Close()
	srv.listenerMutex.Unlock()
	if err != nil {
		return err
	}

	// Wait for opened connection to be closed up to Shutdown timeout.
	shutdownTimeout := srv.ShutdownTimeout
	shutdownTimer := time.NewTimer(shutdownTimeout)
	ticker := time.NewTicker(serverShutdownPoll)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownTimer.C:
			// Write all running goroutines.
			tmp, err := os.CreateTemp("", "minio-goroutines-*.txt")
			if err == nil {
				_ = pprof.Lookup("goroutine").WriteTo(tmp, 1)
				tmp.Close()
				return errors.New("timed out. some connections are still active. goroutines written to " + tmp.Name())
			}
			return errors.New("timed out. some connections are still active")
		case <-ticker.C:
			if atomic.LoadInt32(&srv.requestCount) <= 0 {
				return nil
			}
		}
	}
}

// UseShutdownTimeout configure server shutdown timeout
func (srv *Server) UseShutdownTimeout(d time.Duration) *Server {
	srv.ShutdownTimeout = d
	return srv
}

// UseIdleTimeout configure idle connection timeout
func (srv *Server) UseIdleTimeout(d time.Duration) *Server {
	srv.IdleTimeout = d
	return srv
}

// UseReadHeaderTimeout configure read header timeout
func (srv *Server) UseReadHeaderTimeout(d time.Duration) *Server {
	srv.ReadHeaderTimeout = d
	return srv
}

// UseHandler configure final handler for this HTTP *Server
func (srv *Server) UseHandler(h http.Handler) *Server {
	srv.Handler = h
	return srv
}

// UseTLSConfig pass configured TLSConfig for this HTTP *Server
func (srv *Server) UseTLSConfig(cfg *tls.Config) *Server {
	srv.TLSConfig = cfg
	return srv
}

// UseBaseContext use custom base context for this HTTP *Server
func (srv *Server) UseBaseContext(ctx context.Context) *Server {
	srv.BaseContext = func(listener net.Listener) context.Context {
		return ctx
	}
	return srv
}

// UseCustomLogger use customized logger for this HTTP *Server
func (srv *Server) UseCustomLogger(l *log.Logger) *Server {
	srv.ErrorLog = l
	return srv
}

// NewServer - creates new HTTP server using given arguments.
func NewServer(addrs []string) *Server {
	httpServer := &Server{
		Addrs: addrs,
	}
	// This is not configurable for now.
	httpServer.MaxHeaderBytes = DefaultMaxHeaderBytes
	return httpServer
}

// SetMinIOVersion -- MinIO version from the main package is set here
func SetMinIOVersion(version string) {
	GlobalMinIOVersion = version
}

// SetDeploymentID -- Deployment Id from the main package is set here
func SetDeploymentID(deploymentID string) {
	GlobalDeploymentID = deploymentID
}
