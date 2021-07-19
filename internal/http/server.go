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
	"crypto/tls"
	"errors"
	"io/ioutil"
	"net/http"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/api"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/env"
)

const (
	serverShutdownPoll = 500 * time.Millisecond

	// DefaultShutdownTimeout - default shutdown timeout used for graceful http server shutdown.
	DefaultShutdownTimeout = 5 * time.Second

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
func (srv *Server) Start() (err error) {
	// Take a copy of server fields.
	var tlsConfig *tls.Config
	if srv.TLSConfig != nil {
		tlsConfig = srv.TLSConfig.Clone()
	}
	handler := srv.Handler // if srv.Handler holds non-synced state -> possible data race

	addrs := set.CreateStringSet(srv.Addrs...).ToSlice() // copy and remove duplicates

	// Create new HTTP listener.
	var listener *httpListener
	listener, err = newHTTPListener(
		addrs,
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
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(http.ErrServerClosed.Error()))
			w.(http.Flusher).Flush()
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
			tmp, err := ioutil.TempFile("", "minio-goroutines-*.txt")
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

// NewServer - creates new HTTP server using given arguments.
func NewServer(addrs []string, handler http.Handler, getCert certs.GetCertificateFunc) *Server {
	secureCiphers := env.Get(api.EnvAPISecureCiphers, config.EnableOn) == config.EnableOn

	var tlsConfig *tls.Config
	if getCert != nil {
		tlsConfig = &tls.Config{
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			NextProtos:               []string{"http/1.1", "h2"},
			GetCertificate:           getCert,
		}
		if secureCiphers || fips.Enabled {
			tlsConfig.CipherSuites = fips.CipherSuitesTLS()
			tlsConfig.CurvePreferences = fips.EllipticCurvesTLS()
		}
	}

	httpServer := &Server{
		Addrs:           addrs,
		ShutdownTimeout: DefaultShutdownTimeout,
	}
	httpServer.Handler = handler
	httpServer.TLSConfig = tlsConfig
	httpServer.MaxHeaderBytes = DefaultMaxHeaderBytes

	return httpServer
}
