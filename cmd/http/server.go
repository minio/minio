/*
 * MinIO Cloud Storage, (C) 2017, 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/pkg/certs"
)

const (
	serverShutdownPoll = 500 * time.Millisecond

	// DefaultShutdownTimeout - default shutdown timeout used for graceful http server shutdown.
	DefaultShutdownTimeout = 5 * time.Second

	// DefaultTCPKeepAliveTimeout - default TCP keep alive timeout for accepted connection.
	DefaultTCPKeepAliveTimeout = 30 * time.Second

	// DefaultMaxHeaderBytes - default maximum HTTP header size in bytes.
	DefaultMaxHeaderBytes = 1 * humanize.MiByte
)

// Server - extended http.Server supports multiple addresses to serve and enhanced connection handling.
type Server struct {
	http.Server
	Addrs               []string      // addresses on which the server listens for new connection.
	ShutdownTimeout     time.Duration // timeout used for graceful server shutdown.
	TCPKeepAliveTimeout time.Duration // timeout used for underneath TCP connection.
	listenerMutex       sync.Mutex    // to guard 'listener' field.
	listener            *httpListener // HTTP listener for all 'Addrs' field.
	inShutdown          uint32        // indicates whether the server is in shutdown or not
	requestCount        int32         // counter holds no. of request in progress.
}

// GetRequestCount - returns number of request in progress.
func (srv *Server) GetRequestCount() int32 {
	return atomic.LoadInt32(&srv.requestCount)
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
	tcpKeepAliveTimeout := srv.TCPKeepAliveTimeout

	// Create new HTTP listener.
	var listener *httpListener
	listener, err = newHTTPListener(
		addrs,
		tcpKeepAliveTimeout,
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
				return errors.New("timed out. some connections are still active. doing abnormal shutdown. goroutines written to " + tmp.Name())
			}
			return errors.New("timed out. some connections are still active. doing abnormal shutdown")
		case <-ticker.C:
			if atomic.LoadInt32(&srv.requestCount) <= 0 {
				return err
			}
		}
	}
}

// Secure Go implementations of modern TLS ciphers
// The following ciphers are excluded because:
//  - RC4 ciphers:              RC4 is broken
//  - 3DES ciphers:             Because of the 64 bit blocksize of DES (Sweet32)
//  - CBC-SHA256 ciphers:       No countermeasures against Lucky13 timing attack
//  - CBC-SHA ciphers:          Legacy ciphers (SHA-1) and non-constant time
//                              implementation of CBC.
//                              (CBC-SHA ciphers can be enabled again if required)
//  - RSA key exchange ciphers: Disabled because of dangerous PKCS1-v1.5 RSA
//                              padding scheme. See Bleichenbacher attacks.
var defaultCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

// Go only provides constant-time implementations of Curve25519 and NIST P-256 curve.
var secureCurves = []tls.CurveID{tls.X25519, tls.CurveP256}

// NewServer - creates new HTTP server using given arguments.
func NewServer(addrs []string, handler http.Handler, getCert certs.GetCertificateFunc) *Server {
	var tlsConfig *tls.Config
	if getCert != nil {
		tlsConfig = &tls.Config{
			// TLS hardening
			PreferServerCipherSuites: true,
			CipherSuites:             defaultCipherSuites,
			CurvePreferences:         secureCurves,
			MinVersion:               tls.VersionTLS12,
			// Do not edit the next line, protos priority is kept
			// on purpose in this manner for HTTP 2.0, we would
			// still like HTTP 2.0 clients to negotiate connection
			// to server if needed but by default HTTP 1.1 is
			// expected. We need to change this in future
			// when we wish to go back to HTTP 2.0 as default
			// priority for HTTP protocol negotiation.
			NextProtos: []string{"http/1.1", "h2"},
		}
		tlsConfig.GetCertificate = getCert
	}

	httpServer := &Server{
		Addrs:               addrs,
		ShutdownTimeout:     DefaultShutdownTimeout,
		TCPKeepAliveTimeout: DefaultTCPKeepAliveTimeout,
	}
	httpServer.Handler = handler
	httpServer.TLSConfig = tlsConfig
	httpServer.MaxHeaderBytes = DefaultMaxHeaderBytes

	return httpServer
}
