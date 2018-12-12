/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/certs"
)

const (
	serverShutdownPoll = 500 * time.Millisecond

	// DefaultShutdownTimeout - default shutdown timeout used for graceful http server shutdown.
	DefaultShutdownTimeout = 5 * time.Second

	// DefaultTCPKeepAliveTimeout - default TCP keep alive timeout for accepted connection.
	DefaultTCPKeepAliveTimeout = 30 * time.Second

	// DefaultReadTimeout - default timout to read data from accepted connection.
	DefaultReadTimeout = 5 * time.Minute

	// DefaultWriteTimeout - default timout to write data to accepted connection.
	DefaultWriteTimeout = 5 * time.Minute

	// DefaultMaxHeaderBytes - default maximum HTTP header size in bytes.
	DefaultMaxHeaderBytes = 1 * humanize.MiByte
)

// Server - extended http.Server supports multiple addresses to serve and enhanced connection handling.
type Server struct {
	http.Server
	Addrs                  []string                 // addresses on which the server listens for new connection.
	ShutdownTimeout        time.Duration            // timeout used for graceful server shutdown.
	TCPKeepAliveTimeout    time.Duration            // timeout used for underneath TCP connection.
	UpdateBytesReadFunc    func(*http.Request, int) // function to be called to update bytes read in bufConn.
	UpdateBytesWrittenFunc func(*http.Request, int) // function to be called to update bytes written in bufConn.
	listenerMutex          *sync.Mutex              // to guard 'listener' field.
	listener               *httpListener            // HTTP listener for all 'Addrs' field.
	inShutdown             uint32                   // indicates whether the server is in shutdown or not
	requestCount           int32                    // counter holds no. of request in progress.
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
	readTimeout := srv.ReadTimeout
	writeTimeout := srv.WriteTimeout
	handler := srv.Handler // if srv.Handler holds non-synced state -> possible data race

	addrs := set.CreateStringSet(srv.Addrs...).ToSlice() // copy and remove duplicates
	tcpKeepAliveTimeout := srv.TCPKeepAliveTimeout
	updateBytesReadFunc := srv.UpdateBytesReadFunc
	updateBytesWrittenFunc := srv.UpdateBytesWrittenFunc

	// Create new HTTP listener.
	var listener *httpListener
	listener, err = newHTTPListener(
		addrs,
		tlsConfig,
		tcpKeepAliveTimeout,
		readTimeout,
		writeTimeout,
		srv.MaxHeaderBytes,
		updateBytesReadFunc,
		updateBytesWrittenFunc,
	)
	if err != nil {
		return err
	}

	// Wrap given handler to do additional
	// * return 503 (service unavailable) if the server in shutdown.
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&srv.requestCount, 1)
		defer atomic.AddInt32(&srv.requestCount, -1)

		// If server is in shutdown, return 503 (service unavailable)
		if atomic.LoadUint32(&srv.inShutdown) != 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		// Handle request using passed handler.
		handler.ServeHTTP(w, r)
	})

	srv.listenerMutex.Lock()
	srv.Handler = wrappedHandler
	srv.listener = listener
	srv.listenerMutex.Unlock()

	// Start servicing with listener.
	return srv.Server.Serve(listener)
}

// Shutdown - shuts down HTTP server.
func (srv *Server) Shutdown() error {
	srv.listenerMutex.Lock()
	if srv.listener == nil {
		srv.listenerMutex.Unlock()
		return errors.New("server not initialized")
	}
	srv.listenerMutex.Unlock()

	if atomic.AddUint32(&srv.inShutdown, 1) > 1 {
		// shutdown in progress
		return errors.New("http server already in shutdown")
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
			NextProtos:               []string{"http/1.1"},
		}
		tlsConfig.GetCertificate = getCert
	}

	httpServer := &Server{
		Addrs:               addrs,
		ShutdownTimeout:     DefaultShutdownTimeout,
		TCPKeepAliveTimeout: DefaultTCPKeepAliveTimeout,
		listenerMutex:       &sync.Mutex{},
	}
	httpServer.Handler = handler
	httpServer.TLSConfig = tlsConfig
	httpServer.ReadTimeout = DefaultReadTimeout
	httpServer.WriteTimeout = DefaultWriteTimeout
	httpServer.MaxHeaderBytes = DefaultMaxHeaderBytes

	return httpServer
}
