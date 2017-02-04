/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// The value chosen below is longest word chosen
// from all the http verbs comprising of
// "PRI", "OPTIONS", "GET", "HEAD", "POST",
// "PUT", "DELETE", "TRACE", "CONNECT".
const (
	maxHTTPVerbLen = 7
)

var defaultHTTP2Methods = []string{
	"PRI",
}

var defaultHTTP1Methods = []string{
	"OPTIONS",
	"GET",
	"HEAD",
	"POST",
	"PUT",
	"DELETE",
	"TRACE",
	"CONNECT",
}

// ConnMux - Peeks into the incoming connection for relevant
// protocol without advancing the underlying net.Conn (io.Reader).
// ConnMux - allows us to multiplex between TLS and Regular HTTP
// connections on the same listeners.
type ConnMux struct {
	net.Conn
	bufrw *bufio.ReadWriter
}

// NewConnMux - creates a new ConnMux instance
func NewConnMux(c net.Conn) *ConnMux {
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	return &ConnMux{
		Conn:  c,
		bufrw: bufio.NewReadWriter(br, bw),
	}
}

// List of protocols to be detected by PeekProtocol function.
const (
	protocolTLS   = "tls"
	protocolHTTP1 = "http"
	protocolHTTP2 = "http2"
)

// PeekProtocol - reads the first bytes, then checks if it is similar
// to one of the default http methods. Returns error if there are any
// errors in peeking over the connection.
func (c *ConnMux) PeekProtocol() (string, error) {
	// Peek for HTTP verbs.
	buf, err := c.bufrw.Peek(maxHTTPVerbLen)
	if err != nil {
		return "", err
	}

	// Check for HTTP2 methods first.
	for _, m := range defaultHTTP2Methods {
		if strings.HasPrefix(string(buf), m) {
			return protocolHTTP2, nil
		}
	}

	// Check for HTTP1 methods.
	for _, m := range defaultHTTP1Methods {
		if strings.HasPrefix(string(buf), m) {
			return protocolHTTP1, nil
		}
	}

	// Default to TLS, this is not a real indication
	// that the connection is TLS but that will be
	// validated later by doing a handshake.
	return protocolTLS, nil
}

// Read - streams the ConnMux buffer when reset flag is activated, otherwise
// streams from the incoming network connection
func (c *ConnMux) Read(b []byte) (int, error) {
	// Push read deadline
	c.Conn.SetReadDeadline(time.Now().Add(defaultTCPReadTimeout))
	return c.bufrw.Read(b)
}

// Close the connection.
func (c *ConnMux) Close() (err error) {
	// Make sure that we always close a connection,
	// even if the bufioWriter flush sends an error.
	defer c.Conn.Close()

	// Flush and write to the connection.
	return c.bufrw.Flush()
}

// ListenerMux wraps the standard net.Listener to inspect
// the communication protocol upon network connection
// ListenerMux also wraps net.Listener to ensure that once
// Listener.Close returns, the underlying socket has been closed.
//
// - https://github.com/golang/go/issues/10527
//
// The default Listener returns from Close before the underlying
// socket has been closed if another goroutine has an active
// reference (e.g. is in Accept).
//
// The following sequence of events can happen:
//
// Goroutine 1 is running Accept, and is blocked, waiting for epoll
//
// Goroutine 2 calls Close. It sees an extra reference, and so cannot
//  destroy the socket, but instead decrements a reference, marks the
//  connection as closed and unblocks epoll.
//
// Goroutine 2 returns to the caller, makes a new connection.
// The new connection is sent to the socket (since it hasn't been destroyed)
//
// Goroutine 1 returns from epoll, and accepts the new connection.
//
// To avoid accepting connections after Close, we block Goroutine 2
// from returning from Close till Accept returns an error to the user.
type ListenerMux struct {
	net.Listener
	config *tls.Config
	// acceptResCh is a channel for transporting wrapped net.Conn (regular or tls)
	// after peeking the content of the latter
	acceptResCh chan ListenerMuxAcceptRes
	// Cond is used to signal Close when there are no references to the listener.
	cond *sync.Cond
	refs int
}

// ListenerMuxAcceptRes contains then final net.Conn data (wrapper by tls or not) to be sent to the http handler
type ListenerMuxAcceptRes struct {
	conn net.Conn
	err  error
}

// Default keep alive interval timeout, on your Linux system to figure out
// maximum probes sent
//
// $ cat /proc/sys/net/ipv4/tcp_keepalive_probes
// 9
//
// Effective value of total keep alive comes upto 9 x 10 * time.Second = 1.5 Minutes.
var defaultKeepAliveTimeout = 10 * time.Second // 10 seconds.

// Timeout to close connection when a client is not sending any data
var defaultTCPReadTimeout = 30 * time.Second

// newListenerMux listens and wraps accepted connections with tls after protocol peeking
func newListenerMux(listener net.Listener, config *tls.Config) *ListenerMux {
	l := ListenerMux{
		Listener:    listener,
		config:      config,
		cond:        sync.NewCond(&sync.Mutex{}),
		acceptResCh: make(chan ListenerMuxAcceptRes),
	}
	// Start listening, wrap connections with tls when needed
	go func() {
		// Extract tcp listener.
		tcpListener, ok := l.Listener.(*net.TCPListener)
		if !ok {
			l.acceptResCh <- ListenerMuxAcceptRes{err: errInvalidArgument}
			return
		}

		// Loop for accepting new connections
		for {
			// Use accept TCP method to receive the connection.
			conn, err := tcpListener.AcceptTCP()
			if err != nil {
				l.acceptResCh <- ListenerMuxAcceptRes{err: err}
				continue
			}

			// Enable Read timeout
			conn.SetReadDeadline(time.Now().Add(defaultTCPReadTimeout))

			// Enable keep alive for each connection.
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(defaultKeepAliveTimeout)

			// Allocate new conn muxer.
			connMux := NewConnMux(conn)

			// Wrap the connection with ConnMux to be able to peek the data in the incoming connection
			// and decide if we need to wrap the connection itself with a TLS or not
			go func(connMux *ConnMux) {
				protocol, err := connMux.PeekProtocol()
				if err != nil {
					// io.EOF is usually returned by non-http clients,
					// just close the connection to avoid any leak.
					if err != io.EOF {
						errorIf(err, "Unable to peek into incoming protocol")
					}
					connMux.Close()
					return
				}
				switch protocol {
				case protocolTLS:
					tlsConn := tls.Server(connMux, l.config)
					// Make sure to handshake so that we know that this
					// is a TLS connection, if not we should close and reject
					// such a connection.
					if err = tlsConn.Handshake(); err != nil {
						errorIf(err, "TLS handshake failed")
						tlsConn.Close()
						return
					}
					l.acceptResCh <- ListenerMuxAcceptRes{
						conn: tlsConn,
					}
				default:
					l.acceptResCh <- ListenerMuxAcceptRes{
						conn: connMux,
					}
				}
			}(connMux)
		}
	}()
	return &l
}

// IsClosed - Returns if the underlying listener is closed fully.
func (l *ListenerMux) IsClosed() bool {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()
	return l.refs == 0
}

func (l *ListenerMux) incRef() {
	l.cond.L.Lock()
	l.refs++
	l.cond.L.Unlock()
}

func (l *ListenerMux) decRef() {
	l.cond.L.Lock()
	l.refs--
	newRefs := l.refs
	l.cond.L.Unlock()
	if newRefs == 0 {
		l.cond.Broadcast()
	}
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (l *ListenerMux) Close() error {
	if l == nil {
		return nil
	}

	if err := l.Listener.Close(); err != nil {
		return err
	}

	l.cond.L.Lock()
	for l.refs > 0 {
		l.cond.Wait()
	}
	l.cond.L.Unlock()
	return nil
}

// Accept - peek the protocol to decide if we should wrap the
// network stream with the TLS server
func (l *ListenerMux) Accept() (net.Conn, error) {
	l.incRef()
	defer l.decRef()

	res := <-l.acceptResCh
	return res.conn, res.err
}

// ServerMux - the main mux server
type ServerMux struct {
	Addr      string
	handler   http.Handler
	listeners []*ListenerMux

	gracefulWait    *sync.WaitGroup
	gracefulTimeout time.Duration

	mu     sync.Mutex // guards closed, and listener
	closed bool
}

// NewServerMux constructor to create a ServerMux
func NewServerMux(addr string, handler http.Handler) *ServerMux {
	m := &ServerMux{
		Addr:    addr,
		handler: handler,
		// Wait for 5 seconds for new incoming connnections, otherwise
		// forcibly close them during graceful stop or restart.
		gracefulTimeout: 5 * time.Second,
		gracefulWait:    &sync.WaitGroup{},
	}

	// Returns configured HTTP server.
	return m
}

// Initialize listeners on all ports.
func initListeners(serverAddr string, tls *tls.Config) ([]*ListenerMux, error) {
	host, port, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return nil, err
	}
	var listeners []*ListenerMux
	if host == "" {
		var listener net.Listener
		listener, err = net.Listen("tcp", serverAddr)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, newListenerMux(listener, tls))
		return listeners, nil
	}
	var addrs []string
	if net.ParseIP(host) != nil {
		addrs = append(addrs, host)
	} else {
		addrs, err = net.LookupHost(host)
		if err != nil {
			return nil, err
		}
		if len(addrs) == 0 {
			return nil, errUnexpected
		}
	}
	for _, addr := range addrs {
		var listener net.Listener
		listener, err = net.Listen("tcp", net.JoinHostPort(addr, port))
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, newListenerMux(listener, tls))
	}
	return listeners, nil
}

// ListenAndServe - serve HTTP requests with protocol multiplexing support
// TLS is actived when certFile and keyFile parameters are not empty.
func (m *ServerMux) ListenAndServe(certFile, keyFile string) (err error) {

	tlsEnabled := certFile != "" && keyFile != ""

	config := &tls.Config{
		// Causes servers to use Go's default ciphersuite preferences,
		// which are tuned to avoid attacks. Does nothing on clients.
		PreferServerCipherSuites: true,
		// Only use curves which have assembly implementations
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
		},
		// Set minimum version to TLS 1.2
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,

			// Best disabled, as they don't provide Forward Secrecy,
			// but might be necessary for some clients
			// tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			// tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		},
	} // Always instantiate.

	if tlsEnabled {
		// Configure TLS in the server
		if config.NextProtos == nil {
			config.NextProtos = []string{"http/1.1", "h2"}
		}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
	}

	go m.handleServiceSignals()

	listeners, err := initListeners(m.Addr, config)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.listeners = listeners
	m.mu.Unlock()

	// All http requests start to be processed by httpHandler
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tlsEnabled && r.TLS == nil {
			// TLS is enabled but Request is not TLS configured
			u := url.URL{
				Scheme:   httpsScheme,
				Opaque:   r.URL.Opaque,
				User:     r.URL.User,
				Host:     r.Host,
				Path:     r.URL.Path,
				RawQuery: r.URL.RawQuery,
				Fragment: r.URL.Fragment,
			}
			http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
		} else {
			// Execute registered handlers, protect with a waitgroup
			// to accomplish a graceful shutdown when the user asks to quit
			m.gracefulWait.Add(1)
			m.handler.ServeHTTP(w, r)
			m.gracefulWait.Done()
		}
	})

	var wg = &sync.WaitGroup{}
	for _, listener := range listeners {
		wg.Add(1)
		go func(listener *ListenerMux) {
			defer wg.Done()
			serr := http.Serve(listener, httpHandler)
			// Do not print the error if the listener is closed.
			if !listener.IsClosed() {
				errorIf(serr, "Unable to serve incoming requests.")
			}
		}(listener)
	}
	// Wait for all http.Serve's to return.
	wg.Wait()
	return nil
}

// Close initiates the graceful shutdown
func (m *ServerMux) Close() error {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return errors.New("Server has been closed")
	}
	// Closed completely.
	m.closed = true

	// Close the listeners.
	for _, listener := range m.listeners {
		if err := listener.Close(); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()

	// Prepare for a graceful shutdown
	waitSignal := make(chan struct{})
	go func() {
		defer close(waitSignal)
		m.gracefulWait.Wait()
	}()

	select {
	// Wait for everything to be properly closed
	case <-waitSignal:
	// Forced shutdown
	case <-time.After(m.gracefulTimeout):
	}

	return nil
}
