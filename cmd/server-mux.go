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

// PeekProtocol - reads the first bytes, then checks if it is similar
// to one of the default http methods
func (c *ConnMux) PeekProtocol() string {
	buf, err := c.bufrw.Peek(maxHTTPVerbLen)
	if err != nil {
		errorIf(err, "Unable to peek into the protocol")
		return "http"
	}
	for _, m := range defaultHTTP1Methods {
		if strings.HasPrefix(string(buf), m) {
			return "http"
		}
	}
	for _, m := range defaultHTTP2Methods {
		if strings.HasPrefix(string(buf), m) {
			return "http2"
		}
	}
	return "tls"
}

// Read - streams the ConnMux buffer when reset flag is activated, otherwise
// streams from the incoming network connection
func (c *ConnMux) Read(b []byte) (int, error) {
	return c.bufrw.Read(b)
}

// Close the connection.
func (c *ConnMux) Close() (err error) {
	if err = c.bufrw.Flush(); err != nil {
		return err
	}
	return c.Conn.Close()
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
		// Loop for accepting new connections
		for {
			conn, err := l.Listener.Accept()
			if err != nil {
				l.acceptResCh <- ListenerMuxAcceptRes{err: err}
				return
			}
			// Wrap the connection with ConnMux to be able to peek the data in the incoming connection
			// and decide if we need to wrap the connection itself with a TLS or not
			go func(conn net.Conn) {
				connMux := NewConnMux(conn)
				if connMux.PeekProtocol() == "tls" {
					l.acceptResCh <- ListenerMuxAcceptRes{conn: tls.Server(connMux, l.config)}
				} else {
					l.acceptResCh <- ListenerMuxAcceptRes{conn: connMux}
				}
			}(conn)
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
	*http.Server
	listeners       []*ListenerMux
	WaitGroup       *sync.WaitGroup
	GracefulTimeout time.Duration
	mu              sync.Mutex // guards closed, conns, and listener
	closed          bool
	conns           map[net.Conn]http.ConnState // except terminal states
}

// NewServerMux constructor to create a ServerMux
func NewServerMux(addr string, handler http.Handler) *ServerMux {
	m := &ServerMux{
		Server: &http.Server{
			Addr: addr,
			// Do not add any timeouts Golang net.Conn
			// closes connections right after 10mins even
			// if they are not idle.
			Handler:        handler,
			MaxHeaderBytes: 1 << 20,
		},
		WaitGroup: &sync.WaitGroup{},
		// Wait for 5 seconds for new incoming connnections, otherwise
		// forcibly close them during graceful stop or restart.
		GracefulTimeout: 5 * time.Second,
	}

	// Track connection state
	m.connState()

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

	config := &tls.Config{} // Always instantiate.

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

	listeners, err := initListeners(m.Server.Addr, config)
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
				Scheme:   "https",
				Opaque:   r.URL.Opaque,
				User:     r.URL.User,
				Host:     r.Host,
				Path:     r.URL.Path,
				RawQuery: r.URL.RawQuery,
				Fragment: r.URL.Fragment,
			}
			http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
		} else {
			// Execute registered handlers
			m.Server.Handler.ServeHTTP(w, r)
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

	m.SetKeepAlivesEnabled(false)
	// Force close any idle and new connections. Waiting for other connections
	// to close on their own (within the timeout period)
	for c, st := range m.conns {
		if st == http.StateIdle || st == http.StateNew {
			c.Close()
		}
	}

	// If the GracefulTimeout happens then forcefully close all connections
	t := time.AfterFunc(m.GracefulTimeout, func() {
		for c := range m.conns {
			c.Close()
		}
	})

	// Wait for graceful timeout of connections.
	defer t.Stop()

	m.mu.Unlock()

	// Block until all connections are closed
	m.WaitGroup.Wait()

	return nil
}

// connState setups the ConnState tracking hook to know which connections are idle
func (m *ServerMux) connState() {
	// Set our ConnState to track idle connections
	m.Server.ConnState = func(c net.Conn, cs http.ConnState) {
		m.mu.Lock()
		defer m.mu.Unlock()

		switch cs {
		case http.StateNew:
			// New connections increment the WaitGroup and are added the the conns dictionary
			m.WaitGroup.Add(1)
			if m.conns == nil {
				m.conns = make(map[net.Conn]http.ConnState)
			}
			m.conns[c] = cs
		case http.StateActive:
			// Only update status to StateActive if it's in the conns dictionary
			if _, ok := m.conns[c]; ok {
				m.conns[c] = cs
			}
		case http.StateIdle:
			// Only update status to StateIdle if it's in the conns dictionary
			if _, ok := m.conns[c]; ok {
				m.conns[c] = cs
			}

			// If we've already closed then we need to close this connection.
			// We don't allow connections to become idle after server is closed
			if m.closed {
				c.Close()
			}
		case http.StateHijacked, http.StateClosed:
			// If the connection is hijacked or closed we forget it
			m.forgetConn(c)
		}
	}
}

// forgetConn removes c from conns and decrements WaitGroup
func (m *ServerMux) forgetConn(c net.Conn) {
	if _, ok := m.conns[c]; ok {
		delete(m.conns, c)
		m.WaitGroup.Done()
	}
}
