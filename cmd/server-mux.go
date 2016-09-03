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

// ConnBuf - contains network buffer to record data
type ConnBuf struct {
	buffer []byte
	unRead bool
	offset int
}

// ConnMux - implements a Read() which streams twice the firs bytes from
// the incoming connection, to help peeking protocol
type ConnMux struct {
	net.Conn
	lastError error
	dataBuf   ConnBuf
}

func longestWord(strings []string) int {
	maxLen := 0
	for _, m := range defaultHTTP1Methods {
		if maxLen < len(m) {
			maxLen = len(m)
		}
	}
	for _, m := range defaultHTTP2Methods {
		if maxLen < len(m) {
			maxLen = len(m)
		}
	}

	return maxLen
}

// NewConnMux - creates a new ConnMux instance
func NewConnMux(c net.Conn) *ConnMux {
	h1 := longestWord(defaultHTTP1Methods)
	h2 := longestWord(defaultHTTP2Methods)
	max := h1
	if h2 > max {
		max = h2
	}
	return &ConnMux{Conn: c, dataBuf: ConnBuf{buffer: make([]byte, max+1)}}
}

// PeekProtocol - reads the first bytes, then checks if it is similar
// to one of the default http methods
func (c *ConnMux) PeekProtocol() string {
	var n int
	n, c.lastError = c.Conn.Read(c.dataBuf.buffer)
	if n == 0 || (c.lastError != nil && c.lastError != io.EOF) {
		return ""
	}
	c.dataBuf.unRead = true
	for _, m := range defaultHTTP1Methods {
		if strings.HasPrefix(string(c.dataBuf.buffer), m) {
			return "http"
		}
	}
	for _, m := range defaultHTTP2Methods {
		if strings.HasPrefix(string(c.dataBuf.buffer), m) {
			return "http2"
		}
	}
	return "tls"
}

// Read - streams the ConnMux buffer when reset flag is activated, otherwise
// streams from the incoming network connection
func (c *ConnMux) Read(b []byte) (int, error) {
	if c.dataBuf.unRead {
		n := copy(b, c.dataBuf.buffer[c.dataBuf.offset:])
		c.dataBuf.offset += n
		if c.dataBuf.offset == len(c.dataBuf.buffer) {
			// We finished copying all c.buffer, reset all
			c.dataBuf.unRead = false
			c.dataBuf.offset = 0
			c.dataBuf.buffer = c.dataBuf.buffer[:]
			if n < len(b) {
				// Continue copying from socket if b still has room for data
				tmpBuffer := make([]byte, len(b)-n-1)
				nr, err := c.Conn.Read(tmpBuffer)
				for idx, val := range tmpBuffer {
					b[n+idx] = val
				}
				return n + nr, err
			}
		}
		// We here return the last error
		return n, c.lastError
	}
	return c.Conn.Read(b)
}

// ListenerMux - encapuslates the standard net.Listener to inspect
// the communication protocol upon network connection
type ListenerMux struct {
	net.Listener
	config *tls.Config
}

// Accept - peek the protocol to decide if we should wrap the
// network stream with the TLS server
func (l *ListenerMux) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return conn, err
	}
	connMux := NewConnMux(conn)
	protocol := connMux.PeekProtocol()
	if protocol == "tls" {
		return tls.Server(connMux, l.config), nil
	}
	return connMux, nil
}

// Close Listener
func (l *ListenerMux) Close() error {
	if l == nil {
		return nil
	}
	return l.Listener.Close()
}

// ServerMux - the main mux server
type ServerMux struct {
	http.Server
	listener        *ListenerMux
	WaitGroup       *sync.WaitGroup
	GracefulTimeout time.Duration
	mu              sync.Mutex // guards closed, conns, and listener
	closed          bool
	conns           map[net.Conn]http.ConnState // except terminal states
}

// NewServerMux constructor to create a ServerMux
func NewServerMux(addr string, handler http.Handler) *ServerMux {
	m := &ServerMux{
		Server: http.Server{
			Addr: addr,
			// Do not add any timeouts Golang net.Conn
			// closes connections right after 10mins even
			// if they are not idle.
			Handler:        handler,
			MaxHeaderBytes: 1 << 20,
		},
		WaitGroup:       &sync.WaitGroup{},
		GracefulTimeout: 5 * time.Second,
	}

	// Track connection state
	m.connState()

	// Returns configured HTTP server.
	return m
}

// ListenAndServeTLS - similar to the http.Server version. However, it has the
// ability to redirect http requests to the correct HTTPS url if the client
// mistakenly initiates a http connection over the https port
func (m *ServerMux) ListenAndServeTLS(certFile, keyFile string) error {
	listener, err := net.Listen("tcp", m.Server.Addr)
	if err != nil {
		return err
	}

	config := &tls.Config{} // Always instantiate.
	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1", "h2"}
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	listenerMux := &ListenerMux{Listener: listener, config: config}

	m.mu.Lock()
	m.listener = listenerMux
	m.mu.Unlock()

	err = http.Serve(listenerMux,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// We reach here when ListenerMux.ConnMux is not wrapped with tls.Server
			if r.TLS == nil {
				u := url.URL{
					Scheme:   "https",
					Opaque:   r.URL.Opaque,
					User:     r.URL.User,
					Host:     r.Host,
					Path:     r.URL.Path,
					RawQuery: r.URL.RawQuery,
					Fragment: r.URL.Fragment,
				}
				http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
			} else {
				// Execute registered handlers
				m.Server.Handler.ServeHTTP(w, r)
			}
		}))
	return err
}

// ListenAndServe - Same as the http.Server version
func (m *ServerMux) ListenAndServe() error {
	listener, err := net.Listen("tcp", m.Server.Addr)
	if err != nil {
		return err
	}

	listenerMux := &ListenerMux{Listener: listener, config: &tls.Config{}}

	m.mu.Lock()
	m.listener = listenerMux
	m.mu.Unlock()

	return m.Server.Serve(listenerMux)
}

// Close initiates the graceful shutdown
func (m *ServerMux) Close() error {
	m.mu.Lock()
	if m.closed {
		return errors.New("Server has been closed")
	}
	m.closed = true

	// Make sure a listener was set
	if err := m.listener.Close(); err != nil {
		return err
	}

	m.SetKeepAlivesEnabled(false)
	for c, st := range m.conns {
		// Force close any idle and new connections. Waiting for other connections
		// to close on their own (within the timeout period)
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
