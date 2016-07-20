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

package main

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
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

// MuxConn - implements a Read() which streams twice the firs bytes from
// the incoming connection, to help peeking protocol
type MuxConn struct {
	net.Conn
	lastError error
	dataBuf   ConnBuf
}

// NewMuxConn - creates a new MuxConn instance
func NewMuxConn(c net.Conn) *MuxConn {
	h1 := longestWord(defaultHTTP1Methods)
	h2 := longestWord(defaultHTTP2Methods)
	max := h1
	if h2 > max {
		max = h2
	}
	return &MuxConn{Conn: c, dataBuf: ConnBuf{buffer: make([]byte, max+1)}}
}

// PeekProtocol - reads the first bytes, then checks if it is similar
// to one of the default http methods
func (c *MuxConn) PeekProtocol() string {
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

// Read - streams the MuxConn buffer when reset flag is activated, otherwise
// streams from the incoming network connection
func (c *MuxConn) Read(b []byte) (int, error) {
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

// MuxListener - encapuslates the standard net.Listener to inspect
// the communication protocol upon network connection
type MuxListener struct {
	net.Listener
	config *tls.Config
}

// NewMuxListener - creates new MuxListener, returns error when cert/key files are not found
// or invalid
func NewMuxListener(listener net.Listener, certPath, keyPath string) (MuxListener, error) {
	var err error
	config := &tls.Config{}
	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1", "h2"}
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return MuxListener{}, err
	}
	return MuxListener{Listener: listener, config: config}, nil
}

// Accept - peek the protocol to decide if we should wrap the
// network stream with the TLS server
func (l MuxListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return c, err
	}
	cmux := NewMuxConn(c)
	protocol := cmux.PeekProtocol()
	if protocol == "tls" {
		return tls.Server(cmux, l.config), nil
	}
	return cmux, nil
}

// MuxServer - the main mux server
type MuxServer struct {
	http.Server
}

// ListenAndServeTLS - similar to the http.Server version. However, it has the
// ability to redirect http requests to the correct HTTPS url if the client
// mistakenly initiates a http connection over the https port
func (m *MuxServer) ListenAndServeTLS(certFile, keyFile string) error {
	listener, err := net.Listen("tcp", m.Server.Addr)
	if err != nil {
		return err
	}
	mux, err := NewMuxListener(listener, mustGetCertFile(), mustGetKeyFile())
	if err != nil {
		return err
	}
	err = http.Serve(mux,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// We reach here when MuxListener.MuxConn is not wrapped with tls.Server
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
func (m *MuxServer) ListenAndServe() error {
	return m.Server.ListenAndServe()
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
