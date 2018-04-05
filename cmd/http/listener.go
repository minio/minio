/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

var sslRequiredErrMsg = []byte("HTTP/1.0 403 Forbidden\r\n\r\nSSL required")

// HTTP methods.
var methods = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodConnect,
	http.MethodOptions,
	http.MethodTrace,
	"PRI", // HTTP 2 method
}

// maximum length of above methods + one space.
var methodMaxLen = getMethodMaxLen() + 1

func getMethodMaxLen() int {
	maxLen := 0
	for _, method := range methods {
		if len(method) > maxLen {
			maxLen = len(method)
		}
	}

	return maxLen
}

func isHTTPMethod(s string) bool {
	for _, method := range methods {
		if s == method {
			return true
		}
	}

	return false
}

type acceptResult struct {
	conn net.Conn
	err  error
}

// httpListener - HTTP listener capable of handling multiple server addresses.
type httpListener struct {
	mutex                  sync.Mutex         // to guard Close() method.
	tcpListeners           []*net.TCPListener // underlaying TCP listeners.
	acceptCh               chan acceptResult  // channel where all TCP listeners write accepted connection.
	doneCh                 chan struct{}      // done channel for TCP listener goroutines.
	tlsConfig              *tls.Config        // TLS configuration
	tcpKeepAliveTimeout    time.Duration
	readTimeout            time.Duration
	writeTimeout           time.Duration
	updateBytesReadFunc    func(int)                           // function to be called to update bytes read in BufConn.
	updateBytesWrittenFunc func(int)                           // function to be called to update bytes written in BufConn.
	errorLogFunc           func(error, string, ...interface{}) // function to be called on errors.
}

// isRoutineNetErr returns true if error is due to a network timeout,
// connect reset or io.EOF and false otherwise
func isRoutineNetErr(err error) bool {
	if nErr, ok := err.(*net.OpError); ok {
		// Check if the error is a tcp connection reset
		if syscallErr, ok := nErr.Err.(*os.SyscallError); ok {
			if errno, ok := syscallErr.Err.(syscall.Errno); ok {
				return errno == syscall.ECONNRESET
			}
		}
		// Check if the error is a timeout
		return nErr.Timeout()
	}
	return err == io.EOF
}

// start - starts separate goroutine for each TCP listener.  A valid insecure/TLS HTTP new connection is passed to httpListener.acceptCh.
func (listener *httpListener) start() {
	listener.acceptCh = make(chan acceptResult)
	listener.doneCh = make(chan struct{})

	// Closure to send acceptResult to acceptCh.
	// It returns true if the result is sent else false if returns when doneCh is closed.
	send := func(result acceptResult, doneCh <-chan struct{}) bool {
		select {
		case listener.acceptCh <- result:
			// Successfully written to acceptCh
			return true
		case <-doneCh:
			// As stop signal is received, close accepted connection.
			if result.conn != nil {
				result.conn.Close()
			}
			return false
		}
	}

	// Closure to handle single connection.
	handleConn := func(tcpConn *net.TCPConn, doneCh <-chan struct{}) {
		// Tune accepted TCP connection.
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(listener.tcpKeepAliveTimeout)

		bufconn := newBufConn(tcpConn, listener.readTimeout, listener.writeTimeout,
			listener.updateBytesReadFunc, listener.updateBytesWrittenFunc)

		// Peek bytes of maximum length of all HTTP methods.
		data, err := bufconn.Peek(methodMaxLen)
		if err != nil {
			if listener.errorLogFunc != nil {
				// Peek could fail legitimately when clients abruptly close
				// connection. E.g. Chrome browser opens connections speculatively to
				// speed up loading of a web page. Peek may also fail due to network
				// saturation on a transport with read timeout set. All other kind of
				// errors should be logged for further investigation. Thanks @brendanashworth.
				if !isRoutineNetErr(err) {
					listener.errorLogFunc(err,
						"Error in reading from new connection %s at server %s",
						bufconn.RemoteAddr(), bufconn.LocalAddr())
				}
			}
			bufconn.Close()
			return
		}

		// Return bufconn if read data is a valid HTTP method.
		tokens := strings.SplitN(string(data), " ", 2)
		if isHTTPMethod(tokens[0]) {
			if listener.tlsConfig == nil {
				send(acceptResult{bufconn, nil}, doneCh)
			} else {
				// As TLS is configured and we got plain text HTTP request,
				// return 403 (forbidden) error.
				bufconn.Write(sslRequiredErrMsg)
				bufconn.Close()
			}
			return
		}

		if listener.tlsConfig != nil {
			// As the listener is configured with TLS, try to do TLS handshake, drop the connection if it fails.
			tlsConn := tls.Server(bufconn, listener.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				if listener.errorLogFunc != nil {
					listener.errorLogFunc(err,
						"TLS handshake failed with new connection %s at server %s",
						bufconn.RemoteAddr(), bufconn.LocalAddr())
				}
				bufconn.Close()
				return
			}

			// Check whether the connection contains HTTP request or not.
			bufconn = newBufConn(tlsConn, listener.readTimeout, listener.writeTimeout,
				listener.updateBytesReadFunc, listener.updateBytesWrittenFunc)

			// Peek bytes of maximum length of all HTTP methods.
			data, err := bufconn.Peek(methodMaxLen)
			if err != nil {
				if !isRoutineNetErr(err) && listener.errorLogFunc != nil {
					listener.errorLogFunc(err,
						"Error in reading from new TLS connection %s at server %s",
						bufconn.RemoteAddr(), bufconn.LocalAddr())
				}
				bufconn.Close()
				return
			}

			// Return bufconn if read data is a valid HTTP method.
			tokens := strings.SplitN(string(data), " ", 2)
			if isHTTPMethod(tokens[0]) {
				send(acceptResult{bufconn, nil}, doneCh)
				return
			}
		}

		if listener.errorLogFunc != nil {
			listener.errorLogFunc(errors.New("junk message"),
				"Received non-HTTP message from new connection %s at server %s",
				bufconn.RemoteAddr(), bufconn.LocalAddr())
		}

		bufconn.Close()
		return
	}

	// Closure to handle TCPListener until done channel is closed.
	handleListener := func(tcpListener *net.TCPListener, doneCh <-chan struct{}) {
		for {
			tcpConn, err := tcpListener.AcceptTCP()
			if err != nil {
				// Returns when send fails.
				if !send(acceptResult{nil, err}, doneCh) {
					return
				}
			} else {
				go handleConn(tcpConn, doneCh)
			}
		}
	}

	// Start separate goroutine for each TCP listener to handle connection.
	for _, tcpListener := range listener.tcpListeners {
		go handleListener(tcpListener, listener.doneCh)
	}
}

// Accept - reads from httpListener.acceptCh for one of previously accepted TCP connection and returns the same.
func (listener *httpListener) Accept() (conn net.Conn, err error) {
	result, ok := <-listener.acceptCh
	if ok {
		return result.conn, result.err
	}

	return nil, syscall.EINVAL
}

// Close - closes underneath all TCP listeners.
func (listener *httpListener) Close() (err error) {
	listener.mutex.Lock()
	defer listener.mutex.Unlock()
	if listener.doneCh == nil {
		return syscall.EINVAL
	}

	for i := range listener.tcpListeners {
		listener.tcpListeners[i].Close()
	}
	close(listener.doneCh)

	listener.doneCh = nil
	return nil
}

// Addr - net.Listener interface compatible method returns net.Addr.  In case of multiple TCP listeners, it returns '0.0.0.0' as IP address.
func (listener *httpListener) Addr() (addr net.Addr) {
	addr = listener.tcpListeners[0].Addr()
	if len(listener.tcpListeners) == 1 {
		return addr
	}

	tcpAddr := addr.(*net.TCPAddr)
	if ip := net.ParseIP("0.0.0.0"); ip != nil {
		tcpAddr.IP = ip
	}

	addr = tcpAddr
	return addr
}

// Addrs - returns all address information of TCP listeners.
func (listener *httpListener) Addrs() (addrs []net.Addr) {
	for i := range listener.tcpListeners {
		addrs = append(addrs, listener.tcpListeners[i].Addr())
	}

	return addrs
}

// newHTTPListener - creates new httpListener object which is interface compatible to net.Listener.
// httpListener is capable to
// * listen to multiple addresses
// * controls incoming connections only doing HTTP protocol
func newHTTPListener(serverAddrs []string,
	tlsConfig *tls.Config,
	tcpKeepAliveTimeout time.Duration,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	updateBytesReadFunc func(int),
	updateBytesWrittenFunc func(int),
	errorLogFunc func(error, string, ...interface{})) (listener *httpListener, err error) {

	var tcpListeners []*net.TCPListener
	// Close all opened listeners on error
	defer func() {
		if err == nil {
			return
		}

		for _, tcpListener := range tcpListeners {
			// Ignore error on close.
			tcpListener.Close()
		}
	}()

	for _, serverAddr := range serverAddrs {
		var l net.Listener
		if l, err = net.Listen("tcp", serverAddr); err != nil {
			return nil, err
		}

		tcpListener, ok := l.(*net.TCPListener)
		if !ok {
			return nil, fmt.Errorf("unexpected listener type found %v, expected net.TCPListener", l)
		}

		tcpListeners = append(tcpListeners, tcpListener)
	}

	listener = &httpListener{
		tcpListeners:           tcpListeners,
		tlsConfig:              tlsConfig,
		tcpKeepAliveTimeout:    tcpKeepAliveTimeout,
		readTimeout:            readTimeout,
		writeTimeout:           writeTimeout,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
		errorLogFunc:           errorLogFunc,
	}
	listener.start()

	return listener, nil
}
