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
	"bufio"
	"net"
	"time"
)

// bufConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type bufConn struct {
	net.Conn
	bufReader              *bufio.Reader // buffered reader wraps reader in net.Conn.
	readTimeout            time.Duration // sets the read timeout in the connection.
	writeTimeout           time.Duration // sets the write timeout in the connection.
	updateBytesReadFunc    func(int)     // function to be called to update bytes read.
	updateBytesWrittenFunc func(int)     // function to be called to update bytes written.
}

func (c *bufConn) setReadTimeout() {
	if c.readTimeout != 0 {
		c.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
}

func (c *bufConn) setWriteTimeout() {
	if c.writeTimeout != 0 {
		c.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
}

// Peek - returns the next n bytes without advancing the reader.  It just wraps bufio.Reader.Peek().
func (c *bufConn) Peek(n int) ([]byte, error) {
	c.setReadTimeout()
	return c.bufReader.Peek(n)
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *bufConn) Read(b []byte) (n int, err error) {
	c.setReadTimeout()
	n, err = c.bufReader.Read(b)

	if c.updateBytesReadFunc != nil {
		c.updateBytesReadFunc(n)
	}

	return n, err
}

// Write - writes data to the connection.
func (c *bufConn) Write(b []byte) (n int, err error) {
	c.setWriteTimeout()
	n, err = c.Conn.Write(b)

	if c.updateBytesWrittenFunc != nil {
		c.updateBytesWrittenFunc(n)
	}

	return n, err
}

// newBufConn - creates a new connection object wrapping net.Conn.
func newBufConn(c net.Conn, readTimeout, writeTimeout time.Duration,
	updateBytesReadFunc func(int), updateBytesWrittenFunc func(int)) *bufConn {
	return &bufConn{
		Conn:                   c,
		bufReader:              bufio.NewReader(c),
		readTimeout:            readTimeout,
		writeTimeout:           writeTimeout,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
	}
}
