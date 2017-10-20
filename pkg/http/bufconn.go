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

// BufConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type BufConn struct {
	QuirkConn
	bufReader              *bufio.Reader // buffered reader wraps reader in net.Conn.
	readTimeout            time.Duration // sets the read timeout in the connection.
	writeTimeout           time.Duration // sets the write timeout in the connection.
	updateBytesReadFunc    func(int)     // function to be called to update bytes read.
	updateBytesWrittenFunc func(int)     // function to be called to update bytes written.
}

// Sets read timeout
func (c *BufConn) setReadTimeout() {
	if c.readTimeout != 0 && c.canSetReadDeadline() {
		c.SetReadDeadline(time.Now().UTC().Add(c.readTimeout))
	}
}

func (c *BufConn) setWriteTimeout() {
	if c.writeTimeout != 0 {
		c.SetWriteDeadline(time.Now().UTC().Add(c.writeTimeout))
	}
}

// RemoveTimeout - removes all configured read and write
// timeouts. Used by callers which control net.Conn behavior
// themselves.
func (c *BufConn) RemoveTimeout() {
	c.readTimeout = 0
	c.writeTimeout = 0
	// Unset read/write timeouts, since we use **bufio** it is not
	// guaranteed that the underlying Peek/Read operation in-fact
	// indeed performed a Read() operation on the network. With
	// that in mind we need to unset any timeouts currently set to
	// avoid any pre-mature timeouts.
	c.SetDeadline(time.Time{})
}

// Peek - returns the next n bytes without advancing the reader.  It just wraps bufio.Reader.Peek().
func (c *BufConn) Peek(n int) ([]byte, error) {
	c.setReadTimeout()
	return c.bufReader.Peek(n)
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *BufConn) Read(b []byte) (n int, err error) {
	c.setReadTimeout()
	n, err = c.bufReader.Read(b)
	if err == nil && c.updateBytesReadFunc != nil {
		c.updateBytesReadFunc(n)
	}

	return n, err
}

// Write - writes data to the connection.
func (c *BufConn) Write(b []byte) (n int, err error) {
	c.setWriteTimeout()
	n, err = c.Conn.Write(b)
	if err == nil && c.updateBytesWrittenFunc != nil {
		c.updateBytesWrittenFunc(n)
	}

	return n, err
}

// newBufConn - creates a new connection object wrapping net.Conn.
func newBufConn(c net.Conn, readTimeout, writeTimeout time.Duration,
	updateBytesReadFunc, updateBytesWrittenFunc func(int)) *BufConn {
	return &BufConn{
		QuirkConn:              QuirkConn{Conn: c},
		bufReader:              bufio.NewReader(c),
		readTimeout:            readTimeout,
		writeTimeout:           writeTimeout,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
	}
}
