/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"net"
	"time"
)

// DeadlineConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type DeadlineConn struct {
	net.Conn
	readTimeout            time.Duration // sets the read timeout in the connection.
	writeTimeout           time.Duration // sets the write timeout in the connection.
	updateBytesReadFunc    func(int)     // function to be called to update bytes read.
	updateBytesWrittenFunc func(int)     // function to be called to update bytes written.
}

// Sets read timeout
func (c *DeadlineConn) setReadTimeout() {
	if c.readTimeout != 0 {
		c.SetReadDeadline(time.Now().UTC().Add(c.readTimeout))
	}
}

func (c *DeadlineConn) setWriteTimeout() {
	if c.writeTimeout != 0 {
		c.SetWriteDeadline(time.Now().UTC().Add(c.writeTimeout))
	}
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *DeadlineConn) Read(b []byte) (n int, err error) {
	c.setReadTimeout()
	n, err = c.Conn.Read(b)
	if err == nil && c.updateBytesReadFunc != nil {
		c.updateBytesReadFunc(n)
	}

	return n, err
}

// Write - writes data to the connection.
func (c *DeadlineConn) Write(b []byte) (n int, err error) {
	c.setWriteTimeout()
	n, err = c.Conn.Write(b)
	if err == nil && c.updateBytesWrittenFunc != nil {
		c.updateBytesWrittenFunc(n)
	}

	return n, err
}

// newDeadlineConn - creates a new connection object wrapping net.Conn with deadlines.
func newDeadlineConn(c net.Conn, readTimeout, writeTimeout time.Duration, updateBytesReadFunc, updateBytesWrittenFunc func(int)) *DeadlineConn {
	return &DeadlineConn{
		Conn:                   c,
		readTimeout:            readTimeout,
		writeTimeout:           writeTimeout,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
	}
}
