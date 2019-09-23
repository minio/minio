/*
 * MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
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
)

// AccountingConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type AccountingConn struct {
	net.Conn
	updateBytesReadFunc    func(int) // function to be called to update bytes read.
	updateBytesWrittenFunc func(int) // function to be called to update bytes written.
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *AccountingConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if err == nil && c.updateBytesReadFunc != nil {
		c.updateBytesReadFunc(n)
	}

	return n, err
}

// Write - writes data to the connection.
func (c *AccountingConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if err == nil && c.updateBytesWrittenFunc != nil {
		c.updateBytesWrittenFunc(n)
	}

	return n, err
}

// newAccountingConn - creates a new connection object wrapping net.Conn with deadlines.
func newAccountingConn(c net.Conn, updateBytesReadFunc, updateBytesWrittenFunc func(int)) *AccountingConn {
	return &AccountingConn{
		Conn:                   c,
		updateBytesReadFunc:    updateBytesReadFunc,
		updateBytesWrittenFunc: updateBytesWrittenFunc,
	}
}
