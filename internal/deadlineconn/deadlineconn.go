// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Package deadlineconn implements net.Conn wrapper with configured deadlines.
package deadlineconn

import (
	"net"
	"time"
)

// DeadlineConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type DeadlineConn struct {
	net.Conn
	readDeadline  time.Duration // sets the read deadline on a connection.
	writeDeadline time.Duration // sets the write deadline on a connection.
}

// Sets read deadline
func (c *DeadlineConn) setReadDeadline() {
	if c.readDeadline > 0 {
		c.SetReadDeadline(time.Now().UTC().Add(c.readDeadline))
	}
}

func (c *DeadlineConn) setWriteDeadline() {
	if c.writeDeadline > 0 {
		c.SetWriteDeadline(time.Now().UTC().Add(c.writeDeadline))
	}
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *DeadlineConn) Read(b []byte) (n int, err error) {
	c.setReadDeadline()
	n, err = c.Conn.Read(b)
	return n, err
}

// Write - writes data to the connection.
func (c *DeadlineConn) Write(b []byte) (n int, err error) {
	c.setWriteDeadline()
	n, err = c.Conn.Write(b)
	return n, err
}

// WithReadDeadline sets a new read side net.Conn deadline.
func (c *DeadlineConn) WithReadDeadline(d time.Duration) *DeadlineConn {
	c.readDeadline = d
	return c
}

// WithWriteDeadline sets a new write side net.Conn deadline.
func (c *DeadlineConn) WithWriteDeadline(d time.Duration) *DeadlineConn {
	c.writeDeadline = d
	return c
}

// New - creates a new connection object wrapping net.Conn with deadlines.
func New(c net.Conn) *DeadlineConn {
	return &DeadlineConn{
		Conn: c,
	}
}
