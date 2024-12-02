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
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// updateInterval is the minimum time between deadline updates.
const updateInterval = 250 * time.Millisecond

// DeadlineConn - is a generic stream-oriented network connection supporting buffered reader and read/write timeout.
type DeadlineConn struct {
	net.Conn
	readDeadline            time.Duration // sets the read deadline on a connection.
	readSetAt               time.Time
	writeDeadline           time.Duration // sets the write deadline on a connection.
	writeSetAt              time.Time
	abortReads, abortWrites atomic.Bool // A deadline was set to indicate caller wanted the conn to time out.
	infReads, infWrites     atomic.Bool
	mu                      sync.Mutex
}

// Unwrap will unwrap the connection and remove the deadline if applied.
// If not a *DeadlineConn, the unmodified net.Conn is returned.
func Unwrap(c net.Conn) net.Conn {
	if dc, ok := c.(*DeadlineConn); ok {
		return dc.Conn
	}
	return c
}

// Sets read deadline
func (c *DeadlineConn) setReadDeadline() {
	// Do not set a Read deadline, if upstream wants to cancel all reads.
	if c.readDeadline <= 0 || c.abortReads.Load() || c.infReads.Load() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abortReads.Load() {
		return
	}

	now := time.Now()
	if now.Sub(c.readSetAt) > updateInterval {
		c.Conn.SetReadDeadline(now.Add(c.readDeadline + updateInterval))
		c.readSetAt = now
	}
}

func (c *DeadlineConn) setWriteDeadline() {
	// Do not set a Write deadline, if upstream wants to cancel all reads.
	if c.writeDeadline <= 0 || c.abortWrites.Load() || c.infWrites.Load() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.abortWrites.Load() {
		return
	}
	now := time.Now()
	if now.Sub(c.writeSetAt) > updateInterval {
		c.Conn.SetWriteDeadline(now.Add(c.writeDeadline + updateInterval))
		c.writeSetAt = now
	}
}

// Read - reads data from the connection using wrapped buffered reader.
func (c *DeadlineConn) Read(b []byte) (n int, err error) {
	if c.abortReads.Load() {
		return 0, context.DeadlineExceeded
	}
	c.setReadDeadline()
	n, err = c.Conn.Read(b)
	return n, err
}

// Write - writes data to the connection.
func (c *DeadlineConn) Write(b []byte) (n int, err error) {
	if c.abortWrites.Load() {
		return 0, context.DeadlineExceeded
	}
	c.setWriteDeadline()
	n, err = c.Conn.Write(b)
	return n, err
}

// SetDeadline will set the deadline for reads and writes.
// A zero value for t means I/O operations will not time out.
func (c *DeadlineConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.readSetAt = time.Time{}
	c.writeSetAt = time.Time{}
	c.abortReads.Store(!t.IsZero() && time.Until(t) < 0)
	c.abortWrites.Store(!t.IsZero() && time.Until(t) < 0)
	c.infReads.Store(t.IsZero())
	c.infWrites.Store(t.IsZero())
	return c.Conn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *DeadlineConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abortReads.Store(!t.IsZero() && time.Until(t) < 0)
	c.infReads.Store(t.IsZero())
	c.readSetAt = time.Time{}
	return c.Conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (c *DeadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abortWrites.Store(!t.IsZero() && time.Until(t) < 0)
	c.infWrites.Store(t.IsZero())
	c.writeSetAt = time.Time{}
	return c.Conn.SetWriteDeadline(t)
}

// Close wraps conn.Close and stops sending deadline updates.
func (c *DeadlineConn) Close() error {
	c.abortReads.Store(true)
	c.abortWrites.Store(true)
	return c.Conn.Close()
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
