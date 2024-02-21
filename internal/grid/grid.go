// Copyright (c) 2015-2023 MinIO, Inc.
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

// Package grid provides single-connection two-way grid communication.
package grid

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gobwas/ws/wsutil"
)

// ErrDisconnected is returned when the connection to the remote has been lost during the call.
var ErrDisconnected = RemoteErr("remote disconnected")

const (
	// minBufferSize is the minimum buffer size.
	// Buffers below this is not reused.
	minBufferSize = 1 << 10

	// defaultBufferSize is the default buffer allocation size.
	defaultBufferSize = 4 << 10

	// maxBufferSize is the maximum buffer size.
	// Buffers larger than this is not reused.
	maxBufferSize = 64 << 10

	// If there is a queue, merge up to this many messages.
	maxMergeMessages = 30

	// clientPingInterval will ping the remote handler every 15 seconds.
	// Clients disconnect when we exceed 2 intervals.
	clientPingInterval = 15 * time.Second

	// Deadline for single (non-streaming) requests to complete.
	// Used if no deadline is provided on context.
	defaultSingleRequestTimeout = time.Minute
)

var internalByteBuffer = sync.Pool{
	New: func() any {
		m := make([]byte, 0, defaultBufferSize)
		return &m
	},
}

// GetByteBuffer can be replaced with a function that returns a small
// byte buffer.
// When replacing PutByteBuffer should also be replaced
// There is no minimum size.
var GetByteBuffer = func() []byte {
	b := *internalByteBuffer.Get().(*[]byte)
	return b[:0]
}

// PutByteBuffer is for returning byte buffers.
var PutByteBuffer = func(b []byte) {
	if cap(b) >= minBufferSize && cap(b) < maxBufferSize {
		internalByteBuffer.Put(&b)
	}
}

// readAllInto reads from r and appends to b until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because readAllInto is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func readAllInto(b []byte, r *wsutil.Reader) ([]byte, error) {
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return b, err
		}
	}
}

// getDeadline will truncate the deadline so it is at least 1ms and at most MaxDeadline.
func getDeadline(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return 0
	}
	if d > MaxDeadline {
		return MaxDeadline
	}
	return d
}

type writerWrapper struct {
	ch  chan<- []byte
	ctx context.Context
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	buf := GetByteBuffer()
	if cap(buf) < len(p) {
		PutByteBuffer(buf)
		buf = make([]byte, len(p))
	}
	buf = buf[:len(p)]
	copy(buf, p)
	select {
	case w.ch <- buf:
		return len(p), nil
	case <-w.ctx.Done():
		return 0, context.Cause(w.ctx)
	}
}

// WriterToChannel will return an io.Writer that writes to the given channel.
// The context both allows returning errors on writes and to ensure that
// this isn't abandoned if the channel is no longer being read from.
func WriterToChannel(ctx context.Context, ch chan<- []byte) io.Writer {
	return &writerWrapper{ch: ch, ctx: ctx}
}

// bytesOrLength returns small (<=100b) byte slices as string, otherwise length.
func bytesOrLength(b []byte) string {
	if len(b) > 100 {
		return fmt.Sprintf("%d bytes", len(b))
	}
	return fmt.Sprint(b)
}

type lockedClientMap struct {
	m  map[uint64]*muxClient
	mu sync.Mutex
}

func (m *lockedClientMap) Load(id uint64) (*muxClient, bool) {
	m.mu.Lock()
	v, ok := m.m[id]
	m.mu.Unlock()
	return v, ok
}

func (m *lockedClientMap) LoadAndDelete(id uint64) (*muxClient, bool) {
	m.mu.Lock()
	v, ok := m.m[id]
	if ok {
		delete(m.m, id)
	}
	m.mu.Unlock()
	return v, ok
}

func (m *lockedClientMap) Size() int {
	m.mu.Lock()
	v := len(m.m)
	m.mu.Unlock()
	return v
}

func (m *lockedClientMap) Delete(id uint64) {
	m.mu.Lock()
	delete(m.m, id)
	m.mu.Unlock()
}

func (m *lockedClientMap) Range(fn func(key uint64, value *muxClient) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.m {
		if !fn(k, v) {
			break
		}
	}
}

func (m *lockedClientMap) Clear() {
	m.mu.Lock()
	m.m = map[uint64]*muxClient{}
	m.mu.Unlock()
}

func (m *lockedClientMap) LoadOrStore(id uint64, v *muxClient) (*muxClient, bool) {
	m.mu.Lock()
	v2, ok := m.m[id]
	if ok {
		m.mu.Unlock()
		return v2, true
	}
	m.m[id] = v
	m.mu.Unlock()
	return v, false
}

type lockedServerMap struct {
	m  map[uint64]*muxServer
	mu sync.Mutex
}

func (m *lockedServerMap) Load(id uint64) (*muxServer, bool) {
	m.mu.Lock()
	v, ok := m.m[id]
	m.mu.Unlock()
	return v, ok
}

func (m *lockedServerMap) LoadAndDelete(id uint64) (*muxServer, bool) {
	m.mu.Lock()
	v, ok := m.m[id]
	if ok {
		delete(m.m, id)
	}
	m.mu.Unlock()
	return v, ok
}

func (m *lockedServerMap) Size() int {
	m.mu.Lock()
	v := len(m.m)
	m.mu.Unlock()
	return v
}

func (m *lockedServerMap) Delete(id uint64) {
	m.mu.Lock()
	delete(m.m, id)
	m.mu.Unlock()
}

func (m *lockedServerMap) Range(fn func(key uint64, value *muxServer) bool) {
	m.mu.Lock()
	for k, v := range m.m {
		if !fn(k, v) {
			break
		}
	}
	m.mu.Unlock()
}

func (m *lockedServerMap) Clear() {
	m.mu.Lock()
	m.m = map[uint64]*muxServer{}
	m.mu.Unlock()
}

func (m *lockedServerMap) LoadOrStore(id uint64, v *muxServer) (*muxServer, bool) {
	m.mu.Lock()
	v2, ok := m.m[id]
	if ok {
		m.mu.Unlock()
		return v2, true
	}
	m.m[id] = v
	m.mu.Unlock()
	return v, false
}

func (m *lockedServerMap) LoadOrCompute(id uint64, fn func() *muxServer) (*muxServer, bool) {
	m.mu.Lock()
	v2, ok := m.m[id]
	if ok {
		m.mu.Unlock()
		return v2, true
	}
	v := fn()
	m.m[id] = v
	m.mu.Unlock()
	return v, false
}
