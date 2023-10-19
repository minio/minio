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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gobwas/ws/wsutil"
)

// ErrDisconnected is returned when the connection to the remote has been lost during the call.
var ErrDisconnected = errors.New("remote disconnected")

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
	maxMergeMessages = 20

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
	ch chan<- []byte
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	buf := GetByteBuffer()
	if cap(buf) < len(p) {
		PutByteBuffer(buf)
		buf = make([]byte, len(p))
	}
	buf = buf[:len(p)]
	copy(buf, p)
	w.ch <- buf
	return len(p), nil
}

// WriterToChannel will return an io.Writer that writes to the given channel.
func WriterToChannel(ch chan<- []byte) io.Writer {
	return &writerWrapper{ch: ch}
}

// bytesOrLength returns small (<=100b) byte slices as string, otherwise length.
func bytesOrLength(b []byte) string {
	if len(b) > 100 {
		return fmt.Sprintf("%d bytes", len(b))
	}
	return fmt.Sprint(b)
}
