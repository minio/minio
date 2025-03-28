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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/minio/minio/internal/bpool"
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
	maxBufferSize = 96 << 10

	// This is the assumed size of bigger buffers and allocation size.
	biggerBufMin = 32 << 10

	// This is the maximum size of bigger buffers.
	biggerBufMax = maxBufferSize

	// If there is a queue, merge up to this many messages.
	maxMergeMessages = 50

	// clientPingInterval will ping the remote handler every 15 seconds.
	// Clients disconnect when we exceed 2 intervals.
	clientPingInterval = 15 * time.Second

	// Deadline for single (non-streaming) requests to complete.
	// Used if no deadline is provided on context.
	defaultSingleRequestTimeout = time.Minute
)

var internalByteBuffer = bpool.Pool[*[]byte]{
	New: func() *[]byte {
		m := make([]byte, 0, defaultBufferSize)
		return &m
	},
}

var internal32KByteBuffer = bpool.Pool[*[]byte]{
	New: func() *[]byte {
		m := make([]byte, 0, biggerBufMin)
		return &m
	},
}

// GetByteBuffer can be replaced with a function that returns a small
// byte buffer.
// When replacing PutByteBuffer should also be replaced
// There is no minimum size.
var GetByteBuffer = func() []byte {
	b := *internalByteBuffer.Get()
	return b[:0]
}

// GetByteBufferCap returns a length 0 byte buffer with at least the given capacity.
func GetByteBufferCap(wantSz int) []byte {
	if wantSz < defaultBufferSize {
		b := GetByteBuffer()[:0]
		if cap(b) >= wantSz {
			return b
		}
		PutByteBuffer(b)
	}
	if wantSz <= maxBufferSize {
		b := *internal32KByteBuffer.Get()
		if cap(b) >= wantSz {
			return b[:0]
		}
		internal32KByteBuffer.Put(&b)
	}
	return make([]byte, 0, wantSz)
}

// PutByteBuffer is for returning byte buffers.
var PutByteBuffer = func(b []byte) {
	if cap(b) >= biggerBufMin && cap(b) < biggerBufMax {
		internal32KByteBuffer.Put(&b)
		return
	}
	if cap(b) >= minBufferSize && cap(b) < biggerBufMin {
		internalByteBuffer.Put(&b)
		return
	}
}

// readAllInto reads from r and appends to b until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because readAllInto is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func readAllInto(b []byte, r *wsutil.Reader, want int64) ([]byte, error) {
	read := int64(0)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				if want >= 0 && read+int64(n) != want {
					return nil, io.ErrUnexpectedEOF
				}
				err = nil
			}
			return b, err
		}
		read += int64(n)
		if want >= 0 && read == want {
			// No need to read more...
			return b, nil
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
	buf := GetByteBufferCap(len(p))
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
	return fmt.Sprint(string(b))
}

// ConnDialer is a function that dials a connection to the given address.
// There should be no retries in this function,
// and should have a timeout of something like 2 seconds.
// The returned net.Conn should also have quick disconnect on errors.
// The net.Conn must support all features as described by the net.Conn interface.
type ConnDialer func(ctx context.Context, address string) (net.Conn, error)

// ConnectWSWithRoutePath is like ConnectWS but with a custom grid route path.
func ConnectWSWithRoutePath(dial ContextDialer, auth AuthFn, tls *tls.Config, routePath string) func(ctx context.Context, remote string) (net.Conn, error) {
	return func(ctx context.Context, remote string) (net.Conn, error) {
		toDial := strings.Replace(remote, "http://", "ws://", 1)
		toDial = strings.Replace(toDial, "https://", "wss://", 1)
		toDial += routePath

		dialer := ws.DefaultDialer
		dialer.ReadBufferSize = readBufferSize
		dialer.WriteBufferSize = writeBufferSize
		dialer.Timeout = defaultDialTimeout
		if dial != nil {
			dialer.NetDial = dial
		}
		header := make(http.Header, 2)
		header.Set("Authorization", "Bearer "+auth())
		header.Set("X-Minio-Time", strconv.FormatInt(time.Now().UnixNano(), 10))

		if len(header) > 0 {
			dialer.Header = ws.HandshakeHeaderHTTP(header)
		}
		dialer.TLSConfig = tls

		conn, br, _, err := dialer.Dial(ctx, toDial)
		if br != nil {
			ws.PutReader(br)
		}
		return conn, err
	}
}

// ConnectWS returns a function that dials a websocket connection to the given address.
// Route and auth are added to the connection.
func ConnectWS(dial ContextDialer, auth AuthFn, tls *tls.Config) func(ctx context.Context, remote string) (net.Conn, error) {
	return ConnectWSWithRoutePath(dial, auth, tls, RoutePath)
}

// ValidateTokenFn must validate the token and return an error if it is invalid.
type ValidateTokenFn func(token string) error
