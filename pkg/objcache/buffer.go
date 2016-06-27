/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
 *
 */

package objcache

import (
	"bytes"
	"errors"
	"io"
	"time"
)

// A Buffer is a variable-sized buffer of bytes with Read, Write and Seek methods.
// The zero value for Buffer is an empty buffer ready to use.
type Buffer struct {
	buf        []byte    // contents are the bytes buf[off : len(buf)]
	off        int       // read at &buf[off], write at &buf[len(buf)]
	bootstrap  [64]byte  // memory to hold first slice; helps small buffers (Printf) avoid allocation.
	accessTime time.Time // accessTime holds value of the last access time of this buffer.
}

// NewBuffer creates and initializes a new Buffer using buf as its initial
// contents.  It is intended to prepare a Buffer to read existing data.  It
// can also be used to size the internal buffer for writing. To do that,
// buf should have the desired capacity but a length of zero.
//
// In most cases, new(Buffer) (or just declaring a Buffer variable) is
// sufficient to initialize a Buffer.
func NewBuffer(buf []byte) *Buffer { return &Buffer{buf: buf} }

// Len returns the number of bytes of the unread portion of the buffer;
// b.Len() == len(b.Bytes()).
func (b *Buffer) Len() int { return len(b.buf) - b.off }

// Size returns the original length of the underlying byte slice.
// Size is the number of bytes available for reading via ReadAt.
// The returned value is always the same and is not affected by calls
// to any other method.
func (b *Buffer) Size() int64 { return int64(len(b.buf)) }

// makeSlice allocates a slice of size n. If the allocation fails, it panics
// with ErrTooLarge.
func makeSlice(n int) []byte {
	// If the make fails, give a known error.
	defer func() {
		if recover() != nil {
			panic(bytes.ErrTooLarge)
		}
	}()
	return make([]byte, n)
}

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
// If the buffer can't grow it will panic with ErrTooLarge.
func (b *Buffer) grow(n int) int {
	m := b.Len()
	// If buffer is empty, reset to recover space.
	if m == 0 && b.off != 0 {
		// Reuse buffer space.
		b.buf = b.buf[0:0]
	}
	if len(b.buf)+n > cap(b.buf) {
		var buf []byte
		if b.buf == nil && n <= len(b.bootstrap) {
			buf = b.bootstrap[0:]
		} else if m+n <= cap(b.buf)/2 {
			// We can slide things down instead of allocating a new
			// slice. We only need m+n <= cap(b.buf) to slide, but
			// we instead let capacity get twice as large so we
			// don't spend all our time copying.
			copy(b.buf[:], b.buf[b.off:])
			buf = b.buf[:m]
		} else {
			// not enough space anywhere
			buf = makeSlice(2*cap(b.buf) + n)
			copy(buf, b.buf[b.off:])
		}
		b.buf = buf
		b.off = 0
	}
	b.buf = b.buf[0 : b.off+m+n]
	return b.off + m
}

// Write appends the contents of p to the buffer, growing the buffer as
// needed. The return value n is the length of p; err is always nil. If the
// buffer becomes too large, Write will panic with ErrTooLarge.
func (b *Buffer) Write(p []byte) (n int, err error) {
	m := b.grow(len(p))
	return copy(b.buf[m:], p), nil
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained.  The return value n is the number of bytes read.  If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.off >= len(b.buf) {
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return
}

// Seek implements the io.Seeker interface.
func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0: // Whence 0 sets the offset as new offset.
		abs = offset
	case 1: // Whence 1 sets the current offset and offset as new offset.
		abs = int64(b.off) + offset
	case 2: // Whence 2 sets the total size of the buffer and offset
		// as new offset, not supported yet. // FIXME.
		return 0, errors.New("cache.Buffer.Seek: whence os.SEEK_END is not supported")
	default:
		return 0, errors.New("cache.Buffer.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("cache.Buffer.Seek: negative position")
	}
	b.off = int(abs)
	return abs, nil
}
