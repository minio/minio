// Copyright 2019 smallnest. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package ringbuffer

import (
	"context"
	"errors"
	"io"
	"sync"
	"unsafe"
)

var (
	// ErrTooMuchDataToWrite is returned when the data to write is more than the buffer size.
	ErrTooMuchDataToWrite = errors.New("too much data to write")

	// ErrIsFull is returned when the buffer is full and not blocking.
	ErrIsFull = errors.New("ringbuffer is full")

	// ErrIsEmpty is returned when the buffer is empty and not blocking.
	ErrIsEmpty = errors.New("ringbuffer is empty")

	// ErrIsNotEmpty is returned when the buffer is not empty and not blocking.
	ErrIsNotEmpty = errors.New("ringbuffer is not empty")

	// ErrAcquireLock is returned when the lock is not acquired on Try operations.
	ErrAcquireLock = errors.New("unable to acquire lock")

	// ErrWriteOnClosed is returned when write on a closed ringbuffer.
	ErrWriteOnClosed = errors.New("write on closed ringbuffer")
)

// RingBuffer is a circular buffer that implement io.ReaderWriter interface.
// It operates like a buffered pipe, where data written to a RingBuffer
// and can be read back from another goroutine.
// It is safe to concurrently read and write RingBuffer.
type RingBuffer struct {
	buf       []byte
	size      int
	r         int // next position to read
	w         int // next position to write
	isFull    bool
	err       error
	block     bool
	mu        sync.Mutex
	wg        sync.WaitGroup
	readCond  *sync.Cond // Signaled when data has been read.
	writeCond *sync.Cond // Signaled when data has been written.
}

// New returns a new RingBuffer whose buffer has the given size.
func New(size int) *RingBuffer {
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}
}

// NewBuffer returns a new RingBuffer whose buffer is provided.
func NewBuffer(b []byte) *RingBuffer {
	return &RingBuffer{
		buf:  b,
		size: len(b),
	}
}

// SetBlocking sets the blocking mode of the ring buffer.
// If block is true, Read and Write will block when there is no data to read or no space to write.
// If block is false, Read and Write will return ErrIsEmpty or ErrIsFull immediately.
// By default, the ring buffer is not blocking.
// This setting should be called before any Read or Write operation or after a Reset.
func (r *RingBuffer) SetBlocking(block bool) *RingBuffer {
	r.block = block
	if block {
		r.readCond = sync.NewCond(&r.mu)
		r.writeCond = sync.NewCond(&r.mu)
	}
	return r
}

// WithCancel sets a context to cancel the ring buffer.
// When the context is canceled, the ring buffer will be closed with the context error.
// A goroutine will be started and run until the provided context is canceled.
func (r *RingBuffer) WithCancel(ctx context.Context) *RingBuffer {
	go func() {
		<-ctx.Done()
		r.CloseWithError(ctx.Err())
	}()
	return r
}

func (r *RingBuffer) setErr(err error, locked bool) error {
	if !locked {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	if r.err != nil && r.err != io.EOF {
		return r.err
	}

	switch err {
	// Internal errors are transient
	case nil, ErrIsEmpty, ErrIsFull, ErrAcquireLock, ErrTooMuchDataToWrite, ErrIsNotEmpty:
		return err
	default:
		r.err = err
		if r.block {
			r.readCond.Broadcast()
			r.writeCond.Broadcast()
		}
	}
	return err
}

func (r *RingBuffer) readErr(locked bool) error {
	if !locked {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	if r.err != nil {
		if r.err == io.EOF {
			if r.w == r.r && !r.isFull {
				return io.EOF
			}
			return nil
		}
		return r.err
	}
	return nil
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
// Even if Read returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting for more.
// When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes, it returns the number of bytes read.
// It may return the (non-nil) error from the same call or return the error (and n == 0) from a subsequent call.
// Callers should always process the n > 0 bytes returned before considering the error err.
// Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF behaviors.
func (r *RingBuffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, r.readErr(false)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.readErr(true); err != nil {
		return 0, err
	}

	r.wg.Add(1)
	defer r.wg.Done()
	n, err = r.read(p)
	for err == ErrIsEmpty && r.block {
		r.writeCond.Wait()
		if err = r.readErr(true); err != nil {
			break
		}
		n, err = r.read(p)
	}
	if r.block && n > 0 {
		r.readCond.Broadcast()
	}
	return n, err
}

// TryRead read up to len(p) bytes into p like Read but it is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryRead(p []byte) (n int, err error) {
	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	defer r.mu.Unlock()
	if err := r.readErr(true); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, r.readErr(true)
	}

	n, err = r.read(p)
	if r.block && n > 0 {
		r.readCond.Broadcast()
	}
	return n, err
}

func (r *RingBuffer) read(p []byte) (n int, err error) {
	if r.w == r.r && !r.isFull {
		return 0, ErrIsEmpty
	}

	if r.w > r.r {
		n = min(r.w-r.r, len(p))
		copy(p, r.buf[r.r:r.r+n])
		r.r = (r.r + n) % r.size
		return n, err
	}

	n = min(r.size-r.r+r.w, len(p))

	if r.r+n <= r.size {
		copy(p, r.buf[r.r:r.r+n])
	} else {
		c1 := r.size - r.r
		copy(p, r.buf[r.r:r.size])
		c2 := n - c1
		copy(p[c1:], r.buf[0:c2])
	}
	r.r = (r.r + n) % r.size

	r.isFull = false

	return n, r.readErr(true)
}

// ReadByte reads and returns the next byte from the input or ErrIsEmpty.
func (r *RingBuffer) ReadByte() (b byte, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = r.readErr(true); err != nil {
		return 0, err
	}
	for r.w == r.r && !r.isFull {
		if r.block {
			r.writeCond.Wait()
			err = r.readErr(true)
			if err != nil {
				return 0, err
			}
			continue
		}
		return 0, ErrIsEmpty
	}
	b = r.buf[r.r]
	r.r++
	if r.r == r.size {
		r.r = 0
	}

	r.isFull = false
	return b, r.readErr(true)
}

// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// If blocking n < len(p) will be returned only if an error occurred.
// Write returns a non-nil error if it returns n < len(p).
// Write will not modify the slice data, even temporarily.
func (r *RingBuffer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, r.setErr(nil, false)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return 0, err
	}
	wrote := 0
	for len(p) > 0 {
		n, err = r.write(p)
		wrote += n
		if !r.block || err == nil {
			break
		}
		err = r.setErr(err, true)
		if r.block && (err == ErrIsFull || err == ErrTooMuchDataToWrite) {
			r.writeCond.Broadcast()
			r.readCond.Wait()
			p = p[n:]
			err = nil
			continue
		}
		break
	}
	if r.block && wrote > 0 {
		r.writeCond.Broadcast()
	}

	return wrote, r.setErr(err, true)
}

// TryWrite writes len(p) bytes from p to the underlying buf like Write, but it is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryWrite(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, r.setErr(nil, false)
	}
	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return 0, err
	}

	n, err = r.write(p)
	if r.block && n > 0 {
		r.writeCond.Broadcast()
	}
	return n, r.setErr(err, true)
}

func (r *RingBuffer) write(p []byte) (n int, err error) {
	if r.isFull {
		return 0, ErrIsFull
	}

	var avail int
	if r.w >= r.r {
		avail = r.size - r.w + r.r
	} else {
		avail = r.r - r.w
	}

	if len(p) > avail {
		err = ErrTooMuchDataToWrite
		p = p[:avail]
	}
	n = len(p)

	if r.w >= r.r {
		c1 := r.size - r.w
		if c1 >= n {
			copy(r.buf[r.w:], p)
			r.w += n
		} else {
			copy(r.buf[r.w:], p[:c1])
			c2 := n - c1
			copy(r.buf[0:], p[c1:])
			r.w = c2
		}
	} else {
		copy(r.buf[r.w:], p)
		r.w += n
	}

	if r.w == r.size {
		r.w = 0
	}
	if r.w == r.r {
		r.isFull = true
	}

	return n, err
}

// WriteByte writes one byte into buffer, and returns ErrIsFull if buffer is full.
func (r *RingBuffer) WriteByte(c byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return err
	}
	err := r.writeByte(c)
	for err == ErrIsFull && r.block {
		r.readCond.Wait()
		err = r.setErr(r.writeByte(c), true)
	}
	if r.block && err == nil {
		r.writeCond.Broadcast()
	}
	return err
}

// TryWriteByte writes one byte into buffer without blocking.
// If it has not succeeded to acquire the lock, it return ErrAcquireLock.
func (r *RingBuffer) TryWriteByte(c byte) error {
	ok := r.mu.TryLock()
	if !ok {
		return ErrAcquireLock
	}
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return err
	}

	err := r.writeByte(c)
	if err == nil && r.block {
		r.writeCond.Broadcast()
	}
	return err
}

func (r *RingBuffer) writeByte(c byte) error {
	if r.w == r.r && r.isFull {
		return ErrIsFull
	}
	r.buf[r.w] = c
	r.w++

	if r.w == r.size {
		r.w = 0
	}
	if r.w == r.r {
		r.isFull = true
	}

	return nil
}

// Length return the length of available read bytes.
func (r *RingBuffer) Length() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.w == r.r {
		if r.isFull {
			return r.size
		}
		return 0
	}

	if r.w > r.r {
		return r.w - r.r
	}

	return r.size - r.r + r.w
}

// Capacity returns the size of the underlying buffer.
func (r *RingBuffer) Capacity() int {
	return r.size
}

// Free returns the length of available bytes to write.
func (r *RingBuffer) Free() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.w == r.r {
		if r.isFull {
			return 0
		}
		return r.size
	}

	if r.w < r.r {
		return r.r - r.w
	}

	return r.size - r.w + r.r
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (r *RingBuffer) WriteString(s string) (n int, err error) {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	buf := *(*[]byte)(unsafe.Pointer(&h))
	return r.Write(buf)
}

// Bytes returns all available read bytes.
// It does not move the read pointer and only copy the available data.
// If the dst is big enough it will be used as destination,
// otherwise a new buffer will be allocated.
func (r *RingBuffer) Bytes(dst []byte) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	getDst := func(n int) []byte {
		if cap(dst) < n {
			return make([]byte, n)
		}
		return dst[:n]
	}

	if r.w == r.r {
		if r.isFull {
			buf := getDst(r.size)
			copy(buf, r.buf[r.r:])
			copy(buf[r.size-r.r:], r.buf[:r.w])
			return buf
		}
		return nil
	}

	if r.w > r.r {
		buf := getDst(r.w - r.r)
		copy(buf, r.buf[r.r:r.w])
		return buf
	}

	n := r.size - r.r + r.w
	buf := getDst(n)

	if r.r+n < r.size {
		copy(buf, r.buf[r.r:r.r+n])
	} else {
		c1 := r.size - r.r
		copy(buf, r.buf[r.r:r.size])
		c2 := n - c1
		copy(buf[c1:], r.buf[0:c2])
	}

	return buf
}

// IsFull returns this ringbuffer is full.
func (r *RingBuffer) IsFull() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.isFull
}

// IsEmpty returns this ringbuffer is empty.
func (r *RingBuffer) IsEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return !r.isFull && r.w == r.r
}

// CloseWithError closes the writer; reads will return
// no bytes and the error err, or EOF if err is nil.
//
// CloseWithError never overwrites the previous error if it exists
// and always returns nil.
func (r *RingBuffer) CloseWithError(err error) {
	if err == nil {
		err = io.EOF
	}
	r.setErr(err, false)
}

// CloseWriter closes the writer.
// Reads will return any remaining bytes and io.EOF.
func (r *RingBuffer) CloseWriter() {
	r.setErr(io.EOF, false)
}

// Flush waits for the buffer to be empty and fully read.
// If not blocking ErrIsNotEmpty will be returned if the buffer still contains data.
func (r *RingBuffer) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.w != r.r || r.isFull {
		err := r.readErr(true)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if !r.block {
			return ErrIsNotEmpty
		}
		r.readCond.Wait()
	}

	err := r.readErr(true)
	if err == io.EOF {
		return nil
	}
	return err
}

// Reset the read pointer and writer pointer to zero.
func (r *RingBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Set error so any readers/writers will return immediately.
	r.setErr(errors.New("reset called"), true)
	if r.block {
		r.readCond.Broadcast()
		r.writeCond.Broadcast()
	}

	// Unlock the mutex so readers/writers can finish.
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	r.r = 0
	r.w = 0
	r.err = nil
	r.isFull = false
}

// WriteCloser returns a WriteCloser that writes to the ring buffer.
// When the returned WriteCloser is closed, it will wait for all data to be read before returning.
func (r *RingBuffer) WriteCloser() io.WriteCloser {
	return &writeCloser{RingBuffer: r}
}

type writeCloser struct {
	*RingBuffer
}

// Close provides a close method for the WriteCloser.
func (wc *writeCloser) Close() error {
	wc.CloseWriter()
	return wc.Flush()
}
