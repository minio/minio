// Copyright (c) 2015 Klaus Post, released under MIT License. See LICENSE file.

// The readahead package will do asynchronous read-ahead from an input io.Reader
// and make the data available as an io.Reader.
//
// This should be fully transparent, except that once an error
// has been returned from the Reader, it will not recover.
//
// The readahead object also fulfills the io.WriterTo interface, which
// is likely to speed up copies.
//
// Package home: https://github.com/klauspost/readahead
//
package readahead

import (
	"fmt"
	"io"
)

type reader struct {
	in      io.Reader     // Input reader
	closer  io.Closer     // Optional closer
	ready   chan *buffer  // Buffers ready to be handed to the reader
	reuse   chan *buffer  // Buffers to reuse for input reading
	exit    chan struct{} // Closes when finished
	buffers int           // Number of buffers
	err     error         // If an error has occurred it is here
	cur     *buffer       // Current buffer being served
	exited  chan struct{} // Channel is closed been the async reader shuts down
}

// New returns a reader that will asynchronously read from
// the supplied reader into 4 buffers of 1MB each.
//
// It will start reading from the input at once, maybe even before this
// function has returned.
//
// The input can be read from the returned reader.
// When done use Close() to release the buffers.
func NewReader(rd io.Reader) io.ReadCloser {
	if rd == nil {
		return nil
	}

	ret, err := NewReaderSize(rd, 4, 1<<20)

	// Should not be possible to trigger from other packages.
	if err != nil {
		panic("unexpected error:" + err.Error())
	}
	return ret
}

// New returns a reader that will asynchronously read from
// the supplied reader into 4 buffers of 1MB each.
//
// It will start reading from the input at once, maybe even before this
// function has returned.
//
// The input can be read from the returned reader.
// When done use Close() to release the buffers,
// which will also close the supplied closer.
func NewReadCloser(rd io.ReadCloser) io.ReadCloser {
	if rd == nil {
		return nil
	}

	ret, err := NewReadCloserSize(rd, 4, 1<<20)

	// Should not be possible to trigger from other packages.
	if err != nil {
		panic("unexpected error:" + err.Error())
	}
	return ret
}

// NewReaderSize returns a reader with a custom number of buffers and size.
// buffers is the number of queued buffers and size is the size of each
// buffer in bytes.
func NewReaderSize(rd io.Reader, buffers, size int) (io.ReadCloser, error) {
	if size <= 0 {
		return nil, fmt.Errorf("buffer size too small")
	}
	if buffers <= 0 {
		return nil, fmt.Errorf("number of buffers too small")
	}
	if rd == nil {
		return nil, fmt.Errorf("nil input reader supplied")
	}
	a := &reader{}
	a.init(rd, buffers, size)
	return a, nil
}

// NewReadCloserSize returns a reader with a custom number of buffers and size.
// buffers is the number of queued buffers and size is the size of each
// buffer in bytes.
func NewReadCloserSize(rc io.ReadCloser, buffers, size int) (io.ReadCloser, error) {
	if size <= 0 {
		return nil, fmt.Errorf("buffer size too small")
	}
	if buffers <= 0 {
		return nil, fmt.Errorf("number of buffers too small")
	}
	if rc == nil {
		return nil, fmt.Errorf("nil input reader supplied")
	}
	a := &reader{closer: rc}
	a.init(rc, buffers, size)
	return a, nil
}

// initialize the reader
func (a *reader) init(rd io.Reader, buffers, size int) {
	a.in = rd
	a.ready = make(chan *buffer, buffers)
	a.reuse = make(chan *buffer, buffers)
	a.exit = make(chan struct{}, 0)
	a.exited = make(chan struct{}, 0)
	a.buffers = buffers
	a.cur = nil

	// Create buffers
	for i := 0; i < buffers; i++ {
		a.reuse <- newBuffer(size)
	}

	// Start async reader
	go func() {
		// Ensure that when we exit this is signalled.
		defer close(a.exited)
		for {
			select {
			case b := <-a.reuse:
				err := b.read(a.in)
				a.ready <- b
				if err != nil {
					close(a.ready)
					return
				}
			case <-a.exit:
				return
			}
		}
	}()
}

// fill will check if the current buffer is empty and fill it if it is.
// If an error was returned at the end of the current buffer it is returned.
func (a *reader) fill() (err error) {
	if a.cur.isEmpty() {
		if a.cur != nil {
			a.reuse <- a.cur
			a.cur = nil
		}
		b, ok := <-a.ready
		if !ok {
			return a.err
		}
		a.cur = b
	}
	return nil
}

// Read will return the next available data.
func (a *reader) Read(p []byte) (n int, err error) {
	// Swap buffer and maybe return error
	err = a.fill()
	if err != nil {
		return 0, err
	}

	// Copy what we can
	n = copy(p, a.cur.buffer())
	a.cur.inc(n)

	// If at end of buffer, return any error, if present
	if a.cur.isEmpty() {
		a.err = a.cur.err
		return n, a.err
	}
	return n, nil
}

// WriteTo writes data to w until there's no more data to write or when an error occurs.
// The return value n is the number of bytes written.
// Any error encountered during the write is also returned.
func (a *reader) WriteTo(w io.Writer) (n int64, err error) {
	n = 0
	for {
		err = a.fill()
		if err != nil {
			return n, err
		}
		n2, err := w.Write(a.cur.buffer())
		a.cur.inc(n2)
		n += int64(n2)
		if err != nil {
			return n, err
		}
		if a.cur.err != nil {
			// io.Writer should return nil if we are at EOF.
			if a.cur.err == io.EOF {
				a.err = a.cur.err
				return n, nil
			}
			a.err = a.cur.err
			return n, a.cur.err
		}
	}
}

// Close will ensure that the underlying async reader is shut down.
// It will also close the input supplied on newAsyncReader.
func (a *reader) Close() (err error) {
	select {
	case <-a.exited:
	case a.exit <- struct{}{}:
		<-a.exited
	}
	if a.closer != nil {
		// Only call once
		c := a.closer
		a.closer = nil
		return c.Close()
	}
	return nil
}

// Internal buffer representing a single read.
// If an error is present, it must be returned
// once all buffer content has been served.
type buffer struct {
	buf    []byte
	err    error
	offset int
	size   int
}

func newBuffer(size int) *buffer {
	return &buffer{buf: make([]byte, size), err: nil, size: size}
}

// isEmpty returns true is offset is at end of
// buffer, or if the buffer is nil
func (b *buffer) isEmpty() bool {
	if b == nil {
		return true
	}
	if len(b.buf)-b.offset <= 0 {
		return true
	}
	return false
}

// read into start of the buffer from the supplied reader,
// resets the offset and updates the size of the buffer.
// Any error encountered during the read is returned.
func (b *buffer) read(rd io.Reader) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic reading: %v", r)
			b.err = err
		}
	}()
	var n int
	n, b.err = rd.Read(b.buf[0:b.size])
	b.buf = b.buf[0:n]
	b.offset = 0
	return b.err
}

// Return the buffer at current offset
func (b *buffer) buffer() []byte {
	return b.buf[b.offset:]
}

// inc will increment the read offset
func (b *buffer) inc(n int) {
	b.offset += n
}
