// Copyright (c) 2015-2021 MinIO, Inc.
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

// Package ioutil implements some I/O utility functions which are not covered
// by the standard library.
package ioutil

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime/debug"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/disk"
)

// Block sizes constant.
const (
	SmallBlock  = 32 * humanize.KiByte  // Default r/w block size for smaller objects.
	MediumBlock = 128 * humanize.KiByte // Default r/w block size for medium sized objects.
	LargeBlock  = 1 * humanize.MiByte   // Default r/w block size for normal objects.
)

// AlignedBytePool is a pool of fixed size aligned blocks
type AlignedBytePool struct {
	size int
	p    bpool.Pool[*[]byte]
}

// NewAlignedBytePool creates a new pool with the specified size.
func NewAlignedBytePool(sz int) *AlignedBytePool {
	return &AlignedBytePool{size: sz, p: bpool.Pool[*[]byte]{New: func() *[]byte {
		b := disk.AlignedBlock(sz)
		return &b
	}}}
}

// aligned sync.Pool's
var (
	ODirectPoolLarge  = NewAlignedBytePool(LargeBlock)
	ODirectPoolMedium = NewAlignedBytePool(MediumBlock)
	ODirectPoolSmall  = NewAlignedBytePool(SmallBlock)
)

// Get a block.
func (p *AlignedBytePool) Get() *[]byte {
	return p.p.Get()
}

// Put a block.
func (p *AlignedBytePool) Put(pb *[]byte) {
	if pb != nil && len(*pb) == p.size {
		p.p.Put(pb)
	}
}

// WriteOnCloser implements io.WriteCloser and always
// executes at least one write operation if it is closed.
//
// This can be useful within the context of HTTP. At least
// one write operation must happen to send the HTTP headers
// to the peer.
type WriteOnCloser struct {
	io.Writer
	hasWritten bool
}

func (w *WriteOnCloser) Write(p []byte) (int, error) {
	w.hasWritten = true
	return w.Writer.Write(p)
}

// Close closes the WriteOnCloser. It behaves like io.Closer.
func (w *WriteOnCloser) Close() error {
	if !w.hasWritten {
		_, err := w.Write(nil)
		if err != nil {
			return err
		}
	}
	if closer, ok := w.Writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// HasWritten returns true if at least one write operation was performed.
func (w *WriteOnCloser) HasWritten() bool { return w.hasWritten }

// WriteOnClose takes an io.Writer and returns an ioutil.WriteOnCloser.
func WriteOnClose(w io.Writer) *WriteOnCloser {
	return &WriteOnCloser{w, false}
}

type ioret[V any] struct {
	val V
	err error
}

// WithDeadline will execute a function with a deadline and return a value of a given type.
// If the deadline/context passes before the function finishes executing,
// the zero value and the context error is returned.
func WithDeadline[V any](ctx context.Context, timeout time.Duration, work func(ctx context.Context) (result V, err error)) (result V, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c := make(chan ioret[V], 1)
	go func() {
		v, err := work(ctx)
		c <- ioret[V]{val: v, err: err}
	}()

	select {
	case v := <-c:
		return v.val, v.err
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}

// DeadlineWorker implements the deadline/timeout resiliency pattern.
type DeadlineWorker struct {
	timeout time.Duration
}

// NewDeadlineWorker constructs a new DeadlineWorker with the given timeout.
// To return values, use the WithDeadline helper instead.
func NewDeadlineWorker(timeout time.Duration) *DeadlineWorker {
	dw := &DeadlineWorker{
		timeout: timeout,
	}
	return dw
}

// Run runs the given function, passing it a stopper channel. If the deadline passes before
// the function finishes executing, Run returns context.DeadlineExceeded to the caller.
// channel so that the work function can attempt to exit gracefully.
// Multiple calls to Run will run independently of each other.
func (d *DeadlineWorker) Run(work func() error) error {
	_, err := WithDeadline[struct{}](context.Background(), d.timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, work()
	})
	return err
}

// DeadlineWriter deadline writer with timeout
type DeadlineWriter struct {
	io.WriteCloser
	timeout time.Duration
	err     error
}

// NewDeadlineWriter wraps a writer to make it respect given deadline
// value per Write(). If there is a blocking write, the returned Writer
// will return whenever the timer hits (the return values are n=0
// and err=context.DeadlineExceeded.)
func NewDeadlineWriter(w io.WriteCloser, timeout time.Duration) io.WriteCloser {
	return &DeadlineWriter{WriteCloser: w, timeout: timeout}
}

func (w *DeadlineWriter) Write(buf []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	n, err := WithDeadline[int](context.Background(), w.timeout, func(ctx context.Context) (int, error) {
		return w.WriteCloser.Write(buf)
	})
	w.err = err
	return n, err
}

// Close closer interface to close the underlying closer
func (w *DeadlineWriter) Close() error {
	err := w.WriteCloser.Close()
	w.err = err
	if err == nil {
		w.err = errors.New("we are closed") // Avoids any reuse on the Write() side.
	}
	return err
}

// LimitWriter implements io.WriteCloser.
//
// This is implemented such that we want to restrict
// an enscapsulated writer upto a certain length
// and skip a certain number of bytes.
type LimitWriter struct {
	io.Writer
	skipBytes int64
	wLimit    int64
}

// Write implements the io.Writer interface limiting upto
// configured length, also skips the first N bytes.
func (w *LimitWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	var n1 int
	if w.skipBytes > 0 {
		if w.skipBytes >= int64(len(p)) {
			w.skipBytes -= int64(len(p))
			return n, nil
		}
		p = p[w.skipBytes:]
		w.skipBytes = 0
	}
	if w.wLimit == 0 {
		return n, nil
	}
	if w.wLimit < int64(len(p)) {
		n1, err = w.Writer.Write(p[:w.wLimit])
		w.wLimit -= int64(n1)
		return n, err
	}
	n1, err = w.Writer.Write(p)
	w.wLimit -= int64(n1)
	return n, err
}

// Close closes the LimitWriter. It behaves like io.Closer.
func (w *LimitWriter) Close() error {
	if closer, ok := w.Writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// LimitedWriter takes an io.Writer and returns an ioutil.LimitWriter.
func LimitedWriter(w io.Writer, skipBytes int64, limit int64) *LimitWriter {
	return &LimitWriter{w, skipBytes, limit}
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// NopCloser returns a WriteCloser with a no-op Close method wrapping
// the provided Writer w.
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}

// SkipReader skips a given number of bytes and then returns all
// remaining data.
type SkipReader struct {
	io.Reader

	skipCount int64
}

func (s *SkipReader) Read(p []byte) (int, error) {
	l := int64(len(p))
	if l == 0 {
		return 0, nil
	}
	if s.skipCount > 0 {
		tmp := p
		if s.skipCount > l && l < SmallBlock {
			// We may get a very small buffer, so we grab a temporary buffer.
			bufp := ODirectPoolSmall.Get()
			tmp = *bufp
			defer ODirectPoolSmall.Put(bufp)
			l = int64(len(tmp))
		}
		for s.skipCount > 0 {
			if l > s.skipCount {
				l = s.skipCount
			}
			n, err := s.Reader.Read(tmp[:l])
			if err != nil {
				return 0, err
			}
			s.skipCount -= int64(n)
		}
	}
	return s.Reader.Read(p)
}

// NewSkipReader - creates a SkipReader
func NewSkipReader(r io.Reader, n int64) io.Reader {
	return &SkipReader{r, n}
}

// writerOnly hides an io.Writer value's optional ReadFrom method
// from io.Copy.
type writerOnly struct {
	io.Writer
}

// Copy is exactly like io.Copy but with reusable buffers.
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	bufp := ODirectPoolMedium.Get()
	defer ODirectPoolMedium.Put(bufp)
	buf := *bufp

	return io.CopyBuffer(writerOnly{dst}, src, buf)
}

// SameFile returns if the files are same.
func SameFile(fi1, fi2 os.FileInfo) bool {
	if !os.SameFile(fi1, fi2) {
		return false
	}
	if !fi1.ModTime().Equal(fi2.ModTime()) {
		return false
	}
	if fi1.Mode() != fi2.Mode() {
		return false
	}
	return fi1.Size() == fi2.Size()
}

// DirectioAlignSize - DirectIO alignment needs to be 4K. Defined here as
// directio.AlignSize is defined as 0 in MacOS causing divide by 0 error.
const DirectioAlignSize = 4096

// CopyAligned - copies from reader to writer using the aligned input
// buffer, it is expected that input buffer is page aligned to
// 4K page boundaries. Without passing aligned buffer may cause
// this function to return error.
//
// This code is similar in spirit to io.Copy but it is only to be
// used with DIRECT I/O based file descriptor and it is expected that
// input writer *os.File not a generic io.Writer. Make sure to have
// the file opened for writes with syscall.O_DIRECT flag.
func CopyAligned(w io.Writer, r io.Reader, alignedBuf []byte, totalSize int64, file *os.File) (int64, error) {
	if totalSize == 0 {
		return 0, nil
	}

	var written int64
	for {
		buf := alignedBuf
		if totalSize > 0 {
			remaining := totalSize - written
			if remaining < int64(len(buf)) {
				buf = buf[:remaining]
			}
		}

		nr, err := io.ReadFull(r, buf)
		eof := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if err != nil && !eof {
			return written, err
		}

		buf = buf[:nr]
		var (
			n  int
			un int
			nw int64
		)

		remain := len(buf) % DirectioAlignSize
		if remain == 0 {
			// buf is aligned for directio write()
			n, err = w.Write(buf)
			nw = int64(n)
		} else {
			if remain < len(buf) {
				n, err = w.Write(buf[:len(buf)-remain])
				if err != nil {
					return written, err
				}
				nw = int64(n)
			}

			// Disable O_DIRECT on fd's on unaligned buffer
			// perform an amortized Fdatasync(fd) on the fd at
			// the end, this is performed by the caller before
			// closing 'w'.
			if err = disk.DisableDirectIO(file); err != nil {
				return written, err
			}

			// buf is not aligned, hence use writeUnaligned()
			// for the remainder
			un, err = w.Write(buf[len(buf)-remain:])
			nw += int64(un)
		}

		if nw > 0 {
			written += nw
		}

		if err != nil {
			return written, err
		}

		if nw != int64(len(buf)) {
			return written, io.ErrShortWrite
		}

		if totalSize > 0 && written == totalSize {
			// we have written the entire stream, return right here.
			return written, nil
		}

		if eof {
			// We reached EOF prematurely but we did not write everything
			// that we promised that we would write.
			if totalSize > 0 && written != totalSize {
				return written, io.ErrUnexpectedEOF
			}
			return written, nil
		}
	}
}

// SafeClose safely closes any channel of any type
func SafeClose[T any](c chan<- T) {
	if c != nil {
		close(c)
		return
	}
	// Print stack to check who is sending `c` as `nil`
	// without crashing the server.
	debug.PrintStack()
}
