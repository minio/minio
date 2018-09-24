/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

// Package ioutil implements some I/O utility functions which are not covered
// by the standard library.
package ioutil

import (
	"io"

	humanize "github.com/dustin/go-humanize"
)

// defaultAppendBufferSize - Default buffer size for the AppendFile
const defaultAppendBufferSize = humanize.MiByte

// WriteOnCloser implements io.WriteCloser and always
// exectues at least one write operation if it is closed.
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

// Implements the io.Writer interface limiting upto
// configured length, also skips the first N bytes.
func (w *LimitWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	var n1 int
	if w.skipBytes > 0 {
		if w.skipBytes >= int64(len(p)) {
			w.skipBytes = w.skipBytes - int64(len(p))
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
		w.wLimit = w.wLimit - int64(n1)
		return n, err
	}
	n1, err = w.Writer.Write(p)
	w.wLimit = w.wLimit - int64(n1)
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
	for s.skipCount > 0 {
		if l > s.skipCount {
			l = s.skipCount
		}
		n, err := s.Reader.Read(p[:l])
		if err != nil {
			return 0, err
		}
		s.skipCount -= int64(n)
	}
	return s.Reader.Read(p)
}

// NewSkipReader - creates a SkipReader
func NewSkipReader(r io.Reader, n int64) io.Reader {
	return &SkipReader{r, n}
}
