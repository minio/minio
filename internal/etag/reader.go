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

package etag

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
)

// Tagger is the interface that wraps the basic ETag method.
type Tagger interface {
	ETag() ETag
}

type wrapReader struct {
	io.Reader
	Tagger
}

var _ Tagger = wrapReader{} // compiler check

// ETag returns the ETag of the underlying Tagger.
func (r wrapReader) ETag() ETag {
	if r.Tagger == nil {
		return nil
	}
	return r.Tagger.ETag()
}

// Wrap returns an io.Reader that reads from the wrapped
// io.Reader and implements the Tagger interaface.
//
// If content implements Tagger then the returned Reader
// returns ETag of the content. Otherwise, it returns
// nil as ETag.
//
// Wrap provides an adapter for io.Reader implemetations
// that don't implement the Tagger interface.
// It is mainly used to provide a high-level io.Reader
// access to the ETag computed by a low-level io.Reader:
//
//   content := etag.NewReader(r.Body, nil)
//
//   compressedContent := Compress(content)
//   encryptedContent := Encrypt(compressedContent)
//
//   // Now, we need an io.Reader that can access
//   // the ETag computed over the content.
//   reader := etag.Wrap(encryptedContent, content)
//
func Wrap(wrapped, content io.Reader) io.Reader {
	if t, ok := content.(Tagger); ok {
		return wrapReader{
			Reader: wrapped,
			Tagger: t,
		}
	}
	return wrapReader{
		Reader: wrapped,
	}
}

// A Reader wraps an io.Reader and computes the
// MD5 checksum of the read content as ETag.
//
// Optionally, a Reader can also verify that
// the computed ETag matches an expected value.
// Therefore, it compares both ETags once the
// underlying io.Reader returns io.EOF.
// If the computed ETag does not match the
// expected ETag then Read returns a VerifyError.
//
// Reader implements the Tagger interface.
type Reader struct {
	src io.Reader

	md5      hash.Hash
	checksum ETag

	readN int64
}

// NewReader returns a new Reader that computes the
// MD5 checksum of the content read from r as ETag.
//
// If the provided etag is not nil the returned
// Reader compares the etag with the computed
// MD5 sum once the r returns io.EOF.
func NewReader(r io.Reader, etag ETag) *Reader {
	if er, ok := r.(*Reader); ok {
		if er.readN == 0 && Equal(etag, er.checksum) {
			return er
		}
	}
	return &Reader{
		src:      r,
		md5:      md5.New(),
		checksum: etag,
	}
}

// Read reads up to len(p) bytes from the underlying
// io.Reader as specified by the io.Reader interface.
func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	r.readN += int64(n)
	r.md5.Write(p[:n])

	if err == io.EOF && len(r.checksum) != 0 {
		if etag := r.ETag(); !Equal(etag, r.checksum) {
			return n, VerifyError{
				Expected: r.checksum,
				Computed: etag,
			}
		}
	}
	return n, err
}

// ETag returns the ETag of all the content read
// so far. Reading more content changes the MD5
// checksum. Therefore, calling ETag multiple
// times may return different results.
func (r *Reader) ETag() ETag {
	sum := r.md5.Sum(nil)
	return ETag(sum)
}

// VerifyError is an error signaling that a
// computed ETag does not match an expected
// ETag.
type VerifyError struct {
	Expected ETag
	Computed ETag
}

func (v VerifyError) Error() string {
	return fmt.Sprintf("etag: expected ETag %q does not match computed ETag %q", v.Expected, v.Computed)
}
