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

// Package ioutil implements some I/O utility functions which are not covered
// by the standard library.
package ioutil

import (
	"errors"
	"io"
)

// ErrOverread is returned to the reader when the hard limit of HardLimitReader is exceeded.
var ErrOverread = errors.New("input provided more bytes than specified")

// HardLimitReader returns a Reader that reads from r
// but returns an error if the source provides more data than allowed.
// This means the source *will* be overread unless EOF is returned prior.
// The underlying implementation is a *HardLimitedReader.
// This will ensure that at most n bytes are returned and EOF is reached.
func HardLimitReader(r io.Reader, n int64) io.Reader { return &HardLimitedReader{r, n} }

// A HardLimitedReader reads from R but limits the amount of
// data returned to just N bytes. Each call to Read
// updates N to reflect the new amount remaining.
// Read returns EOF when N <= 0 or when the underlying R returns EOF.
type HardLimitedReader struct {
	R io.Reader // underlying reader
	N int64     // max bytes remaining
}

func (l *HardLimitedReader) Read(p []byte) (n int, err error) {
	if l.N < 0 {
		return 0, ErrOverread
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	if l.N < 0 {
		return 0, ErrOverread
	}
	return n, err
}
