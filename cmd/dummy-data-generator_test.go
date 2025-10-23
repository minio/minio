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

package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

var alphabets = []byte("abcdefghijklmnopqrstuvwxyz0123456789")

// DummyDataGen returns a reader that repeats the bytes in `alphabets`
// upto the desired length.
type DummyDataGen struct {
	b           []byte
	idx, length int64
}

// NewDummyDataGen returns a ReadSeeker over the first `totalLength`
// bytes from the infinite stream consisting of repeated
// concatenations of `alphabets`.
//
// The skipOffset (generally = 0) can be used to skip a given number
// of bytes from the beginning of the infinite stream. This is useful
// to compare such streams of bytes that may be split up, because:
//
// Given the function:
//
//	f := func(r io.Reader) string {
//	          b, _ := io.ReadAll(r)
//	          return string(b)
//	}
//
// for example, the following is true:
//
// f(NewDummyDataGen(100, 0)) == f(NewDummyDataGen(50, 0)) + f(NewDummyDataGen(50, 50))
func NewDummyDataGen(totalLength, skipOffset int64) io.ReadSeeker {
	if totalLength < 0 {
		panic("Negative length passed to DummyDataGen!")
	}
	if skipOffset < 0 {
		panic("Negative rotations are not allowed")
	}

	skipOffset %= int64(len(alphabets))
	const multiply = 100
	as := bytes.Repeat(alphabets, multiply)
	b := as[skipOffset : skipOffset+int64(len(alphabets)*(multiply-1))]
	return &DummyDataGen{
		length: totalLength,
		b:      b,
	}
}

func (d *DummyDataGen) Read(b []byte) (n int, err error) {
	k := len(b)
	numLetters := int64(len(d.b))
	for k > 0 && d.idx < d.length {
		w := copy(b[len(b)-k:], d.b[d.idx%numLetters:])
		k -= w
		d.idx += int64(w)
		n += w
	}
	if d.idx >= d.length {
		extraBytes := d.idx - d.length
		n -= int(extraBytes)
		if n < 0 {
			n = 0
		}
		err = io.EOF
	}
	return n, err
}

func (d *DummyDataGen) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.New("Invalid offset")
		}
		d.idx = offset
	case io.SeekCurrent:
		if d.idx+offset < 0 {
			return 0, errors.New("Invalid offset")
		}
		d.idx += offset
	case io.SeekEnd:
		if d.length+offset < 0 {
			return 0, errors.New("Invalid offset")
		}
		d.idx = d.length + offset
	}
	return d.idx, nil
}

func TestDummyDataGenerator(t *testing.T) {
	readAll := func(r io.Reader) string {
		b, _ := io.ReadAll(r)
		return string(b)
	}
	checkEq := func(a, b string) {
		if a != b {
			t.Fatalf("Unexpected equality failure")
		}
	}

	checkEq(readAll(NewDummyDataGen(0, 0)), "")

	checkEq(readAll(NewDummyDataGen(10, 0)), readAll(NewDummyDataGen(10, int64(len(alphabets)))))

	checkEq(readAll(NewDummyDataGen(100, 0)), readAll(NewDummyDataGen(50, 0))+readAll(NewDummyDataGen(50, 50)))

	r := NewDummyDataGen(100, 0)
	r.Seek(int64(len(alphabets)), 0)
	checkEq(readAll(r), readAll(NewDummyDataGen(100-int64(len(alphabets)), 0)))
}

// Compares all the bytes returned by the given readers. Any Read
// errors cause a `false` result. A string describing the error is
// also returned.
func cmpReaders(r1, r2 io.Reader) (bool, string) {
	bufLen := 32 * 1024
	b1, b2 := make([]byte, bufLen), make([]byte, bufLen)
	for i := 0; true; i++ {
		n1, e1 := io.ReadFull(r1, b1)
		n2, e2 := io.ReadFull(r2, b2)
		if n1 != n2 {
			return false, fmt.Sprintf("Read %d != %d bytes from the readers", n1, n2)
		}
		if !bytes.Equal(b1[:n1], b2[:n2]) {
			return false, fmt.Sprintf("After reading %d equal buffers (32Kib each), we got the following two strings:\n%v\n%v\n",
				i, b1, b2)
		}
		// Check if stream has ended
		if (e1 == io.ErrUnexpectedEOF && e2 == io.ErrUnexpectedEOF) || (e1 == io.EOF && e2 == io.EOF) {
			break
		}
		if e1 != nil || e2 != nil {
			return false, fmt.Sprintf("Got unexpected error values: %v == %v", e1, e2)
		}
	}
	return true, ""
}

func TestCmpReaders(t *testing.T) {
	{
		r1 := bytes.NewReader([]byte("abc"))
		r2 := bytes.NewReader([]byte("abc"))
		ok, msg := cmpReaders(r1, r2)
		if !ok || msg != "" {
			t.Fatalf("unexpected")
		}
	}

	{
		r1 := bytes.NewReader([]byte("abc"))
		r2 := bytes.NewReader([]byte("abcd"))
		ok, _ := cmpReaders(r1, r2)
		if ok {
			t.Fatalf("unexpected")
		}
	}
}
