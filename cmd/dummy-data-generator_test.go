/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
// f := func(r io.Reader) string {
//           b, _ := ioutil.ReadAll(r)
//           return string(b)
// }
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

	skipOffset = skipOffset % int64(len(alphabets))
	as := make([]byte, 2*len(alphabets))
	copy(as, alphabets)
	copy(as[len(alphabets):], alphabets)
	b := as[skipOffset : skipOffset+int64(len(alphabets))]
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
	return
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
		b, _ := ioutil.ReadAll(r)
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
		r1 := bytes.NewBuffer([]byte("abc"))
		r2 := bytes.NewBuffer([]byte("abc"))
		ok, msg := cmpReaders(r1, r2)
		if !(ok && msg == "") {
			t.Fatalf("unexpected")
		}
	}

	{
		r1 := bytes.NewBuffer([]byte("abc"))
		r2 := bytes.NewBuffer([]byte("abcd"))
		ok, _ := cmpReaders(r1, r2)
		if ok {
			t.Fatalf("unexpected")
		}
	}
}
