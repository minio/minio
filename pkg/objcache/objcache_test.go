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
 */

package objcache

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

// Tests different types of seekable operations on an allocated buffer.
func TestBufferSeek(t *testing.T) {
	r := NewBuffer([]byte("0123456789"))
	tests := []struct {
		off     int64
		seek    int
		n       int
		want    string
		wantpos int64
		seekerr string
	}{
		{seek: os.SEEK_SET, off: 0, n: 20, want: "0123456789"},
		{seek: os.SEEK_SET, off: 1, n: 1, want: "1"},
		{seek: os.SEEK_CUR, off: 1, wantpos: 3, n: 2, want: "34"},
		{seek: os.SEEK_SET, off: -1, seekerr: "cache.Buffer.Seek: negative position"},
		{seek: os.SEEK_SET, off: 1 << 33, wantpos: 1 << 33},
		{seek: os.SEEK_CUR, off: 1, wantpos: 1<<33 + 1},
		{seek: os.SEEK_SET, n: 5, want: "01234"},
		{seek: os.SEEK_CUR, n: 5, want: "56789"},
		{seek: os.SEEK_END, off: -1, seekerr: "cache.Buffer.Seek: whence os.SEEK_END is not supported"},
	}

	for i, tt := range tests {
		pos, err := r.Seek(tt.off, tt.seek)
		if err == nil && tt.seekerr != "" {
			t.Errorf("%d. want seek error %q", i, tt.seekerr)
			continue
		}
		if err != nil && err.Error() != tt.seekerr {
			t.Errorf("%d. seek error = %q; want %q", i, err.Error(), tt.seekerr)
			continue
		}
		if tt.wantpos != 0 && tt.wantpos != pos {
			t.Errorf("%d. pos = %d, want %d", i, pos, tt.wantpos)
		}
		buf := make([]byte, tt.n)
		n, err := r.Read(buf)
		if err != nil {
			t.Errorf("%d. read = %v", i, err)
			continue
		}
		got := string(buf[:n])
		if got != tt.want {
			t.Errorf("%d. got %q; want %q", i, got, tt.want)
		}
	}
}

// Tests read operation after big seek.
func TestReadAfterBigSeek(t *testing.T) {
	r := NewBuffer([]byte("0123456789"))
	if _, err := r.Seek(1<<31+5, os.SEEK_SET); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(make([]byte, 10)); n != 0 || err != io.EOF {
		t.Errorf("Read = %d, %v; want 0, EOF", n, err)
	}
}

// tests that Len is affected by reads, but Size is not.
func TestBufferLenSize(t *testing.T) {
	r := NewBuffer([]byte("abc"))
	io.CopyN(ioutil.Discard, r, 1)
	if r.Len() != 2 {
		t.Errorf("Len = %d; want 2", r.Len())
	}
	if r.Size() != 3 {
		t.Errorf("Size = %d; want 3", r.Size())
	}
}
