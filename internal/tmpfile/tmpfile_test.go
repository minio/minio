// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tmpfile

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestTempFile(t *testing.T) {
	f, remove, err := TempFile("/_not_exists_", os.O_RDWR)
	if f != nil || remove || err == nil {
		t.Errorf("TempFile(`/_not_exists_`) = %v, %v, %v", f, remove, err)
	}

	dir := os.TempDir()
	f, remove, err = TempFile(dir, os.O_RDWR)
	if f == nil || err != nil {
		t.Errorf("TempFile(dir) = %v, %v, %v", f, remove, err)
	}
	if f != nil {
		f.Close()
	}
	if remove {
		if err = os.Remove(f.Name()); err != nil {
			t.Error(err)
		}
	}
}

func TestTempFileWithExistingFile(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Error(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	f, remove, err := TempFile(f.Name(), os.O_RDWR)
	if f != nil || err == nil {
		t.Errorf("TempFile(%+q) = %v, %v, %v", f.Name(), f, remove, err)
	}
	if f != nil {
		f.Close()
	}
}
