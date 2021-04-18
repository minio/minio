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

package safe

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

type MySuite struct {
	root string
}

func (s *MySuite) SetUpSuite(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "safe_test.go.")
	if err != nil {
		t.Fatal(err)
	}
	s.root = root
}

func (s *MySuite) TearDownSuite(t *testing.T) {
	err := os.RemoveAll(s.root)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSafeAbort(t *testing.T) {
	s := &MySuite{}
	s.SetUpSuite(t)
	defer s.TearDownSuite(t)

	f, err := CreateFile(path.Join(s.root, "testfile-abort"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "testfile-abort"))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	err = f.Abort()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		if err.Error() != "close on aborted file" {
			t.Fatal(err)
		}
	}
}

func TestSafeClose(t *testing.T) {
	s := &MySuite{}
	s.SetUpSuite(t)
	defer s.TearDownSuite(t)

	f, err := CreateFile(path.Join(s.root, "testfile-close"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "testfile-close"))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "testfile-close"))
	if err != nil {
		t.Fatal(err)
	}

	err = os.Remove(path.Join(s.root, "testfile-close"))
	if err != nil {
		t.Fatal(err)
	}

	err = f.Abort()
	if err != nil {
		if err.Error() != "abort on closed file" {
			t.Fatal(err)
		}
	}
}

func TestSafe(t *testing.T) {
	s := &MySuite{}
	s.SetUpSuite(t)
	defer s.TearDownSuite(t)

	f, err := CreateFile(path.Join(s.root, "testfile-safe"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "testfile-safe"))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write([]byte("Test"))
	if err != nil {
		if err.Error() != "write on closed file" {
			t.Fatal(err)
		}
	}

	err = f.Close()
	if err != nil {
		if err.Error() != "close on closed file" {
			t.Fatal(err)
		}
	}

	_, err = os.Stat(path.Join(s.root, "testfile-safe"))
	if err != nil {
		t.Fatal(err)
	}

	err = os.Remove(path.Join(s.root, "testfile-safe"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSafeAbortWrite(t *testing.T) {
	s := &MySuite{}
	s.SetUpSuite(t)
	defer s.TearDownSuite(t)

	f, err := CreateFile(path.Join(s.root, "purgefile-abort"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "purgefile-abort"))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = f.Abort()
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(path.Join(s.root, "purgefile-abort"))
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	err = f.Abort()
	if err != nil {
		if err.Error() != "abort on aborted file" {
			t.Fatal(err)
		}
	}

	_, err = f.Write([]byte("Test"))
	if err != nil {
		if err.Error() != "write on aborted file" {
			t.Fatal(err)
		}
	}
}
