/*
 * Minio Client (C) 2015, 2016, 2017 Minio, Inc.
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
