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
	"os"
	"runtime"
	"testing"

	"github.com/minio/minio/internal/lock"
)

// Tests long path calls.
func TestRWPoolLongPath(t *testing.T) {
	rwPool := &fsIOPool{
		readersMap: make(map[string]*lock.RLockedFile),
	}

	longPath := "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
	if _, err := rwPool.Create(longPath); err != errFileNameTooLong {
		t.Fatal(err)
	}

	if _, err := rwPool.Write(longPath); err != errFileNameTooLong {
		t.Fatal(err)
	}

	if _, err := rwPool.Open(longPath); err != errFileNameTooLong {
		t.Fatal(err)
	}
}

// Tests all RWPool methods.
func TestRWPool(t *testing.T) {
	// create xlStorage test setup
	_, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	rwPool := &fsIOPool{
		readersMap: make(map[string]*lock.RLockedFile),
	}
	wlk, err := rwPool.Create(pathJoin(path, "success-vol", "file/path/1.txt"))
	if err != nil {
		t.Fatal(err)
	}
	wlk.Close()

	// Fails to create a parent directory if there is a file.
	_, err = rwPool.Create(pathJoin(path, "success-vol", "file/path/1.txt/test"))
	if err != errFileAccessDenied {
		t.Fatal("Unexpected error", err)
	}

	// Fails to create a file if there is a directory.
	_, err = rwPool.Create(pathJoin(path, "success-vol", "file"))
	if runtime.GOOS == globalWindowsOSName {
		if err != errFileAccessDenied {
			t.Fatal("Unexpected error", err)
		}
	} else {
		if err != errIsNotRegular {
			t.Fatal("Unexpected error", err)
		}
	}

	rlk, err := rwPool.Open(pathJoin(path, "success-vol", "file/path/1.txt"))
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	rlk.Close()

	// Fails to read a directory.
	_, err = rwPool.Open(pathJoin(path, "success-vol", "file"))
	if runtime.GOOS == globalWindowsOSName {
		if err != errFileAccessDenied {
			t.Fatal("Unexpected error", err)
		}
	} else {
		if err != errIsNotRegular {
			t.Fatal("Unexpected error", err)
		}
	}

	// Fails to open a file which has a parent as file.
	_, err = rwPool.Open(pathJoin(path, "success-vol", "file/path/1.txt/test"))
	if runtime.GOOS != globalWindowsOSName {
		if err != errFileAccessDenied {
			t.Fatal("Unexpected error", err)
		}
	} else {
		if err != errFileNotFound {
			t.Fatal("Unexpected error", err)
		}
	}

}
