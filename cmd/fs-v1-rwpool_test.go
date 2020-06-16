/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"os"
	"runtime"
	"testing"

	"github.com/minio/minio/pkg/lock"
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
