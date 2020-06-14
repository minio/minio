// +build linux darwin dragonfly freebsd netbsd openbsd

/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"
)

// Based on `man getumask` a vaporware GNU extension to glibc.
// returns file mode creation mask.
func getUmask() int {
	mask := syscall.Umask(0)
	syscall.Umask(mask)
	return mask
}

// Tests if the directory and file creations happen with proper umask.
func TestIsValidUmaskVol(t *testing.T) {
	tmpPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Initializing temporary directory failed with %s.", err)
	}
	testCases := []struct {
		volName       string
		expectedUmask int
	}{
		{"is-this-valid", getUmask()},
	}
	testCase := testCases[0]

	// Initialize a new xlStorage layer.
	disk, err := newXLStorage(tmpPath, "")
	if err != nil {
		t.Fatalf("Initializing xlStorage failed with %s.", err)
	}

	// Attempt to create a volume to verify the permissions later.
	// MakeVol creates 0777.
	if err = disk.MakeVol(testCase.volName); err != nil {
		t.Fatalf("Creating a volume failed with %s expected to pass.", err)
	}
	defer os.RemoveAll(tmpPath)

	// Stat to get permissions bits.
	st, err := os.Stat(path.Join(tmpPath, testCase.volName))
	if err != nil {
		t.Fatalf("Stat failed with %s expected to pass.", err)
	}

	// Get umask of the bits stored.
	currentUmask := 0777 - uint32(st.Mode().Perm())

	// Verify if umask is correct.
	if int(currentUmask) != testCase.expectedUmask {
		t.Fatalf("Umask check failed expected %d, got %d", testCase.expectedUmask, currentUmask)
	}
}

// Tests if the file creations happen with proper umask.
func TestIsValidUmaskFile(t *testing.T) {
	tmpPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Initializing temporary directory failed with %s.", err)
	}
	testCases := []struct {
		volName       string
		expectedUmask int
	}{
		{"is-this-valid", getUmask()},
	}
	testCase := testCases[0]

	// Initialize a new xlStorage layer.
	disk, err := newXLStorage(tmpPath, "")
	if err != nil {
		t.Fatalf("Initializing xlStorage failed with %s.", err)
	}

	// Attempt to create a volume to verify the permissions later.
	// MakeVol creates directory with 0777 perms.
	if err = disk.MakeVol(testCase.volName); err != nil {
		t.Fatalf("Creating a volume failed with %s expected to pass.", err)
	}

	defer os.RemoveAll(tmpPath)

	// Attempt to create a file to verify the permissions later.
	// AppendFile creates file with 0666 perms.
	if err = disk.AppendFile(testCase.volName, pathJoin("hello-world.txt", xlStorageFormatFile), []byte("Hello World")); err != nil {
		t.Fatalf("Create a file `test` failed with %s expected to pass.", err)
	}

	// CheckFile - stat the file.
	if err := disk.CheckFile(testCase.volName, "hello-world.txt"); err != nil {
		t.Fatalf("Stat failed with %s expected to pass.", err)
	}
}
