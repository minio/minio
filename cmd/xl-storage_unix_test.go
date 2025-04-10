//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd
// +build linux darwin dragonfly freebsd netbsd openbsd

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
	tmpPath := t.TempDir()
	testCases := []struct {
		volName       string
		expectedUmask int
	}{
		{"is-this-valid", getUmask()},
	}
	testCase := testCases[0]

	// Initialize a new xlStorage layer.
	disk, err := newLocalXLStorage(tmpPath)
	if err != nil {
		t.Fatalf("Initializing xlStorage failed with %s.", err)
	}

	// Attempt to create a volume to verify the permissions later.
	// MakeVol creates 0777.
	if err = disk.MakeVol(t.Context(), testCase.volName); err != nil {
		t.Fatalf("Creating a volume failed with %s expected to pass.", err)
	}

	// Stat to get permissions bits.
	st, err := os.Stat(path.Join(tmpPath, testCase.volName))
	if err != nil {
		t.Fatalf("Stat failed with %s expected to pass.", err)
	}

	// Get umask of the bits stored.
	currentUmask := 0o777 - uint32(st.Mode().Perm())

	// Verify if umask is correct.
	if int(currentUmask) != testCase.expectedUmask {
		t.Fatalf("Umask check failed expected %d, got %d", testCase.expectedUmask, currentUmask)
	}
}

// Tests if the file creations happen with proper umask.
func TestIsValidUmaskFile(t *testing.T) {
	tmpPath := t.TempDir()
	testCases := []struct {
		volName       string
		expectedUmask int
	}{
		{"is-this-valid", getUmask()},
	}
	testCase := testCases[0]

	// Initialize a new xlStorage layer.
	disk, err := newLocalXLStorage(tmpPath)
	if err != nil {
		t.Fatalf("Initializing xlStorage failed with %s.", err)
	}

	// Attempt to create a volume to verify the permissions later.
	// MakeVol creates directory with 0777 perms.
	if err = disk.MakeVol(t.Context(), testCase.volName); err != nil {
		t.Fatalf("Creating a volume failed with %s expected to pass.", err)
	}

	// Attempt to create a file to verify the permissions later.
	// AppendFile creates file with 0666 perms.
	if err = disk.AppendFile(t.Context(), testCase.volName, pathJoin("hello-world.txt", xlStorageFormatFile), []byte("Hello World")); err != nil {
		t.Fatalf("Create a file `test` failed with %s expected to pass.", err)
	}

	// CheckFile - stat the file.
	if _, err := disk.StatInfoFile(t.Context(), testCase.volName, "hello-world.txt/"+xlStorageFormatFile, false); err != nil {
		t.Fatalf("Stat failed with %s expected to pass.", err)
	}
}
