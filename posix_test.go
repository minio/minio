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

package main

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
)

// Tests posix.getDiskInfo()
func TestGetDiskInfo(t *testing.T) {
	path, err := ioutil.TempDir(os.TempDir(), "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path)

	testCases := []struct {
		diskPath    string
		expectedErr error
	}{
		{path, nil},
		{"/nonexistent-dir", errDiskNotFound},
	}

	// Check test cases.
	for _, testCase := range testCases {
		if _, err := getDiskInfo(testCase.diskPath); err != testCase.expectedErr {
			t.Fatalf("expected: %s, got: %s", testCase.expectedErr, err)
		}
	}
}

// Tests the functionality implemented by ReadAll storage API.
func TestReadAll(t *testing.T) {
	path, err := ioutil.TempDir(os.TempDir(), "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer removeAll(path)

	// Initialize posix storage layer.
	posix, err := newPosix(path)
	if err != nil {
		t.Fatalf("Unable to initialize posix, %s", err)
	}

	// Create files for the test cases.
	if err = posix.MakeVol("exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = posix.AppendFile("exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = posix.AppendFile("exists", "as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}

	// Testcases to validate different conditions for ReadAll API.
	testCases := []struct {
		volume string
		path   string
		err    error
	}{
		// Validate volume does not exist.
		{
			"i-dont-exist",
			"",
			errVolumeNotFound,
		},
		// Validate bad condition file does not exist.
		{
			"exists",
			"as-file-not-found",
			errFileNotFound,
		},
		// Validate bad condition file exists as prefix/directory and
		// we are attempting to read it.
		{
			"exists",
			"as-directory",
			errFileNotFound,
		},
		// Validate the good condition file exists and we are able to
		// read it.
		{
			"exists",
			"as-file",
			nil,
		},
		// Add more cases here.
	}

	// Run through all the test cases and validate for ReadAll.
	for i, testCase := range testCases {
		_, err = posix.ReadAll(testCase.volume, testCase.path)
		if err != testCase.err {
			t.Errorf("Test %d expected err %s, got err %s", i+1, testCase.err, err)
		}
	}
}

// TestNewPosix all the cases handled in posix storage layer initialization.
func TestNewPosix(t *testing.T) {
	// Temporary dir name.
	tmpDirName := os.TempDir() + "/" + "minio-" + nextSuffix()
	// Temporary file name.
	tmpFileName := os.TempDir() + "/" + "minio-" + nextSuffix()
	f, _ := os.Create(tmpFileName)
	f.Close()
	defer os.Remove(tmpFileName)

	// List of all tests for posix initialization.
	testCases := []struct {
		diskPath string
		err      error
	}{
		// Validates input argument cannot be empty.
		{
			"",
			errInvalidArgument,
		},
		// Validates if the directory does not exist and
		// gets automatically created.
		{
			tmpDirName,
			nil,
		},
		// Validates if the disk exists as file and returns error
		// not a directory.
		{
			tmpFileName,
			syscall.ENOTDIR,
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		_, err := newPosix(testCase.diskPath)
		if err != testCase.err {
			t.Fatalf("Test %d failed wanted: %s, got: %s", i+1, err, testCase.err)
		}
	}
}
