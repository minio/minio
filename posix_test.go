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
	"testing"
)

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
