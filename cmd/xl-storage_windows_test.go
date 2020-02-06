// +build windows

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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

// Test if various paths work as expected when converted to UNC form
func TestUNCPaths(t *testing.T) {
	var testCases = []struct {
		objName string
		pass    bool
	}{
		{"/abcdef", true},
		{"/a/b/c/d/e/f/g", true},
		{string(bytes.Repeat([]byte("界"), 85)), true},
		// Each path component must be <= 255 bytes long.
		{string(bytes.Repeat([]byte("界"), 280)), false},
		{`/p/q/r/s/t`, true},
	}
	dir, err := ioutil.TempDir("", "testdisk-")
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup on exit of test
	defer os.RemoveAll(dir)

	// Instantiate posix object to manage a disk
	var fs StorageAPI
	fs, err = newXLStorage(dir, "")
	if err != nil {
		t.Fatal(err)
	}

	// Create volume to use in conjunction with other StorageAPI's file API(s)
	err = fs.MakeVol("voldir")
	if err != nil {
		t.Fatal(err)
	}

	for i, test := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			err = fs.AppendFile("voldir", test.objName, []byte("hello"))
			if err != nil && test.pass {
				t.Error(err)
			} else if err == nil && !test.pass {
				t.Error(err)
			}
			fs.DeleteFile("voldir", test.objName)
		})
	}
}

// Test to validate xlStorage behavior on windows when a non-final path component is a file.
func TestUNCPathENOTDIR(t *testing.T) {
	// Instantiate posix object to manage a disk
	dir, err := ioutil.TempDir("", "testdisk-")
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup on exit of test
	defer os.RemoveAll(dir)

	var fs StorageAPI
	fs, err = newXLStorage(dir, "")
	if err != nil {
		t.Fatal(err)
	}

	// Create volume to use in conjunction with other StorageAPI's file API(s)
	err = fs.MakeVol("voldir")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.AppendFile("voldir", "/file", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	// Try to create a file that includes a file in its path components.
	// In *nix, this returns syscall.ENOTDIR while in windows we receive the following error.
	err = fs.AppendFile("voldir", "/file/obj1", []byte("hello"))
	if err != errFileAccessDenied {
		t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
	}
}
