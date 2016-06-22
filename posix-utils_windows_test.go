// +build windows

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
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestUNCPaths(t *testing.T) {
	var testCases = []struct {
		objName string
		pass    bool
	}{
		{"/abcdef", true},
		{"/a/b/c/d/e/f/g", true},
		{string(bytes.Repeat([]byte("界"), 85)), true},
		{string(bytes.Repeat([]byte("界"), 100)), false},
		{`\\p\q\r\s\t`, true},
	}
	var err error
	err = os.Mkdir("c:\\testdisk", 0700)
	defer os.RemoveAll("c:\\testdisk")
	var fs StorageAPI
	fs, err = newPosix("c:\\testdisk")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.MakeVol("voldir")
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range testCases {
		err = fs.AppendFile("voldir", test.objName, []byte("hello"))
		if err != nil && test.pass {
			t.Error(err)
		} else if err == nil && !test.pass {
			t.Error(err)
		}

		fs.DeleteFile("voldir", test.objName)
	}
}

func TestUNCPathENOTDIR(t *testing.T) {
	var err error
	err = os.Mkdir("c:\\testdisk", 0700)
	defer os.RemoveAll("c:\\testdisk")
	var fs StorageAPI
	fs, err = newPosix("c:\\testdisk")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.MakeVol("voldir")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.AppendFile("voldir", "/file", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	err = fs.AppendFile("voldir", "/file/obj1", []byte("hello"))
	winErr := "The system cannot find the path specified."
	if !strings.Contains(err.Error(), winErr) {
		t.Errorf("expected to recieve %s, but received %s", winErr, err.Error())
	}
}
