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
	"errors"
	"io"
	"io/ioutil"
	"os"
	slashpath "path"
	"runtime"
	"syscall"
	"testing"
)

// creates a temp dir and sets up posix layer.
// returns posix layer, temp dir path to be used for the purpose of tests.
func newPosixTestSetup() (StorageAPI, string, error) {
	diskPath, err := ioutil.TempDir(os.TempDir(), "minio-")
	if err != nil {
		return nil, "", err
	}
	posixStorage, err := newPosix(diskPath)
	if err != nil {
		return nil, "", err
	}
	return posixStorage, diskPath, nil
}

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
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}

	defer removeAll(path)

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
	if err = posix.AppendFile("exists", "as-file-parent", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
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
		{
			"exists",
			"as-file-parent/as-file",
			errFileNotFound,
		},
		// Validate the good condition file exists and we are able to read it.
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

// Test posix.MakeVol()
func TestMakeVol(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	// Create a file.
	if err := ioutil.WriteFile(slashpath.Join(path, "vol-as-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	// Create a directory.
	if err := os.Mkdir(slashpath.Join(path, "existing-vol"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		{"success-vol", nil},
		{"vol-as-file", errVolumeExists},
		{"existing-vol", errVolumeExists},
	}

	for _, testCase := range testCases {
		if err := posix.MakeVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// Test for permission denied.
	if runtime.GOOS != "windows" {
		// Initialize posix storage layer for permission denied error.
		posix, err := newPosix("/usr")
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err := posix.MakeVol("test-vol"); err != errDiskAccessDenied {
			t.Fatalf("expected: %s, got: %s", errDiskAccessDenied, err)
		}
	}
}

// Test posix.DeleteVol()
func TestDeleteVol(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err := posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Test failure cases.
	vol := slashpath.Join(path, "nonempty-vol")
	if err := os.Mkdir(vol, 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}
	if err := ioutil.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		{"success-vol", nil},
		{"nonexistent-vol", errVolumeNotFound},
		{"nonempty-vol", errVolumeNotEmpty},
	}

	for _, testCase := range testCases {
		if err := posix.DeleteVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// Test for permission denied.
	if runtime.GOOS != "windows" {
		// Initialize posix storage layer for permission denied error.
		posix, err := newPosix("/usr")
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err := posix.DeleteVol("bin"); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}
}

// Test posix.StatVol()
func TestStatVol(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err := posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		{"success-vol", nil},
		{"nonexistent-vol", errVolumeNotFound},
	}

	for _, testCase := range testCases {
		if _, err := posix.StatVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}
}

// Test posix.ListVols()
func TestListVols(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Test empty list vols.
	if volInfo, err := posix.ListVols(); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfo) != 0 {
		t.Fatalf("expected: [], got: %s", volInfo)
	}

	// Test non-empty list vols.
	if err := posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if volInfo, err := posix.ListVols(); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfo) != 1 {
		t.Fatalf("expected: 1, got: %d", len(volInfo))
	} else if volInfo[0].Name != "success-vol" {
		t.Fatalf("expected: success-vol, got: %s", volInfo[0].Name)
	}
}

// Test posix.DeleteFile()
func TestDeleteFile(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err := posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err := posix.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		fileName    string
		expectedErr error
	}{
		{"success-file", nil},
		{"nonexistent-file", errFileNotFound},
	}

	for _, testCase := range testCases {
		if err := posix.DeleteFile("success-vol", testCase.fileName); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// Test for permission denied.
	if runtime.GOOS != "windows" {
		// Initialize posix storage layer for permission denied error.
		posix, err := newPosix("/usr")
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err := posix.DeleteFile("bin", "yes"); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}
}

// Test posix.ReadFile()
func TestReadFile(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)
	volume := "success-vol"
	// Setup test environment.
	if err = posix.MakeVol(volume); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		volume      string
		fileName    string
		offset      int64
		bufSize     int
		expectedBuf []byte
		expectedErr error
	}{
		// Successful read at offset 0 and proper buffer size. - 1
		{
			volume, "myobject", 0, 5,
			[]byte("hello"), nil,
		},
		// Success read at hierarchy. - 2
		{
			volume, "path/to/my/object", 0, 5,
			[]byte("hello"), nil,
		},
		// One path segment length is 255 chars long. - 3
		{
			volume, "path/to/my/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, []byte("hello"), nil},
		// Whole path is 1024 characters long, success case. - 4
		{
			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, []byte("hello"),
			func() error {
				// On darwin HFS does not support > 1024 characters.
				if runtime.GOOS == "darwin" {
					return errFileNameTooLong
				}
				// On all other platforms return success.
				return nil
			}(),
		},
		// Object is a directory. - 5
		{
			volume, "object-as-dir",
			0, 5, nil, errIsNotRegular},
		// One path segment length is > 255 chars long. - 6
		{
			volume, "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Path length is > 1024 chars long. - 7
		{
			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Buffer size greater than object size. - 8
		{
			volume, "myobject", 0, 16,
			[]byte("hello, world"),
			io.ErrUnexpectedEOF,
		},
		// Reading from an offset success. - 9
		{
			volume, "myobject", 7, 5,
			[]byte("world"), nil,
		},
		// Reading from an object but buffer size greater. - 10
		{
			volume, "myobject",
			7, 8,
			[]byte("world"),
			io.ErrUnexpectedEOF,
		},
		// Seeking into a wrong offset, return PathError. - 11
		{
			volume, "myobject",
			-1, 5,
			nil,
			func() error {
				if runtime.GOOS == "windows" {
					return &os.PathError{
						Op:   "seek",
						Path: preparePath(slashpath.Join(path, "success-vol", "myobject")),
						Err:  errors.New("An attempt was made to move the file pointer before the beginning of the file."),
					}
				}
				return &os.PathError{
					Op:   "seek",
					Path: preparePath(slashpath.Join(path, "success-vol", "myobject")),
					Err:  os.ErrInvalid,
				}
			}(),
		},
		// Seeking ahead returns io.EOF. - 12
		{
			volume, "myobject", 14, 1, nil, io.EOF,
		},
		// Empty volume name. - 13
		{
			"", "myobject", 14, 1, nil, errInvalidArgument,
		},
		// Empty filename name. - 14
		{
			volume, "", 14, 1, nil, errIsNotRegular,
		},
		// Non existent volume name - 15.
		{
			"abcd", "", 14, 1, nil, errVolumeNotFound,
		},
		// Non existent filename - 16.
		{
			volume, "abcd", 14, 1, nil, errFileNotFound,
		},
	}

	// Create all files needed during testing.
	appendFiles := testCases[:4]

	// Create test files for further reading.
	for i, appendFile := range appendFiles {
		err = posix.AppendFile(volume, appendFile.fileName, []byte("hello, world"))
		if err != appendFile.expectedErr {
			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
		}
	}

	// Following block validates all ReadFile test cases.
	for i, testCase := range testCases {
		// Common read buffer.
		var buf = make([]byte, testCase.bufSize)
		n, err := posix.ReadFile(testCase.volume, testCase.fileName, testCase.offset, buf)
		if err != nil && testCase.expectedErr != nil {
			// Validate if the type string of the errors are an exact match.
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
			}
			// Err unexpected EOF special case, where we verify we have provided a larger
			// buffer than the data itself, but the results are in-fact valid. So we validate
			// this error condition specifically treating it as a good condition with valid
			// results. In this scenario return 'n' is always lesser than the input buffer.
			if err == io.ErrUnexpectedEOF {
				if !bytes.Equal(testCase.expectedBuf, buf[:n]) {
					t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:testCase.bufSize]))
				}
				if n > int64(len(buf)) {
					t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
				}
			}
		}
		// ReadFile has returned success, but our expected error is non 'nil'.
		if err == nil && err != testCase.expectedErr {
			t.Errorf("Case: %d %#v, expected: %s, got :%s", i+1, testCase, testCase.expectedErr, err)
		}
		// Expected error retured, proceed further to validate the returned results.
		if err == nil && err == testCase.expectedErr {
			if !bytes.Equal(testCase.expectedBuf, buf) {
				t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:testCase.bufSize]))
			}
			if n != int64(testCase.bufSize) {
				t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
			}
		}
	}

	// Test for permission denied.
	if runtime.GOOS == "linux" {
		// Initialize posix storage layer for permission denied error.
		posix, err := newPosix("/")
		if err != nil {
			t.Errorf("Unable to initialize posix, %s", err)
		}
		if err == nil {
			// Common read buffer.
			var buf = make([]byte, 10)
			if _, err = posix.ReadFile("proc", "1/fd", 0, buf); err != errFileAccessDenied {
				t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
			}
		}
	}
}

// Test posix.AppendFile()
func TestAppendFile(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err = posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		fileName    string
		expectedErr error
	}{
		{"myobject", nil},
		{"path/to/my/object", nil},
		// Test to append to previously created file.
		{"myobject", nil},
		// Test to use same path of previously created file.
		{"path/to/my/testobject", nil},
		// One path segment length is 255 chars long.
		{"path/to/my/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", nil},
		{"object-as-dir", errIsNotRegular},
		// path segment uses previously uploaded object.
		{"myobject/testobject", errFileAccessDenied},
		// One path segment length is > 255 chars long.
		{"path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
		// path length is > 1024 chars long.
		{"level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
	}

	// Add path length > 1024 test specially as OS X system does not support 1024 long path.
	err = errFileNameTooLong
	if runtime.GOOS != "darwin" {
		err = nil
	}
	// path length is 1024 chars long.
	testCases = append(testCases, struct {
		fileName    string
		expectedErr error
	}{"level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", err})

	for _, testCase := range testCases {
		if err := posix.AppendFile("success-vol", testCase.fileName, []byte("hello, world")); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// Test for permission denied.
	if runtime.GOOS != "windows" {
		// Initialize posix storage layer for permission denied error.
		posix, err := newPosix("/usr")
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err := posix.AppendFile("bin", "yes", []byte("hello, world")); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}
}

// Test posix.RenameFile()
func TestRenameFile(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err := posix.MakeVol("src-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posix.MakeVol("dest-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posix.AppendFile("src-vol", "file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posix.AppendFile("src-vol", "file2", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posix.AppendFile("src-vol", "path/to/file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcPath     string
		destPath    string
		expectedErr error
	}{
		{"file1", "file-one", nil},
		{"path/", "new-path/", nil},
		// Test to overwrite destination file.
		{"file2", "file-one", nil},
		// Test to check failure of source and destination are not same type.
		{"path/", "file-one", errFileAccessDenied},
		// Test to check failure of destination directory exists.
		{"path/", "new-path/", errFileAccessDenied},
	}

	for _, testCase := range testCases {
		if err := posix.RenameFile("src-vol", testCase.srcPath, "dest-vol", testCase.destPath); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}
}

// Test posix.StatFile()
func TestStatFile(t *testing.T) {
	// create posix test setup
	posix, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer removeAll(path)

	// Setup test environment.
	if err := posix.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posix.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posix.AppendFile("success-vol", "path/to/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		path        string
		expectedErr error
	}{
		{"success-file", nil},
		{"path/to/success-file", nil},
		// Test nonexistent file.
		{"nonexistent-file", errFileNotFound},
		// Test nonexistent path.
		{"path/2/success-file", errFileNotFound},
		// Test a directory.
		{"path", errFileNotFound},
	}

	for _, testCase := range testCases {
		if _, err := posix.StatFile("success-vol", testCase.path); err != testCase.expectedErr {
			t.Fatalf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}
}
