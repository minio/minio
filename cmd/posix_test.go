/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	slashpath "path"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/minio/minio/pkg/disk"
)

// creates a temp dir and sets up posix layer.
// returns posix layer, temp dir path to be used for the purpose of tests.
func newPosixTestSetup() (StorageAPI, string, error) {
	diskPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		return nil, "", err
	}
	// Initialize a new posix layer.
	posixStorage, err := newPosix(diskPath)
	if err != nil {
		return nil, "", err
	}
	return posixStorage, diskPath, nil
}

// createPermDeniedFile - creates temporary directory and file with path '/mybucket/myobject'
func createPermDeniedFile(t *testing.T) (permDeniedDir string) {
	var errMsg string

	defer func() {
		if errMsg == "" {
			return
		}

		if permDeniedDir != "" {
			os.RemoveAll(permDeniedDir)
		}

		t.Fatalf(errMsg)
	}()

	var err error
	if permDeniedDir, err = ioutil.TempDir(globalTestTmpDir, "minio-"); err != nil {
		errMsg = fmt.Sprintf("Unable to create temporary directory. %v", err)
		return permDeniedDir
	}

	if err = os.Mkdir(slashpath.Join(permDeniedDir, "mybucket"), 0775); err != nil {
		errMsg = fmt.Sprintf("Unable to create temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
		return permDeniedDir
	}

	if err = ioutil.WriteFile(slashpath.Join(permDeniedDir, "mybucket", "myobject"), []byte(""), 0400); err != nil {
		errMsg = fmt.Sprintf("Unable to create file %v. %v", slashpath.Join(permDeniedDir, "mybucket", "myobject"), err)
		return permDeniedDir
	}

	if err = os.Chmod(slashpath.Join(permDeniedDir, "mybucket"), 0400); err != nil {
		errMsg = fmt.Sprintf("Unable to change permission to temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
		return permDeniedDir
	}

	if err = os.Chmod(permDeniedDir, 0400); err != nil {
		errMsg = fmt.Sprintf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
	}

	return permDeniedDir
}

// removePermDeniedFile - removes temporary directory and file with path '/mybucket/myobject'
func removePermDeniedFile(permDeniedDir string) {
	if err := os.Chmod(permDeniedDir, 0775); err == nil {
		if err = os.Chmod(slashpath.Join(permDeniedDir, "mybucket"), 0775); err == nil {
			os.RemoveAll(permDeniedDir)
		}
	}
}

// TestPosixs posix.getDiskInfo()
func TestPosixGetDiskInfo(t *testing.T) {
	path, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Unable to create a temporary directory, %s", err)
	}
	defer os.RemoveAll(path)

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

func TestPosixIsDirEmpty(t *testing.T) {
	tmp, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	// Should give false on non-existent directory.
	dir1 := slashpath.Join(tmp, "non-existent-directory")
	if isDirEmpty(dir1) != false {
		t.Error("expected false for non-existent directory, got true")
	}

	// Should give false for not-a-directory.
	dir2 := slashpath.Join(tmp, "file")
	err = ioutil.WriteFile(dir2, []byte("hello"), 0777)
	if err != nil {
		t.Fatal(err)
	}

	if isDirEmpty(dir2) != false {
		t.Error("expected false for a file, got true")
	}

	// Should give true for a real empty directory.
	dir3 := slashpath.Join(tmp, "empty")
	err = os.Mkdir(dir3, 0777)
	if err != nil {
		t.Fatal(err)
	}

	if isDirEmpty(dir3) != true {
		t.Error("expected true for empty dir, got false")
	}
}

// TestPosixReadAll - TestPosixs the functionality implemented by posix ReadAll storage API.
func TestPosixReadAll(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}

	defer os.RemoveAll(path)

	// Create files for the test cases.
	if err = posixStorage.MakeVol("exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = posixStorage.AppendFile("exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = posixStorage.AppendFile("exists", "as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}
	if err = posixStorage.AppendFile("exists", "as-file-parent", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
	}

	// TestPosixcases to validate different conditions for ReadAll API.
	testCases := []struct {
		volume string
		path   string
		err    error
	}{
		// TestPosix case - 1.
		// Validate volume does not exist.
		{
			volume: "i-dont-exist",
			path:   "",
			err:    errVolumeNotFound,
		},
		// TestPosix case - 2.
		// Validate bad condition file does not exist.
		{
			volume: "exists",
			path:   "as-file-not-found",
			err:    errFileNotFound,
		},
		// TestPosix case - 3.
		// Validate bad condition file exists as prefix/directory and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "as-directory",
			err:    errFileNotFound,
		},
		// TestPosix case - 4.
		{
			volume: "exists",
			path:   "as-file-parent/as-file",
			err:    errFileNotFound,
		},
		// TestPosix case - 5.
		// Validate the good condition file exists and we are able to read it.
		{
			volume: "exists",
			path:   "as-file",
			err:    nil,
		},
		// TestPosix case - 6.
		// TestPosix case with invalid volume name.
		{
			volume: "ab",
			path:   "as-file",
			err:    errInvalidArgument,
		},
	}

	var dataRead []byte
	// Run through all the test cases and validate for ReadAll.
	for i, testCase := range testCases {
		dataRead, err = posixStorage.ReadAll(testCase.volume, testCase.path)
		if err != testCase.err {
			t.Fatalf("TestPosix %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
		}
		if err == nil {
			if string(dataRead) != string([]byte("Hello, World")) {
				t.Errorf("TestPosix %d: Expected the data read to be \"%s\", but instead got \"%s\"", i+1, "Hello, World", string(dataRead))
			}
		}
	}
	// TestPosixing for faulty disk.
	// Setting ioErrCount > maxAllowedIOError.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(6)
	} else {
		t.Errorf("Expected the StorageAPI to be of type *posix")
	}
	_, err = posixStorage.ReadAll("abcd", "efg")
	if err != errFaultyDisk {
		t.Errorf("Expected err \"%s\", got err \"%s\"", errFaultyDisk, err)
	}
}

// TestPosixNewPosix all the cases handled in posix storage layer initialization.
func TestPosixNewPosix(t *testing.T) {
	// Temporary dir name.
	tmpDirName := globalTestTmpDir + "/" + "minio-" + nextSuffix()
	// Temporary file name.
	tmpFileName := globalTestTmpDir + "/" + "minio-" + nextSuffix()
	f, _ := os.Create(tmpFileName)
	f.Close()
	defer os.Remove(tmpFileName)

	// List of all tests for posix initialization.
	testCases := []struct {
		name string
		err  error
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
		// Initialize a new posix layer.
		_, err := newPosix(testCase.name)
		if err != testCase.err {
			t.Fatalf("TestPosix %d failed wanted: %s, got: %s", i+1, err, testCase.err)
		}
	}
}

// TestPosixMakeVol - TestPosix validate the logic for creation of new posix volume.
// Asserts the failures too against the expected failures.
func TestPosixMakeVol(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

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
		ioErrCount  int
		expectedErr error
	}{
		// TestPosix case - 1.
		// A valid case, volume creation is expected to succeed.
		{
			volName:     "success-vol",
			ioErrCount:  0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		// Case where a file exists by the name of the volume to be created.
		{
			volName:     "vol-as-file",
			ioErrCount:  0,
			expectedErr: errVolumeExists,
		},
		// TestPosix case - 3.
		{
			volName:     "existing-vol",
			ioErrCount:  0,
			expectedErr: errVolumeExists,
		},
		// TestPosix case - 4.
		// IO error > maxAllowedIOError, should fail with errFaultyDisk.
		{
			volName:     "vol",
			ioErrCount:  6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 5.
		// TestPosix case with invalid volume name.
		{
			volName:     "ab",
			ioErrCount:  0,
			expectedErr: errInvalidArgument,
		},
	}

	for i, testCase := range testCases {
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCount)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		if err := posixStorage.MakeVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("TestPosix %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
		if err != nil {
			t.Fatalf("Unable to create temporary directory. %v", err)
		}
		defer os.RemoveAll(permDeniedDir)
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}
		// Initialize posix storage layer for permission denied error.
		posixStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err := posixStorage.MakeVol("test-vol"); err != errDiskAccessDenied {
			t.Fatalf("expected: %s, got: %s", errDiskAccessDenied, err)
		}
	}
}

// TestPosixDeleteVol - Validates the expected behaviour of posix.DeleteVol for various cases.
func TestPosixDeleteVol(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// TestPosix failure cases.
	vol := slashpath.Join(path, "nonempty-vol")
	if err = os.Mkdir(vol, 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}
	if err = ioutil.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		volName     string
		ioErrCount  int
		expectedErr error
	}{
		// TestPosix case  - 1.
		// A valida case. Empty vol, should be possible to delete.
		{
			volName:     "success-vol",
			ioErrCount:  0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		// volume is non-existent.
		{
			volName:     "nonexistent-vol",
			ioErrCount:  0,
			expectedErr: errVolumeNotFound,
		},
		// TestPosix case - 3.
		// It shouldn't be possible to delete an non-empty volume, validating the same.
		{
			volName:     "nonempty-vol",
			ioErrCount:  0,
			expectedErr: errVolumeNotEmpty,
		},
		// TestPosix case - 4.
		// IO error > maxAllowedIOError, should fail with errFaultyDisk.
		{
			volName:     "my-disk",
			ioErrCount:  6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 5.
		// Invalid volume name.
		{
			volName:     "ab",
			ioErrCount:  0,
			expectedErr: errInvalidArgument,
		},
	}

	for i, testCase := range testCases {
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCount)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		if err = posixStorage.DeleteVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("TestPosix: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		var permDeniedDir string
		if permDeniedDir, err = ioutil.TempDir(globalTestTmpDir, "minio-"); err != nil {
			t.Fatalf("Unable to create temporary directory. %v", err)
		}
		defer removePermDeniedFile(permDeniedDir)
		if err = os.Mkdir(slashpath.Join(permDeniedDir, "mybucket"), 0400); err != nil {
			t.Fatalf("Unable to create temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
		}
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}
		// Initialize posix storage layer for permission denied error.
		posixStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err = posixStorage.DeleteVol("bin"); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	posixDeletedStorage, diskPath, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestPosix for delete on an removed disk.
	// should fail with disk not found.
	err = posixDeletedStorage.DeleteVol("Del-Vol")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestPosixStatVol - TestPosixs validate the volume info returned by posix.StatVol() for various inputs.
func TestPosixStatVol(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	testCases := []struct {
		volName     string
		ioErrCount  int
		expectedErr error
	}{
		// TestPosix case - 1.
		{
			volName:     "success-vol",
			ioErrCount:  0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		{
			volName:     "nonexistent-vol",
			ioErrCount:  0,
			expectedErr: errVolumeNotFound,
		},
		// TestPosix case - 3.
		{
			volName:     "success-vol",
			ioErrCount:  6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 4.
		{
			volName:     "ab",
			ioErrCount:  0,
			expectedErr: errInvalidArgument,
		},
	}

	for i, testCase := range testCases {
		var volInfo VolInfo
		// setting ioErrCnt from the test case.
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCount)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		volInfo, err = posixStorage.StatVol(testCase.volName)
		if err != testCase.expectedErr {
			t.Fatalf("TestPosix case : %d, Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}

		if err == nil {
			if volInfo.Name != volInfo.Name {
				t.Errorf("TestPosix case %d: Expected the volume name to be \"%s\", instead found \"%s\"", i+1, volInfo.Name, volInfo.Name)
			}
		}
	}

	posixDeletedStorage, diskPath, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestPosix for delete on an removed disk.
	// should fail with disk not found.
	_, err = posixDeletedStorage.StatVol("Stat vol")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestPosixListVols - Validates the result and the error output for posix volume listing functionality posix.ListVols().
func TestPosixListVols(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}

	var volInfo []VolInfo
	// TestPosix empty list vols.
	if volInfo, err = posixStorage.ListVols(); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfo) != 0 {
		t.Fatalf("expected: [], got: %s", volInfo)
	}

	// TestPosix non-empty list vols.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if volInfo, err = posixStorage.ListVols(); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfo) != 1 {
		t.Fatalf("expected: 1, got: %d", len(volInfo))
	} else if volInfo[0].Name != "success-vol" {
		t.Errorf("expected: success-vol, got: %s", volInfo[0].Name)
	}
	// setting ioErrCnt to be > maxAllowedIOError.
	// should fail with errFaultyDisk.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(6)
	} else {
		t.Errorf("Expected the StorageAPI to be of type *posix")
	}
	if _, err = posixStorage.ListVols(); err != errFaultyDisk {
		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errFaultyDisk, err)
	}
	// removing the path and simulating disk failure
	os.RemoveAll(path)
	// Resetting the IO error.
	// should fail with errDiskNotFound.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(0)
	} else {
		t.Errorf("Expected the StorageAPI to be of type *posix")
	}
	if _, err = posixStorage.ListVols(); err != errDiskNotFound {
		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errDiskNotFound, err)
	}
}

// TestPosixPosixListDir -  TestPosixs validate the directory listing functionality provided by posix.ListDir .
func TestPosixPosixListDir(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create posix test setup.
	posixDeletedStorage, diskPath, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = posixStorage.AppendFile("success-vol", "abc/def/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err = posixStorage.AppendFile("success-vol", "abc/xyz/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol   string
		srcPath  string
		ioErrCnt int
		// expected result.
		expectedListDir []string
		expectedErr     error
	}{
		// TestPosix case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc",
			ioErrCnt:        0,
			expectedListDir: []string{"def/", "xyz/"},
			expectedErr:     nil,
		},
		// TestPosix case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def",
			ioErrCnt:        0,
			expectedListDir: []string{"ghi/"},
			expectedErr:     nil,
		},
		// TestPosix case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def/ghi",
			ioErrCnt:        0,
			expectedListDir: []string{"success-file"},
			expectedErr:     nil,
		},
		// TestPosix case - 2.
		{
			srcVol:      "success-vol",
			srcPath:     "abcdef",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 3.
		// TestPosix case with invalid volume name.
		{
			srcVol:      "ab",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errInvalidArgument,
		},
		// TestPosix case - 4.
		// TestPosix case with io error count > max limit.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 5.
		// TestPosix case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		var dirList []string
		// setting ioErrCnt from the test case.
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCnt)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		dirList, err = posixStorage.ListDir(testCase.srcVol, testCase.srcPath)
		if err != testCase.expectedErr {
			t.Fatalf("TestPosix case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
		if err == nil {
			for _, expected := range testCase.expectedListDir {
				if !strings.Contains(strings.Join(dirList, ","), expected) {
					t.Errorf("TestPosix case %d: Expected the directory listing to be \"%v\", but got \"%v\"", i+1, testCase.expectedListDir, dirList)
				}
			}
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize posix storage layer for permission denied error.
		posixStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		errMsg := fmt.Sprintf("stat %v: permission denied", slashpath.Join(permDeniedDir, "mybucket"))
		if err = posixStorage.DeleteFile("mybucket", "myobject"); err.Error() != errMsg {
			t.Errorf("expected: %s, got: %s", errMsg, err)
		}
	}

	// TestPosix for delete on an removed disk.
	// should fail with disk not found.
	err = posixDeletedStorage.DeleteFile("del-vol", "my-file")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestPosixDeleteFile - Series of test cases construct valid and invalid input data and validates the result and the error response.
func TestPosixDeleteFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create posix test setup
	posixDeletedStorage, diskPath, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = posixStorage.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err = posixStorage.MakeVol("no-permissions"); err != nil {
		t.Fatalf("Unable to create volume, %s", err.Error())
	}
	if err = posixStorage.AppendFile("no-permissions", "dir/file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err.Error())
	}
	// Parent directory must have write permissions, this is read + execute.
	if err = os.Chmod(pathJoin(path, "no-permissions"), 0555); err != nil {
		t.Fatalf("Unable to chmod directory, %s", err.Error())
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		ioErrCnt    int
		expectedErr error
	}{
		// TestPosix case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		// The file was deleted in the last  case, so DeleteFile should fail.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 3.
		// TestPosix case with io error count > max limit.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 4.
		// TestPosix case with segment of the volume name > 255.
		{
			srcVol:      "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errInvalidArgument,
		},
		// TestPosix case - 5.
		// TestPosix case with non-existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errVolumeNotFound,
		},
		// TestPosix case - 6.
		// TestPosix case with src path segment > 255.
		{
			srcVol:      "success-vol",
			srcPath:     "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			ioErrCnt:    0,
			expectedErr: errFileNameTooLong,
		},
		// TestPosix case - 7.
		// TestPosix case with undeletable parent directory.
		// File can delete, dir cannot delete because no-permissions doesn't have write perms.
		{
			srcVol:      "no-permissions",
			srcPath:     "dir/file",
			ioErrCnt:    0,
			expectedErr: nil,
		},
	}

	for i, testCase := range testCases {
		// setting ioErrCnt from the test case.
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCnt)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		if err = posixStorage.DeleteFile(testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
			t.Errorf("TestPosix case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize posix storage layer for permission denied error.
		posixStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		errMsg := fmt.Sprintf("stat %v: permission denied", slashpath.Join(permDeniedDir, "mybucket"))
		if err = posixStorage.DeleteFile("mybucket", "myobject"); err.Error() != errMsg {
			t.Errorf("expected: %s, got: %s", errMsg, err)
		}
	}

	// TestPosix for delete on an removed disk.
	// should fail with disk not found.
	err = posixDeletedStorage.DeleteFile("del-vol", "my-file")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestPosixReadFile - TestPosixs posix.ReadFile with wide range of cases and asserts the result and error response.
func TestPosixReadFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	volume := "success-vol"
	// Setup test environment.
	if err = posixStorage.MakeVol(volume); err != nil {
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
		// Seeking ahead returns io.EOF. - 11
		{
			volume, "myobject", 14, 1, nil, io.EOF,
		},
		// Empty volume name. - 12
		{
			"", "myobject", 14, 1, nil, errInvalidArgument,
		},
		// Empty filename name. - 13
		{
			volume, "", 14, 1, nil, errIsNotRegular,
		},
		// Non existent volume name - 14.
		{
			"abcd", "", 14, 1, nil, errVolumeNotFound,
		},
		// Non existent filename - 15.
		{
			volume, "abcd", 14, 1, nil, errFileNotFound,
		},
	}

	// Create all files needed during testing.
	appendFiles := testCases[:4]

	// Create test files for further reading.
	for i, appendFile := range appendFiles {
		err = posixStorage.AppendFile(volume, appendFile.fileName, []byte("hello, world"))
		if err != appendFile.expectedErr {
			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
		}
	}

	// Check PathError specially.
	{
		buf := make([]byte, 5)
		if _, err = posixStorage.ReadFile(volume, "myobject", -1, buf, nil); err != nil {
			isPathError := false
			switch err.(type) {
			case *os.PathError:
				isPathError = true
			}

			if !isPathError {
				t.Fatalf("expected: <os.PathError>, got: %v", err)
			}
		} else {
			t.Fatalf("expected: <os.PathError>, got: <nil>")
		}
	}

	// Following block validates all ReadFile test cases.
	for i, testCase := range testCases {
		var n int64
		// Common read buffer.
		var buf = make([]byte, testCase.bufSize)
		n, err = posixStorage.ReadFile(testCase.volume, testCase.fileName, testCase.offset, buf, nil)
		if err != nil && testCase.expectedErr != nil {
			// Validate if the type string of the errors are an exact match.
			if err.Error() != testCase.expectedErr.Error() {
				if runtime.GOOS != globalWindowsOSName {
					t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
				} else {
					var resultErrno, expectErrno uintptr
					if pathErr, ok := err.(*os.PathError); ok {
						if errno, pok := pathErr.Err.(syscall.Errno); pok {
							resultErrno = uintptr(errno)
						}
					}
					if pathErr, ok := testCase.expectedErr.(*os.PathError); ok {
						if errno, pok := pathErr.Err.(syscall.Errno); pok {
							expectErrno = uintptr(errno)
						}
					}
					if !(expectErrno != 0 && resultErrno != 0 && expectErrno == resultErrno) {
						t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
					}
				}
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

	// TestPosix for permission denied.
	if runtime.GOOS == "linux" {
		// Initialize posix storage layer for permission denied error.
		posixStorage, err = newPosix("/")
		if err != nil {
			t.Errorf("Unable to initialize posix, %s", err)
		}
		if err == nil {
			// Common read buffer.
			var buf = make([]byte, 10)
			if _, err = posixStorage.ReadFile("proc", "1/fd", 0, buf, nil); err != errFileAccessDenied {
				t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
			}
		}
	}

	// TestPosixing for faulty disk.
	// setting ioErrCnt to 6.
	// should fail with errFaultyDisk.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(6)
		// Common read buffer.
		var buf = make([]byte, 10)
		_, err = posixType.ReadFile("abc", "yes", 0, buf, nil)
		if err != errFaultyDisk {
			t.Fatalf("Expected \"Faulty Disk\", got: \"%s\"", err)
		}
	} else {
		t.Fatalf("Expected the StorageAPI to be of type *posix")
	}
}

var posixReadFileWithVerifyTests = []struct {
	file      string
	offset    int
	length    int
	algorithm BitrotAlgorithm
	expError  error
}{
	{file: "myobject", offset: 0, length: 100, algorithm: SHA256, expError: nil},                     // 0
	{file: "myobject", offset: 25, length: 74, algorithm: SHA256, expError: nil},                     // 1
	{file: "myobject", offset: 29, length: 70, algorithm: SHA256, expError: nil},                     // 2
	{file: "myobject", offset: 100, length: 0, algorithm: SHA256, expError: nil},                     // 3
	{file: "myobject", offset: 1, length: 120, algorithm: SHA256, expError: hashMismatchError{}},     // 4
	{file: "myobject", offset: 3, length: 1100, algorithm: SHA256, expError: nil},                    // 5
	{file: "myobject", offset: 2, length: 100, algorithm: SHA256, expError: hashMismatchError{}},     // 6
	{file: "myobject", offset: 1000, length: 1001, algorithm: SHA256, expError: nil},                 // 7
	{file: "myobject", offset: 0, length: 100, algorithm: BLAKE2b512, expError: hashMismatchError{}}, // 8
	{file: "myobject", offset: 25, length: 74, algorithm: BLAKE2b512, expError: nil},                 // 9
	{file: "myobject", offset: 29, length: 70, algorithm: BLAKE2b512, expError: hashMismatchError{}}, // 10
	{file: "myobject", offset: 100, length: 0, algorithm: BLAKE2b512, expError: nil},                 // 11
	{file: "myobject", offset: 1, length: 120, algorithm: BLAKE2b512, expError: nil},                 // 12
	{file: "myobject", offset: 3, length: 1100, algorithm: BLAKE2b512, expError: nil},                // 13
	{file: "myobject", offset: 2, length: 100, algorithm: BLAKE2b512, expError: nil},                 // 14
	{file: "myobject", offset: 1000, length: 1001, algorithm: BLAKE2b512, expError: nil},             // 15
}

// TestPosixReadFile with bitrot verification - tests the posix level
// ReadFile API with a BitrotVerifier. Only tests hashing related
// functionality. Other functionality is tested with
// TestPosixReadFile.
func TestPosixReadFileWithVerify(t *testing.T) {
	volume, object := "test-vol", "myobject"
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	if err = posixStorage.MakeVol(volume); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create volume %s: %v", volume, err)
	}
	data := make([]byte, 8*1024)
	if _, err = io.ReadFull(rand.Reader, data); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create generate random data: %v", err)
	}
	if err = posixStorage.AppendFile(volume, object, data); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create object: %v", err)
	}

	for i, test := range posixReadFileWithVerifyTests {
		h := test.algorithm.New()
		h.Write(data)
		if test.expError != nil {
			expected := h.Sum(nil)
			h.Write([]byte{0})
			test.expError = hashMismatchError{hex.EncodeToString(h.Sum(nil)), hex.EncodeToString(expected)}
		}

		buffer := make([]byte, test.length)
		n, err := posixStorage.ReadFile(volume, test.file, int64(test.offset), buffer, NewBitrotVerifier(test.algorithm, h.Sum(nil)))

		switch {
		case err == nil && test.expError != nil:
			t.Errorf("Test %d: Expected error %v but got none.", i, test.expError)
		case err == nil && n != int64(test.length):
			t.Errorf("Test %d: %d bytes were expected, but %d were written", i, test.length, n)
		case err == nil && !bytes.Equal(data[test.offset:test.offset+test.length], buffer):
			t.Errorf("Test %d: Expected bytes: %v, but got: %v", i, data[test.offset:test.offset+test.length], buffer)
		case err != nil && err != test.expError:
			t.Errorf("Test %d: Expected error: %v, but got: %v", i, test.expError, err)
		}
	}
}

// TestPosix posix.AppendFile()
func TestPosixAppendFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
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
		// TestPosix to append to previously created file.
		{"myobject", nil},
		// TestPosix to use same path of previously created file.
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
		if err = posixStorage.AppendFile("success-vol", testCase.fileName, []byte("hello, world")); err != testCase.expectedErr {
			t.Errorf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		var posixPermStorage StorageAPI
		// Initialize posix storage layer for permission denied error.
		posixPermStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err = posixPermStorage.AppendFile("mybucket", "myobject", []byte("hello, world")); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}
	// TestPosix case with invalid volume name.
	// A valid volume name should be atleast of size 3.
	err = posixStorage.AppendFile("bn", "yes", []byte("hello, world"))
	if err != errInvalidArgument {
		t.Fatalf("expected: \"Invalid argument error\", got: \"%s\"", err)
	}

	// TestPosix case with IO error count > max limit.

	// setting ioErrCnt to 6.
	// should fail with errFaultyDisk.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(6)
		err = posixType.AppendFile("abc", "yes", []byte("hello, world"))
		if err != errFaultyDisk {
			t.Fatalf("Expected \"Faulty Disk\", got: \"%s\"", err)
		}
	} else {
		t.Fatalf("Expected the StorageAPI to be of type *posix")
	}
}

// TestPosix posix.PrepareFile()
func TestPosixPrepareFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		fileName    string
		expectedErr error
	}{
		{"myobject", nil},
		{"path/to/my/object", nil},
		// TestPosix to append to previously created file.
		{"myobject", nil},
		// TestPosix to use same path of previously created file.
		{"path/to/my/testobject", nil},
		{"object-as-dir", errIsNotRegular},
		// path segment uses previously uploaded object.
		{"myobject/testobject", errFileAccessDenied},
		// One path segment length is > 255 chars long.
		{"path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
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
		if err = posixStorage.PrepareFile("success-vol", testCase.fileName, 16); err != testCase.expectedErr {
			t.Errorf("Case: %s, expected: %s, got: %s", testCase, testCase.expectedErr, err)
		}
	}

	// TestPosix for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		var posixPermStorage StorageAPI
		// Initialize posix storage layer for permission denied error.
		posixPermStorage, err = newPosix(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize posix, %s", err)
		}

		if err = posixPermStorage.PrepareFile("mybucket", "myobject", 16); !os.IsPermission(err) {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	// TestPosix case with invalid file size which should be strictly positive
	err = posixStorage.PrepareFile("bn", "yes", -3)
	if err != errInvalidArgument {
		t.Fatalf("should fail: %v", err)
	}

	// TestPosix case with invalid volume name.
	// A valid volume name should be atleast of size 3.
	err = posixStorage.PrepareFile("bn", "yes", 16)
	if err != errInvalidArgument {
		t.Fatalf("expected: \"Invalid argument error\", got: \"%s\"", err)
	}

	// TestPosix case with IO error count > max limit.

	// setting ioErrCnt to 6.
	// should fail with errFaultyDisk.
	if posixType, ok := posixStorage.(*posix); ok {
		// setting the io error count from as specified in the test case.
		posixType.ioErrCount = int32(6)
		err = posixType.PrepareFile("abc", "yes", 16)
		if err != errFaultyDisk {
			t.Fatalf("Expected \"Faulty Disk\", got: \"%s\"", err)
		}
	} else {
		t.Fatalf("Expected the StorageAPI to be of type *posix")
	}
}

// TestPosix posix.RenameFile()
func TestPosixRenameFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := posixStorage.MakeVol("src-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posixStorage.MakeVol("dest-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posixStorage.AppendFile("src-vol", "file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posixStorage.AppendFile("src-vol", "file2", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := posixStorage.AppendFile("src-vol", "file3", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := posixStorage.AppendFile("src-vol", "file4", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posixStorage.AppendFile("src-vol", "file5", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := posixStorage.AppendFile("src-vol", "path/to/file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol      string
		destVol     string
		srcPath     string
		destPath    string
		ioErrCnt    int
		expectedErr error
	}{
		// TestPosix case - 1.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file1",
			destPath:    "file-one",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 3.
		// TestPosix to overwrite destination file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file2",
			destPath:    "file-one",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 4.
		// TestPosix case with io error count set to 1.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file3",
			destPath:    "file-two",
			ioErrCnt:    1,
			expectedErr: nil,
		},
		// TestPosix case - 5.
		// TestPosix case with io error count set to maximum allowed count.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "file-three",
			ioErrCnt:    5,
			expectedErr: nil,
		},
		// TestPosix case - 6.
		// TestPosix case with non-existent source file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "non-existent-file",
			destPath:    "file-three",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 7.
		// TestPosix to check failure of source and destination are not same type.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "file-one",
			ioErrCnt:    0,
			expectedErr: errFileAccessDenied,
		},
		// TestPosix case - 8.
		// TestPosix to check failure of destination directory exists.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errFileAccessDenied,
		},
		// TestPosix case - 9.
		// TestPosix case with io error count is greater than maxAllowedIOError.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			ioErrCnt:    6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 10.
		// TestPosix case with source being a file and destination being a directory.
		// Either both have to be files or directories.
		// Expecting to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errFileAccessDenied,
		},
		// TestPosix case - 11.
		// TestPosix case with non-existent source volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol-non-existent",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errVolumeNotFound,
		},
		// TestPosix case - 12.
		// TestPosix case with non-existent destination volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errVolumeNotFound,
		},
		// TestPosix case - 13.
		// TestPosix case with invalid src volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "ab",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errInvalidArgument,
		},
		// TestPosix case - 14.
		// TestPosix case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errInvalidArgument,
		},
		// TestPosix case - 15.
		// TestPosix case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			ioErrCnt:    0,
			expectedErr: errInvalidArgument,
		},
		// TestPosix case - 16.
		// TestPosix case with the parent of the destination being a file.
		// expected to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file5",
			destPath:    "file-one/parent-is-file",
			ioErrCnt:    0,
			expectedErr: errFileAccessDenied,
		},
		// TestPosix case - 17.
		// TestPosix case with segment of source file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			destPath:    "file-six",
			ioErrCnt:    0,
			expectedErr: errFileNameTooLong,
		},
		// TestPosix case - 18.
		// TestPosix case with segment of destination file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file6",
			destPath:    "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			ioErrCnt:    0,
			expectedErr: errFileNameTooLong,
		},
	}

	for i, testCase := range testCases {
		// setting ioErrCnt from the test case.
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCnt)
		} else {
			t.Fatalf("Expected the StorageAPI to be of type *posix")
		}

		if err := posixStorage.RenameFile(testCase.srcVol, testCase.srcPath, testCase.destVol, testCase.destPath); err != testCase.expectedErr {
			t.Fatalf("TestPosix %d:  Expected the error to be : \"%v\", got: \"%v\".", i+1, testCase.expectedErr, err)
		}
	}
}

// TestPosix posix.StatFile()
func TestPosixStatFile(t *testing.T) {
	// create posix test setup
	posixStorage, path, err := newPosixTestSetup()
	if err != nil {
		t.Fatalf("Unable to create posix test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := posixStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := posixStorage.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := posixStorage.AppendFile("success-vol", "path/to/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		ioErrCnt    int
		expectedErr error
	}{
		// TestPosix case - 1.
		// TestPosix case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 2.
		// TestPosix case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "path/to/success-file",
			ioErrCnt:    0,
			expectedErr: nil,
		},
		// TestPosix case - 3.
		// TestPosix case with non-existent file.
		{
			srcVol:      "success-vol",
			srcPath:     "nonexistent-file",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 4.
		// TestPosix case with non-existent file path.
		{
			srcVol:      "success-vol",
			srcPath:     "path/2/success-file",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 5.
		// TestPosix case with path being a directory.
		{
			srcVol:      "success-vol",
			srcPath:     "path",
			ioErrCnt:    0,
			expectedErr: errFileNotFound,
		},
		// TestPosix case - 6.
		// TestPosix case with io error count > max limit.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			ioErrCnt:    6,
			expectedErr: errFaultyDisk,
		},
		// TestPosix case - 7.
		// TestPosix case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			ioErrCnt:    0,
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		// setting ioErrCnt from the test case.
		if posixType, ok := posixStorage.(*posix); ok {
			// setting the io error count from as specified in the test case.
			posixType.ioErrCount = int32(testCase.ioErrCnt)
		} else {
			t.Errorf("Expected the StorageAPI to be of type *posix")
		}
		if _, err := posixStorage.StatFile(testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
			t.Fatalf("TestPosix case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}
}

// Checks for restrictions for min total disk space and inodes.
func TestCheckDiskTotalMin(t *testing.T) {
	testCases := []struct {
		diskInfo disk.Info
		err      error
	}{
		// Test 1 - when fstype is nfs.
		{
			diskInfo: disk.Info{
				Total:  diskMinTotalSpace * 3,
				FSType: "NFS",
			},
			err: nil,
		},
		// Test 2 - when fstype is xfs and total inodes are less than 10k.
		{
			diskInfo: disk.Info{
				Total:  diskMinTotalSpace * 3,
				FSType: "XFS",
				Files:  9999,
			},
			err: nil,
		},
		// Test 3 - when fstype is btrfs and total inodes is empty.
		{
			diskInfo: disk.Info{
				Total:  diskMinTotalSpace * 3,
				FSType: "BTRFS",
				Files:  0,
			},
			err: nil,
		},
		// Test 4 - when fstype is xfs and total disk space is really small.
		{
			diskInfo: disk.Info{
				Total:  diskMinTotalSpace - diskMinTotalSpace/1024,
				FSType: "XFS",
				Files:  9999,
			},
			err: errDiskFull,
		},
	}

	// Validate all cases.
	for i, test := range testCases {
		if err := checkDiskMinTotal(test.diskInfo); test.err != err {
			t.Errorf("Test %d: Expected error %s, got %s", i+1, test.err, err)
		}
	}
}

// Checks for restrictions for min free disk space and inodes.
func TestCheckDiskFreeMin(t *testing.T) {
	testCases := []struct {
		diskInfo disk.Info
		err      error
	}{
		// Test 1 - when fstype is nfs.
		{
			diskInfo: disk.Info{
				Free:   diskMinTotalSpace * 3,
				FSType: "NFS",
			},
			err: nil,
		},
		// Test 2 - when fstype is xfs and total inodes are less than 10k.
		{
			diskInfo: disk.Info{
				Free:   diskMinTotalSpace * 3,
				FSType: "XFS",
				Files:  9999,
				Ffree:  9999,
			},
			err: nil,
		},
		// Test 3 - when fstype is btrfs and total inodes are empty.
		{
			diskInfo: disk.Info{
				Free:   diskMinTotalSpace * 3,
				FSType: "BTRFS",
				Files:  0,
			},
			err: nil,
		},
		// Test 4 - when fstype is xfs and total disk space is really small.
		{
			diskInfo: disk.Info{
				Free:   diskMinTotalSpace - diskMinTotalSpace/1024,
				FSType: "XFS",
				Files:  9999,
			},
			err: errDiskFull,
		},
	}

	// Validate all cases.
	for i, test := range testCases {
		if err := checkDiskMinFree(test.diskInfo); test.err != err {
			t.Errorf("Test %d: Expected error %s, got %s", i+1, test.err, err)
		}
	}
}
