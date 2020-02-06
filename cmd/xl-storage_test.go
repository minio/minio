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
	"crypto/rand"
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

func TestCheckPathLength(t *testing.T) {
	// Check path length restrictions are not same on windows/darwin
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip()
	}

	testCases := []struct {
		path        string
		expectedErr error
	}{
		{".", errFileAccessDenied},
		{"/", errFileAccessDenied},
		{"..", errFileAccessDenied},
		{"data/G_792/srv-tse/c/users/denis/documents/gestion!20locative/heritier/propri!E9taire/20190101_a2.03!20-!20m.!20heritier!20re!B4mi!20-!20proce!60s-verbal!20de!20livraison!20et!20de!20remise!20des!20cle!B4s!20acque!B4reurs!20-!204-!20livraison!20-!20lp!20promotion!20toulouse!20-!20encre!20et!20plume!20-!205!20de!B4c.!202019!20a!60!2012-49.pdf.ecc", errFileNameTooLong},
		{"data/G_792/srv-tse/c/users/denis/documents/gestionlocative.txt", nil},
	}

	for _, testCase := range testCases {
		gotErr := checkPathLength(testCase.path)
		t.Run("", func(t *testing.T) {
			if gotErr != testCase.expectedErr {
				t.Errorf("Expected %s, got %s", testCase.expectedErr, gotErr)
			}
		})
	}
}

// Tests validate volume name.
func TestIsValidVolname(t *testing.T) {
	testCases := []struct {
		volName    string
		shouldPass bool
	}{
		// Cases which should pass the test.
		// passing in valid bucket names.
		{"lol", true},
		{"1-this-is-valid", true},
		{"1-this-too-is-valid-1", true},
		{"this.works.too.1", true},
		{"1234567", true},
		{"123", true},
		{"s3-eu-west-1.amazonaws.com", true},
		{"ideas-are-more-powerful-than-guns", true},
		{"testbucket", true},
		{"1bucket", true},
		{"bucket1", true},
		{"$this-is-not-valid-too", true},
		{"contains-$-dollar", true},
		{"contains-^-carrot", true},
		{"contains-$-dollar", true},
		{"contains-$-dollar", true},
		{".starts-with-a-dot", true},
		{"ends-with-a-dot.", true},
		{"ends-with-a-dash-", true},
		{"-starts-with-a-dash", true},
		{"THIS-BEINGS-WITH-UPPERCASe", true},
		{"tHIS-ENDS-WITH-UPPERCASE", true},
		{"ThisBeginsAndEndsWithUpperCase", true},
		{"una ñina", true},
		{"lalalallalallalalalallalallalala-theString-size-is-greater-than-64", true},
		// cases for which test should fail.
		// passing invalid bucket names.
		{"", false},
		{SlashSeparator, false},
		{"a", false},
		{"ab", false},
		{"ab/", true},
		{"......", true},
	}

	for i, testCase := range testCases {
		isValidVolname := isValidVolname(testCase.volName)
		if testCase.shouldPass && !isValidVolname {
			t.Errorf("Test case %d: Expected \"%s\" to be a valid bucket name", i+1, testCase.volName)
		}
		if !testCase.shouldPass && isValidVolname {
			t.Errorf("Test case %d: Expected bucket name \"%s\" to be invalid", i+1, testCase.volName)
		}
	}
}

// creates a temp dir and sets up xlStorage layer.
// returns xlStorage layer, temp dir path to be used for the purpose of tests.
func newXLStorageTestSetup() (*xlStorageDiskIDCheck, string, error) {
	diskPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		return nil, "", err
	}

	// Initialize a new xlStorage layer.
	storage, err := newXLStorage(diskPath, "")
	if err != nil {
		return nil, "", err
	}
	err = storage.MakeVol(minioMetaBucket)
	if err != nil {
		return nil, "", err
	}
	// Create a sample format.json file
	err = storage.WriteAll(minioMetaBucket, formatConfigFile, bytes.NewBufferString(`{"version":"1","format":"xl","id":"592a41c2-b7cc-4130-b883-c4b5cb15965b","xl":{"version":"3","this":"da017d62-70e3-45f1-8a1a-587707e69ad1","sets":[["e07285a6-8c73-4962-89c6-047fb939f803","33b8d431-482d-4376-b63c-626d229f0a29","cff6513a-4439-4dc1-bcaa-56c9e880c352","da017d62-70e3-45f1-8a1a-587707e69ad1","9c9f21d5-1f15-4737-bce6-835faa0d9626","0a59b346-1424-4fc2-9fa2-a2e80541d0c1","7924a3dc-b69a-4971-9a2e-014966d6aebb","4d2b8dd9-4e48-444b-bdca-c89194b26042"]],"distributionAlgo":"CRCMOD"}}`))
	if err != nil {
		return nil, "", err
	}
	return &xlStorageDiskIDCheck{storage: storage, diskID: "da017d62-70e3-45f1-8a1a-587707e69ad1"}, diskPath, nil
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

// TestXLStorages xlStorage.getDiskInfo()
func TestXLStorageGetDiskInfo(t *testing.T) {
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

func TestXLStorageIsDirEmpty(t *testing.T) {
	tmp, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	// Should give false on non-existent directory.
	dir1 := slashpath.Join(tmp, "non-existent-directory")
	if isDirEmpty(dir1) {
		t.Error("expected false for non-existent directory, got true")
	}

	// Should give false for not-a-directory.
	dir2 := slashpath.Join(tmp, "file")
	err = ioutil.WriteFile(dir2, []byte("hello"), 0777)
	if err != nil {
		t.Fatal(err)
	}

	if isDirEmpty(dir2) {
		t.Error("expected false for a file, got true")
	}

	// Should give true for a real empty directory.
	dir3 := slashpath.Join(tmp, "empty")
	err = os.Mkdir(dir3, 0777)
	if err != nil {
		t.Fatal(err)
	}

	if !isDirEmpty(dir3) {
		t.Error("expected true for empty dir, got false")
	}
}

// TestXLStorageReadAll - TestXLStorages the functionality implemented by xlStorage ReadAll storage API.
func TestXLStorageReadAll(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	defer os.RemoveAll(path)

	// Create files for the test cases.
	if err = xlStorage.MakeVol("exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = xlStorage.AppendFile("exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = xlStorage.AppendFile("exists", "as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}
	if err = xlStorage.AppendFile("exists", "as-file-parent", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
	}

	// TestXLStoragecases to validate different conditions for ReadAll API.
	testCases := []struct {
		volume string
		path   string
		err    error
	}{
		// TestXLStorage case - 1.
		// Validate volume does not exist.
		{
			volume: "i-dont-exist",
			path:   "",
			err:    errVolumeNotFound,
		},
		// TestXLStorage case - 2.
		// Validate bad condition file does not exist.
		{
			volume: "exists",
			path:   "as-file-not-found",
			err:    errFileNotFound,
		},
		// TestXLStorage case - 3.
		// Validate bad condition file exists as prefix/directory and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "as-directory",
			err:    errFileNotFound,
		},
		// TestXLStorage case - 4.
		{
			volume: "exists",
			path:   "as-file-parent/as-file",
			err:    errFileNotFound,
		},
		// TestXLStorage case - 5.
		// Validate the good condition file exists and we are able to read it.
		{
			volume: "exists",
			path:   "as-file",
			err:    nil,
		},
		// TestXLStorage case - 6.
		// TestXLStorage case with invalid volume name.
		{
			volume: "ab",
			path:   "as-file",
			err:    errVolumeNotFound,
		},
	}

	var dataRead []byte
	// Run through all the test cases and validate for ReadAll.
	for i, testCase := range testCases {
		dataRead, err = xlStorage.ReadAll(testCase.volume, testCase.path)
		if err != testCase.err {
			t.Fatalf("TestXLStorage %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
		}
		if err == nil {
			if string(dataRead) != string([]byte("Hello, World")) {
				t.Errorf("TestXLStorage %d: Expected the data read to be \"%s\", but instead got \"%s\"", i+1, "Hello, World", string(dataRead))
			}
		}
	}
}

// TestNewXLStorage all the cases handled in xlStorage storage layer initialization.
func TestNewXLStorage(t *testing.T) {
	// Temporary dir name.
	tmpDirName := globalTestTmpDir + SlashSeparator + "minio-" + nextSuffix()
	// Temporary file name.
	tmpFileName := globalTestTmpDir + SlashSeparator + "minio-" + nextSuffix()
	f, _ := os.Create(tmpFileName)
	f.Close()
	defer os.Remove(tmpFileName)

	// List of all tests for xlStorage initialization.
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
			errDiskNotDir,
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		// Initialize a new xlStorage layer.
		_, err := newXLStorage(testCase.name, "")
		if err != testCase.err {
			t.Fatalf("TestXLStorage %d failed wanted: %s, got: %s", i+1, err, testCase.err)
		}
	}
}

// TestXLStorageMakeVol - TestXLStorage validate the logic for creation of new xlStorage volume.
// Asserts the failures too against the expected failures.
func TestXLStorageMakeVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
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
		expectedErr error
	}{
		// TestXLStorage case - 1.
		// A valid case, volume creation is expected to succeed.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		// Case where a file exists by the name of the volume to be created.
		{
			volName:     "vol-as-file",
			expectedErr: errVolumeExists,
		},
		// TestXLStorage case - 3.
		{
			volName:     "existing-vol",
			expectedErr: errVolumeExists,
		},
		// TestXLStorage case - 5.
		// TestXLStorage case with invalid volume name.
		{
			volName:     "ab",
			expectedErr: errInvalidArgument,
		},
	}

	for i, testCase := range testCases {
		if err := xlStorage.MakeVol(testCase.volName); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
		if err != nil {
			t.Fatalf("Unable to create temporary directory. %v", err)
		}
		defer os.RemoveAll(permDeniedDir)
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err := xlStorageNew.MakeVol("test-vol"); err != errDiskAccessDenied {
			t.Fatalf("expected: %s, got: %s", errDiskAccessDenied, err)
		}
	}
}

// TestXLStorageDeleteVol - Validates the expected behavior of xlStorage.DeleteVol for various cases.
func TestXLStorageDeleteVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// TestXLStorage failure cases.
	vol := slashpath.Join(path, "nonempty-vol")
	if err = os.Mkdir(vol, 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}
	if err = ioutil.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		// TestXLStorage case  - 1.
		// A valida case. Empty vol, should be possible to delete.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		// volume is non-existent.
		{
			volName:     "nonexistent-vol",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 3.
		// It shouldn't be possible to delete an non-empty volume, validating the same.
		{
			volName:     "nonempty-vol",
			expectedErr: errVolumeNotEmpty,
		},
		// TestXLStorage case - 5.
		// Invalid volume name.
		{
			volName:     "ab",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		if err = xlStorage.DeleteVol(testCase.volName, false); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
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

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err = xlStorageNew.DeleteVol("mybucket", false); err != errDiskAccessDenied {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.DeleteVol("Del-Vol", false)
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestXLStorageStatVol - TestXLStorages validate the volume info returned by xlStorage.StatVol() for various inputs.
func TestXLStorageStatVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		// TestXLStorage case - 1.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		{
			volName:     "nonexistent-vol",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 3.
		{
			volName:     "ab",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		var volInfo VolInfo
		volInfo, err = xlStorage.StatVol(testCase.volName)
		if err != testCase.expectedErr {
			t.Fatalf("TestXLStorage case : %d, Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}

		if err == nil {
			if volInfo.Name != testCase.volName {
				t.Errorf("TestXLStorage case %d: Expected the volume name to be \"%s\", instead found \"%s\"",
					i+1, volInfo.Name, testCase.volName)
			}
		}
	}

	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	_, err = xlStorageDeletedStorage.StatVol("Stat vol")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestXLStorageListVols - Validates the result and the error output for xlStorage volume listing functionality xlStorage.ListVols().
func TestXLStorageListVols(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	var volInfos []VolInfo
	// TestXLStorage empty list vols.
	if volInfos, err = xlStorage.ListVols(); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfos) != 1 {
		t.Fatalf("expected: one entry, got: %s", volInfos)
	}

	// TestXLStorage non-empty list vols.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	volInfos, err = xlStorage.ListVols()
	if err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	}
	if len(volInfos) != 2 {
		t.Fatalf("expected: 2, got: %d", len(volInfos))
	}
	volFound := false
	for _, info := range volInfos {
		if info.Name == "success-vol" {
			volFound = true
			break
		}
	}
	if !volFound {
		t.Errorf("expected: success-vol to be created")
	}

	// removing the path and simulating disk failure
	os.RemoveAll(path)
	// should fail with errDiskNotFound.
	if _, err = xlStorage.ListVols(); err != errDiskNotFound {
		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errDiskNotFound, err)
	}
}

// TestXLStorageXlStorageListDir -  TestXLStorages validate the directory listing functionality provided by xlStorage.ListDir .
func TestXLStorageXlStorageListDir(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create xlStorage test setup.
	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = xlStorage.AppendFile("success-vol", "abc/def/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err = xlStorage.AppendFile("success-vol", "abc/xyz/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol  string
		srcPath string
		// expected result.
		expectedListDir []string
		expectedErr     error
	}{
		// TestXLStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc",
			expectedListDir: []string{"def/", "xyz/"},
			expectedErr:     nil,
		},
		// TestXLStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def",
			expectedListDir: []string{"ghi/"},
			expectedErr:     nil,
		},
		// TestXLStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def/ghi",
			expectedListDir: []string{"success-file"},
			expectedErr:     nil,
		},
		// TestXLStorage case - 2.
		{
			srcVol:      "success-vol",
			srcPath:     "abcdef",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 3.
		// TestXLStorage case with invalid volume name.
		{
			srcVol:      "ab",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 4.
		// TestXLStorage case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		var dirList []string
		dirList, err = xlStorage.ListDir(testCase.srcVol, testCase.srcPath, -1)
		if err != testCase.expectedErr {
			t.Fatalf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
		if err == nil {
			for _, expected := range testCase.expectedListDir {
				if !strings.Contains(strings.Join(dirList, ","), expected) {
					t.Errorf("TestXLStorage case %d: Expected the directory listing to be \"%v\", but got \"%v\"", i+1, testCase.expectedListDir, dirList)
				}
			}
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStorageNew.DeleteFile("mybucket", "myobject"); err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.DeleteFile("del-vol", "my-file")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestXLStorageDeleteFile - Series of test cases construct valid and invalid input data and validates the result and the error response.
func TestXLStorageDeleteFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create xlStorage test setup
	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = xlStorage.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err = xlStorage.MakeVol("no-permissions"); err != nil {
		t.Fatalf("Unable to create volume, %s", err.Error())
	}
	if err = xlStorage.AppendFile("no-permissions", "dir/file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err.Error())
	}
	// Parent directory must have write permissions, this is read + execute.
	if err = os.Chmod(pathJoin(path, "no-permissions"), 0555); err != nil {
		t.Fatalf("Unable to chmod directory, %s", err.Error())
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		expectedErr error
	}{
		// TestXLStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		// The file was deleted in the last  case, so DeleteFile should fail.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 3.
		// TestXLStorage case with segment of the volume name > 255.
		{
			srcVol:      "my",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 4.
		// TestXLStorage case with non-existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 5.
		// TestXLStorage case with src path segment > 255.
		{
			srcVol:      "success-vol",
			srcPath:     "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			expectedErr: errFileNameTooLong,
		},
		// TestXLStorage case - 6.
		// TestXLStorage case with undeletable parent directory.
		// File can delete, dir cannot delete because no-permissions doesn't have write perms.
		{
			srcVol:      "no-permissions",
			srcPath:     "dir/file",
			expectedErr: nil,
		},
	}

	for i, testCase := range testCases {
		if err = xlStorage.DeleteFile(testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
			t.Errorf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStorageNew.DeleteFile("mybucket", "myobject"); err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.DeleteFile("del-vol", "my-file")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
	}
}

// TestXLStorageReadFile - TestXLStorages xlStorage.ReadFile with wide range of cases and asserts the result and error response.
func TestXLStorageReadFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	volume := "success-vol"
	// Setup test environment.
	if err = xlStorage.MakeVol(volume); err != nil {
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
		// Object is a directory. - 3
		{
			volume, "object-as-dir",
			0, 5, nil, errIsNotRegular},
		// One path segment length is > 255 chars long. - 4
		{
			volume, "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Path length is > 1024 chars long. - 5
		{
			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Buffer size greater than object size. - 6
		{
			volume, "myobject", 0, 16,
			[]byte("hello, world"),
			io.ErrUnexpectedEOF,
		},
		// Reading from an offset success. - 7
		{
			volume, "myobject", 7, 5,
			[]byte("world"), nil,
		},
		// Reading from an object but buffer size greater. - 8
		{
			volume, "myobject",
			7, 8,
			[]byte("world"),
			io.ErrUnexpectedEOF,
		},
		// Seeking ahead returns io.EOF. - 9
		{
			volume, "myobject", 14, 1, nil, io.EOF,
		},
		// Empty volume name. - 10
		{
			"", "myobject", 14, 1, nil, errVolumeNotFound,
		},
		// Empty filename name. - 11
		{
			volume, "", 14, 1, nil, errIsNotRegular,
		},
		// Non existent volume name - 12
		{
			"abcd", "", 14, 1, nil, errVolumeNotFound,
		},
		// Non existent filename - 13
		{
			volume, "abcd", 14, 1, nil, errFileNotFound,
		},
	}

	// Create all files needed during testing.
	appendFiles := testCases[:4]
	v := NewBitrotVerifier(SHA256, getSHA256Sum([]byte("hello, world")))
	// Create test files for further reading.
	for i, appendFile := range appendFiles {
		err = xlStorage.AppendFile(volume, appendFile.fileName, []byte("hello, world"))
		if err != appendFile.expectedErr {
			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
		}
	}

	{
		buf := make([]byte, 5)
		// Test for negative offset.
		if _, err = xlStorage.ReadFile(volume, "myobject", -1, buf, v); err == nil {
			t.Fatalf("expected: error, got: <nil>")
		}
	}

	// Following block validates all ReadFile test cases.
	for i, testCase := range testCases {
		var n int64
		// Common read buffer.
		var buf = make([]byte, testCase.bufSize)
		n, err = xlStorage.ReadFile(testCase.volume, testCase.fileName, testCase.offset, buf, v)
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
					t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:n]))
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

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStoragePermStorage, err := newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// Common read buffer.
		var buf = make([]byte, 10)
		if _, err = xlStoragePermStorage.ReadFile("mybucket", "myobject", 0, buf, v); err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}
}

var xlStorageReadFileWithVerifyTests = []struct {
	file      string
	offset    int
	length    int
	algorithm BitrotAlgorithm
	expError  error
}{
	{file: "myobject", offset: 0, length: 100, algorithm: SHA256, expError: nil},                // 0
	{file: "myobject", offset: 25, length: 74, algorithm: SHA256, expError: nil},                // 1
	{file: "myobject", offset: 29, length: 70, algorithm: SHA256, expError: nil},                // 2
	{file: "myobject", offset: 100, length: 0, algorithm: SHA256, expError: nil},                // 3
	{file: "myobject", offset: 1, length: 120, algorithm: SHA256, expError: errFileCorrupt},     // 4
	{file: "myobject", offset: 3, length: 1100, algorithm: SHA256, expError: nil},               // 5
	{file: "myobject", offset: 2, length: 100, algorithm: SHA256, expError: errFileCorrupt},     // 6
	{file: "myobject", offset: 1000, length: 1001, algorithm: SHA256, expError: nil},            // 7
	{file: "myobject", offset: 0, length: 100, algorithm: BLAKE2b512, expError: errFileCorrupt}, // 8
	{file: "myobject", offset: 25, length: 74, algorithm: BLAKE2b512, expError: nil},            // 9
	{file: "myobject", offset: 29, length: 70, algorithm: BLAKE2b512, expError: errFileCorrupt}, // 10
	{file: "myobject", offset: 100, length: 0, algorithm: BLAKE2b512, expError: nil},            // 11
	{file: "myobject", offset: 1, length: 120, algorithm: BLAKE2b512, expError: nil},            // 12
	{file: "myobject", offset: 3, length: 1100, algorithm: BLAKE2b512, expError: nil},           // 13
	{file: "myobject", offset: 2, length: 100, algorithm: BLAKE2b512, expError: nil},            // 14
	{file: "myobject", offset: 1000, length: 1001, algorithm: BLAKE2b512, expError: nil},        // 15
}

// TestXLStorageReadFile with bitrot verification - tests the xlStorage level
// ReadFile API with a BitrotVerifier. Only tests hashing related
// functionality. Other functionality is tested with
// TestXLStorageReadFile.
func TestXLStorageReadFileWithVerify(t *testing.T) {
	volume, object := "test-vol", "myobject"
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	if err = xlStorage.MakeVol(volume); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create volume %s: %v", volume, err)
	}
	data := make([]byte, 8*1024)
	if _, err = io.ReadFull(rand.Reader, data); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create generate random data: %v", err)
	}
	if err = xlStorage.AppendFile(volume, object, data); err != nil {
		os.RemoveAll(path)
		t.Fatalf("Unable to create object: %v", err)
	}

	for i, test := range xlStorageReadFileWithVerifyTests {
		h := test.algorithm.New()
		h.Write(data)
		if test.expError != nil {
			h.Write([]byte{0})
		}

		buffer := make([]byte, test.length)
		n, err := xlStorage.ReadFile(volume, test.file, int64(test.offset), buffer, NewBitrotVerifier(test.algorithm, h.Sum(nil)))

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

// TestXLStorageFormatFileChange - to test if changing the diskID makes the calls fail.
func TestXLStorageFormatFileChange(t *testing.T) {
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	if err = xlStorage.MakeVol(volume); err != nil {
		t.Fatalf("MakeVol failed with %s", err)
	}

	// Change the format.json such that "this" is changed to "randomid".
	if err = ioutil.WriteFile(pathJoin(xlStorage.String(), minioMetaBucket, formatConfigFile), []byte(`{"version":"1","format":"xl","id":"592a41c2-b7cc-4130-b883-c4b5cb15965b","xl":{"version":"3","this":"randomid","sets":[["e07285a6-8c73-4962-89c6-047fb939f803","33b8d431-482d-4376-b63c-626d229f0a29","cff6513a-4439-4dc1-bcaa-56c9e880c352","randomid","9c9f21d5-1f15-4737-bce6-835faa0d9626","0a59b346-1424-4fc2-9fa2-a2e80541d0c1","7924a3dc-b69a-4971-9a2e-014966d6aebb","4d2b8dd9-4e48-444b-bdca-c89194b26042"]],"distributionAlgo":"CRCMOD"}}`), 0644); err != nil {
		t.Fatalf("ioutil.WriteFile failed with %s", err)
	}

	err = xlStorage.MakeVol(volume)
	if err != errVolumeExists {
		t.Fatalf("MakeVol expected to fail with errDiskNotFound but failed with %s", err)
	}
}

// TestXLStorage xlStorage.AppendFile()
func TestXLStorageAppendFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = xlStorage.MakeVol("success-vol"); err != nil {
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
		// TestXLStorage to append to previously created file.
		{"myobject", nil},
		// TestXLStorage to use same path of previously created file.
		{"path/to/my/testobject", nil},
		// TestXLStorage to use object is a directory now.
		{"object-as-dir", errIsNotRegular},
		// path segment uses previously uploaded object.
		{"myobject/testobject", errFileAccessDenied},
		// One path segment length is > 255 chars long.
		{"path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
		// path length is > 1024 chars long.
		{"level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
	}

	for i, testCase := range testCases {
		if err = xlStorage.AppendFile("success-vol", testCase.fileName, []byte("hello, world")); err != testCase.expectedErr {
			t.Errorf("Case: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		var xlStoragePermStorage StorageAPI
		// Initialize xlStorage storage layer for permission denied error.
		_, err = newXLStorage(permDeniedDir, "")
		if err != nil && !os.IsPermission(err) {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStoragePermStorage, err = newXLStorage(permDeniedDir, "")
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStoragePermStorage.AppendFile("mybucket", "myobject", []byte("hello, world")); err != errFileAccessDenied {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	// TestXLStorage case with invalid volume name.
	// A valid volume name should be atleast of size 3.
	err = xlStorage.AppendFile("bn", "yes", []byte("hello, world"))
	if err != errVolumeNotFound {
		t.Fatalf("expected: \"Invalid argument error\", got: \"%s\"", err)
	}
}

// TestXLStorage xlStorage.RenameFile()
func TestXLStorageRenameFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := xlStorage.MakeVol("src-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.MakeVol("dest-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.AppendFile("src-vol", "file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile("src-vol", "file2", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile("src-vol", "file3", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile("src-vol", "file4", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile("src-vol", "file5", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile("src-vol", "path/to/file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol      string
		destVol     string
		srcPath     string
		destPath    string
		expectedErr error
	}{
		// TestXLStorage case - 1.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file1",
			destPath:    "file-one",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			expectedErr: nil,
		},
		// TestXLStorage case - 3.
		// TestXLStorage to overwrite destination file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file2",
			destPath:    "file-one",
			expectedErr: nil,
		},
		// TestXLStorage case - 4.
		// TestXLStorage case with io error count set to 1.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file3",
			destPath:    "file-two",
			expectedErr: nil,
		},
		// TestXLStorage case - 5.
		// TestXLStorage case with io error count set to maximum allowed count.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "file-three",
			expectedErr: nil,
		},
		// TestXLStorage case - 6.
		// TestXLStorage case with non-existent source file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "non-existent-file",
			destPath:    "file-three",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 7.
		// TestXLStorage to check failure of source and destination are not same type.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "file-one",
			expectedErr: errFileAccessDenied,
		},
		// TestXLStorage case - 8.
		// TestXLStorage to check failure of destination directory exists.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			expectedErr: errFileAccessDenied,
		},
		// TestXLStorage case - 9.
		// TestXLStorage case with source being a file and destination being a directory.
		// Either both have to be files or directories.
		// Expecting to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errFileAccessDenied,
		},
		// TestXLStorage case - 10.
		// TestXLStorage case with non-existent source volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol-non-existent",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 11.
		// TestXLStorage case with non-existent destination volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 12.
		// TestXLStorage case with invalid src volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "ab",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 13.
		// TestXLStorage case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 14.
		// TestXLStorage case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 15.
		// TestXLStorage case with the parent of the destination being a file.
		// expected to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file5",
			destPath:    "file-one/parent-is-file",
			expectedErr: errFileAccessDenied,
		},
		// TestXLStorage case - 16.
		// TestXLStorage case with segment of source file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			destPath:    "file-six",
			expectedErr: errFileNameTooLong,
		},
		// TestXLStorage case - 17.
		// TestXLStorage case with segment of destination file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file6",
			destPath:    "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			expectedErr: errFileNameTooLong,
		},
	}

	for i, testCase := range testCases {
		if err := xlStorage.RenameFile(testCase.srcVol, testCase.srcPath, testCase.destVol, testCase.destPath); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage %d:  Expected the error to be : \"%v\", got: \"%v\".", i+1, testCase.expectedErr, err)
		}
	}
}

// TestXLStorage xlStorage.CheckFile()
func TestXLStorageCheckFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := xlStorage.MakeVol("success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.AppendFile("success-vol", pathJoin("success-file", xlStorageFormatFile), []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile("success-vol", pathJoin("path/to/success-file", xlStorageFormatFile), []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		expectedErr error
	}{
		// TestXLStorage case - 1.
		// TestXLStorage case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: nil,
		},
		// TestXLStorage case - 2.
		// TestXLStorage case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "path/to/success-file",
			expectedErr: nil,
		},
		// TestXLStorage case - 3.
		// TestXLStorage case with non-existent file.
		{
			srcVol:      "success-vol",
			srcPath:     "nonexistent-file",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 4.
		// TestXLStorage case with non-existent file path.
		{
			srcVol:      "success-vol",
			srcPath:     "path/2/success-file",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 5.
		// TestXLStorage case with path being a directory.
		{
			srcVol:      "success-vol",
			srcPath:     "path",
			expectedErr: errFileNotFound,
		},
		// TestXLStorage case - 6.
		// TestXLStorage case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		if err := xlStorage.CheckFile(testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}
}

// Test xlStorage.VerifyFile()
func TestXLStorageVerifyFile(t *testing.T) {
	// We test 4 cases:
	// 1) Whole-file bitrot check on proper file
	// 2) Whole-file bitrot check on corrupted file
	// 3) Streaming bitrot check on proper file
	// 4) Streaming bitrot check on corrupted file

	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	volName := "testvol"
	fileName := "testfile"
	if err := xlStorage.MakeVol(volName); err != nil {
		t.Fatal(err)
	}

	// 1) Whole-file bitrot check on proper file
	size := int64(4*1024*1024 + 100*1024) // 4.1 MB
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	algo := HighwayHash256
	h := algo.New()
	h.Write(data)
	hashBytes := h.Sum(nil)
	if err := xlStorage.WriteAll(volName, fileName, bytes.NewBuffer(data)); err != nil {
		t.Fatal(err)
	}
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size, algo, hashBytes, 0); err != nil {
		t.Fatal(err)
	}

	// 2) Whole-file bitrot check on corrupted file
	if err := xlStorage.AppendFile(volName, fileName, []byte("a")); err != nil {
		t.Fatal(err)
	}

	// Check if VerifyFile reports the incorrect file length (the correct length is `size+1`)
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size, algo, hashBytes, 0); err == nil {
		t.Fatal("expected to fail bitrot check")
	}

	// Check if bitrot fails
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size+1, algo, hashBytes, 0); err == nil {
		t.Fatal("expected to fail bitrot check")
	}

	if err := xlStorage.DeleteFile(volName, fileName); err != nil {
		t.Fatal(err)
	}

	// 3) Streaming bitrot check on proper file
	algo = HighwayHash256S
	shardSize := int64(1024 * 1024)
	shard := make([]byte, shardSize)
	w := newStreamingBitrotWriter(xlStorage, volName, fileName, size, algo, shardSize)
	reader := bytes.NewReader(data)
	for {
		// Using io.CopyBuffer instead of this loop will not work for us as io.CopyBuffer
		// will use bytes.Buffer.ReadFrom() which will not do shardSize'ed writes causing error.
		n, err := reader.Read(shard)
		w.Write(shard[:n])
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		t.Fatal(err)
	}
	w.Close()
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size, algo, nil, shardSize); err != nil {
		t.Fatal(err)
	}

	// 4) Streaming bitrot check on corrupted file
	filePath := pathJoin(xlStorage.String(), volName, fileName)
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("a"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size, algo, nil, shardSize); err == nil {
		t.Fatal("expected to fail bitrot check")
	}
	if err := xlStorage.storage.bitrotVerify(pathJoin(path, volName, fileName), size+1, algo, nil, shardSize); err == nil {
		t.Fatal("expected to fail bitrot check")
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
			err: errMinDiskSize,
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
