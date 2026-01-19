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
	"bytes"
	"crypto/rand"
	"io"
	"net/url"
	"os"
	slashpath "path"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
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

func newLocalXLStorage(path string) (*xlStorage, error) {
	return newLocalXLStorageWithDiskIdx(path, 0)
}

// Initialize a new storage disk.
func newLocalXLStorageWithDiskIdx(path string, diskIdx int) (*xlStorage, error) {
	u := url.URL{Path: path}
	return newXLStorage(Endpoint{
		URL:     &u,
		IsLocal: true,
		PoolIdx: 0,
		SetIdx:  0,
		DiskIdx: diskIdx,
	}, true)
}

// creates a temp dir and sets up xlStorage layer.
// returns xlStorage layer, temp dir path to be used for the purpose of tests.
func newXLStorageTestSetup(tb testing.TB) (*xlStorageDiskIDCheck, string, error) {
	diskPath := tb.TempDir()

	// Initialize a new xlStorage layer.
	storage, err := newLocalXLStorageWithDiskIdx(diskPath, 3)
	if err != nil {
		return nil, "", err
	}

	// Create a sample format.json file
	if err = storage.WriteAll(tb.Context(), minioMetaBucket, formatConfigFile, []byte(`{"version":"1","format":"xl","id":"592a41c2-b7cc-4130-b883-c4b5cb15965b","xl":{"version":"3","this":"da017d62-70e3-45f1-8a1a-587707e69ad1","sets":[["e07285a6-8c73-4962-89c6-047fb939f803","33b8d431-482d-4376-b63c-626d229f0a29","cff6513a-4439-4dc1-bcaa-56c9e880c352","da017d62-70e3-45f1-8a1a-587707e69ad1","9c9f21d5-1f15-4737-bce6-835faa0d9626","0a59b346-1424-4fc2-9fa2-a2e80541d0c1","7924a3dc-b69a-4971-9a2e-014966d6aebb","4d2b8dd9-4e48-444b-bdca-c89194b26042"]],"distributionAlgo":"CRCMOD"}}`)); err != nil {
		return nil, "", err
	}

	disk := newXLStorageDiskIDCheck(storage, false)
	disk.SetDiskID("da017d62-70e3-45f1-8a1a-587707e69ad1")
	return disk, diskPath, nil
}

// createPermDeniedFile - creates temporary directory and file with path '/mybucket/myobject'
func createPermDeniedFile(t *testing.T) (permDeniedDir string) {
	var err error
	permDeniedDir = t.TempDir()

	if err = os.Mkdir(slashpath.Join(permDeniedDir, "mybucket"), 0o775); err != nil {
		t.Fatalf("Unable to create temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
	}

	if err = os.WriteFile(slashpath.Join(permDeniedDir, "mybucket", "myobject"), []byte(""), 0o400); err != nil {
		t.Fatalf("Unable to create file %v. %v", slashpath.Join(permDeniedDir, "mybucket", "myobject"), err)
	}

	if err = os.Chmod(slashpath.Join(permDeniedDir, "mybucket"), 0o400); err != nil {
		t.Fatalf("Unable to change permission to temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
	}
	t.Cleanup(func() {
		os.Chmod(slashpath.Join(permDeniedDir, "mybucket"), 0o775)
	})

	if err = os.Chmod(permDeniedDir, 0o400); err != nil {
		t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
	}
	t.Cleanup(func() {
		os.Chmod(permDeniedDir, 0o775)
	})

	return permDeniedDir
}

// TestXLStorages xlStorage.getDiskInfo()
func TestXLStorageGetDiskInfo(t *testing.T) {
	path := t.TempDir()

	testCases := []struct {
		diskPath    string
		expectedErr error
	}{
		{path, nil},
		{"/nonexistent-dir", errDiskNotFound},
	}

	// Check test cases.
	for _, testCase := range testCases {
		if _, _, err := getDiskInfo(testCase.diskPath); err != testCase.expectedErr {
			t.Fatalf("expected: %s, got: %s", testCase.expectedErr, err)
		}
	}
}

func TestXLStorageIsDirEmpty(t *testing.T) {
	tmp := t.TempDir()

	// Should give false on non-existent directory.
	dir1 := slashpath.Join(tmp, "non-existent-directory")
	if isDirEmpty(dir1, true) {
		t.Error("expected false for non-existent directory, got true")
	}

	// Should give false for not-a-directory.
	dir2 := slashpath.Join(tmp, "file")
	err := os.WriteFile(dir2, []byte("hello"), 0o777)
	if err != nil {
		t.Fatal(err)
	}

	if isDirEmpty(dir2, true) {
		t.Error("expected false for a file, got true")
	}

	// Should give true for a real empty directory.
	dir3 := slashpath.Join(tmp, "empty")
	err = os.Mkdir(dir3, 0o777)
	if err != nil {
		t.Fatal(err)
	}

	if !isDirEmpty(dir3, true) {
		t.Error("expected true for empty dir, got false")
	}
}

func TestXLStorageReadVersionLegacy(t *testing.T) {
	const legacyJSON = `{"version":"1.0.1","format":"xl","stat":{"size":2016,"modTime":"2021-10-11T23:40:34.914361617Z"},"erasure":{"algorithm":"klauspost/reedsolomon/vandermonde","data":2,"parity":2,"blockSize":10485760,"index":2,"distribution":[2,3,4,1],"checksum":[{"name":"part.1","algorithm":"highwayhash256S"}]},"minio":{"release":"RELEASE.2019-12-30T05-45-39Z"},"meta":{"X-Minio-Internal-Server-Side-Encryption-Iv":"kInsJB/0yxyz/40ZI+lmQYJfZacDYqZsGh2wEiv+N50=","X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id":"my-minio-key","X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key":"eyJhZWFkIjoiQUVTLTI1Ni1HQ00tSE1BQy1TSEEtMjU2IiwiaWQiOiJjMzEwNDVjODFmMTA2MWU5NTI4ODcxZmNhMmRkYzA3YyIsIml2IjoiOWQ5cUxGMFhSaFBXbEVqT2JDMmo0QT09Iiwibm9uY2UiOiJYaERsemlCU1cwSENuK2RDIiwiYnl0ZXMiOiJUM0lmY1haQ1dtMWpLeWxBWmFUUnczbDVoYldLWW95dm5iNTZVaWJEbE5LOFZVU2tuQmx3NytIMG8yZnRzZ1UrIn0=","X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key":"IAAfANqt801MT+wwzQRkfFhTrndmhfNiN0alKwDS4AQ1dznNADRQgoq6I4pPVfRsbDp5rQawlripQZvPWUSNJA==","X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm":"DAREv2-HMAC-SHA256","content-type":"application/octet-stream","etag":"20000f00cf5e68d3d6b60e44fcd8b9e8-1"},"parts":[{"number":1,"name":"part.1","etag":"","size":2016,"actualSize":1984}]}`

	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to cfgreate xlStorage test setup, %s", err)
	}

	// Create files for the test cases.
	if err = xlStorage.MakeVol(t.Context(), "exists-legacy"); err != nil {
		t.Fatalf("Unable to create a volume \"exists-legacy\", %s", err)
	}

	if err = xlStorage.AppendFile(t.Context(), "exists-legacy", "as-file/xl.json", []byte(legacyJSON)); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}

	fi, err := xlStorage.ReadVersion(t.Context(), "", "exists-legacy", "as-file", "", ReadOptions{})
	if err != nil {
		t.Fatalf("Unable to read older 'xl.json' content: %s", err)
	}

	if !fi.XLV1 {
		t.Fatal("Unexpected 'xl.json' content should be correctly interpreted as legacy content")
	}
}

// TestXLStorageReadVersion - TestXLStorages the functionality implemented by xlStorage ReadVersion storage API.
func TestXLStorageReadVersion(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to cfgreate xlStorage test setup, %s", err)
	}

	xlMeta, _ := os.ReadFile("testdata/xl.meta")
	fi, _ := getFileInfo(xlMeta, "exists", "as-file", "", fileInfoOpts{Data: false})

	// Create files for the test cases.
	if err = xlStorage.MakeVol(t.Context(), "exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-directory/as-file/xl.meta", xlMeta); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-file/xl.meta", xlMeta); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-file-parent/xl.meta", xlMeta); err != nil {
		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
	}
	if err = xlStorage.MakeVol(t.Context(), "exists/as-file/"+fi.DataDir); err != nil {
		t.Fatalf("Unable to create a dataDir %s,  %s", fi.DataDir, err)
	}

	// TestXLStoragecases to validate different conditions for ReadVersion API.
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

	// Run through all the test cases and validate for ReadVersion.
	for i, testCase := range testCases {
		_, err = xlStorage.ReadVersion(t.Context(), "", testCase.volume, testCase.path, "", ReadOptions{})
		if err != testCase.err {
			t.Fatalf("TestXLStorage %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
		}
	}
}

// TestXLStorageReadAll - TestXLStorages the functionality implemented by xlStorage ReadAll storage API.
func TestXLStorageReadAll(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Create files for the test cases.
	if err = xlStorage.MakeVol(t.Context(), "exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "exists", "as-file-parent", []byte("Hello, World")); err != nil {
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
		dataRead, err = xlStorage.ReadAll(t.Context(), testCase.volume, testCase.path)
		if err != testCase.err {
			t.Errorf("TestXLStorage %d: Expected err \"%v\", got err \"%v\"", i+1, testCase.err, err)
			continue
		}
		if err == nil {
			if !bytes.Equal(dataRead, []byte("Hello, World")) {
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
		_, err := newLocalXLStorage(testCase.name)
		if err != testCase.err {
			t.Fatalf("TestXLStorage %d failed wanted: %s, got: %s", i+1, err, testCase.err)
		}
	}
}

// TestXLStorageMakeVol - TestXLStorage validate the logic for creation of new xlStorage volume.
// Asserts the failures too against the expected failures.
func TestXLStorageMakeVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	// Create a file.
	if err := os.WriteFile(slashpath.Join(path, "vol-as-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	// Create a directory.
	if err := os.Mkdir(slashpath.Join(path, "existing-vol"), 0o777); err != nil {
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
		if err := xlStorage.MakeVol(t.Context(), testCase.volName); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		if err = os.Chmod(permDeniedDir, 0o400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0o400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err := xlStorageNew.MakeVol(t.Context(), "test-vol"); err != errDiskAccessDenied {
			t.Fatalf("expected: %s, got: %s", errDiskAccessDenied, err)
		}
	}
}

// TestXLStorageDeleteVol - Validates the expected behavior of xlStorage.DeleteVol for various cases.
func TestXLStorageDeleteVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// TestXLStorage failure cases.
	vol := slashpath.Join(path, "nonempty-vol")
	if err = os.Mkdir(vol, 0o777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}
	if err = os.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
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
		if err = xlStorage.DeleteVol(t.Context(), testCase.volName, false); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := t.TempDir()
		if err = os.Mkdir(slashpath.Join(permDeniedDir, "mybucket"), 0o400); err != nil {
			t.Fatalf("Unable to create temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
		}
		t.Cleanup(func() {
			os.Chmod(slashpath.Join(permDeniedDir, "mybucket"), 0o775)
		})

		if err = os.Chmod(permDeniedDir, 0o400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}
		t.Cleanup(func() {
			os.Chmod(permDeniedDir, 0o775)
		})

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0o400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err = xlStorageNew.DeleteVol(t.Context(), "mybucket", false); err != errDiskAccessDenied {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.DeleteVol(t.Context(), "Del-Vol", false)
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Drive not found\", got \"%s\"", err)
	}
}

// TestXLStorageStatVol - TestXLStorages validate the volume info returned by xlStorage.StatVol() for various inputs.
func TestXLStorageStatVol(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
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
		volInfo, err = xlStorage.StatVol(t.Context(), testCase.volName)
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

	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	_, err = xlStorageDeletedStorage.StatVol(t.Context(), "Stat vol")
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Drive not found\", got \"%s\"", err)
	}
}

// TestXLStorageListVols - Validates the result and the error output for xlStorage volume listing functionality xlStorage.ListVols().
func TestXLStorageListVols(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	var volInfos []VolInfo
	// TestXLStorage empty list vols.
	if volInfos, err = xlStorage.ListVols(t.Context()); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfos) != 1 {
		t.Fatalf("expected: one entry, got: %v", volInfos)
	}

	// TestXLStorage non-empty list vols.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	volInfos, err = xlStorage.ListVols(t.Context())
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
	if _, err = xlStorage.ListVols(t.Context()); err != errDiskNotFound {
		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errDiskNotFound, err)
	}
}

// TestXLStorageListDir -  TestXLStorages validate the directory listing functionality provided by xlStorage.ListDir .
func TestXLStorageListDir(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// create xlStorage test setup.
	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "success-vol", "abc/def/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "success-vol", "abc/xyz/ghi/success-file", []byte("Hello, world")); err != nil {
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
		dirList, err = xlStorage.ListDir(t.Context(), "", testCase.srcVol, testCase.srcPath, -1)
		if err != testCase.expectedErr {
			t.Errorf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
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

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStorageNew.Delete(t.Context(), "mybucket", "myobject", DeleteOptions{
			Recursive: false,
			Immediate: false,
		}); err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.Delete(t.Context(), "del-vol", "my-file", DeleteOptions{
		Recursive: false,
		Immediate: false,
	})
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Drive not found\", got \"%s\"", err)
	}
}

// TestXLStorageDeleteFile - Series of test cases construct valid and invalid input data and validates the result and the error response.
func TestXLStorageDeleteFile(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		t.Skip()
	}

	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = xlStorage.AppendFile(t.Context(), "success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err = xlStorage.MakeVol(t.Context(), "no-permissions"); err != nil {
		t.Fatalf("Unable to create volume, %s", err.Error())
	}
	if err = xlStorage.AppendFile(t.Context(), "no-permissions", "dir/file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err.Error())
	}
	// Parent directory must have write permissions, this is read + execute.
	if err = os.Chmod(pathJoin(path, "no-permissions"), 0o555); err != nil {
		t.Fatalf("Unable to chmod directory, %s", err.Error())
	}
	t.Cleanup(func() {
		os.Chmod(pathJoin(path, "no-permissions"), 0o775)
	})

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
		// The file was deleted in the last  case, so Delete should not fail.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: nil,
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
	}

	for i, testCase := range testCases {
		if err = xlStorage.Delete(t.Context(), testCase.srcVol, testCase.srcPath, DeleteOptions{
			Recursive: false,
			Immediate: false,
		}); err != testCase.expectedErr {
			t.Errorf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStorageNew, err := newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStorageNew.Delete(t.Context(), "mybucket", "myobject", DeleteOptions{
			Recursive: false,
			Immediate: false,
		}); err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}

	// create xlStorage test setup
	xlStorageDeletedStorage, diskPath, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	err = os.RemoveAll(diskPath)
	if err != nil {
		t.Fatalf("Unable to remoe xlStorage diskpath, %s", err)
	}

	// TestXLStorage for delete on an removed disk.
	// should fail with disk not found.
	err = xlStorageDeletedStorage.Delete(t.Context(), "del-vol", "my-file", DeleteOptions{
		Recursive: false,
		Immediate: false,
	})
	if err != errDiskNotFound {
		t.Errorf("Expected: \"Drive not found\", got \"%s\"", err)
	}
}

// TestXLStorageReadFile - TestXLStorages xlStorage.ReadFile with wide range of cases and asserts the result and error response.
func TestXLStorageReadFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	volume := "success-vol"
	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), volume); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0o777); err != nil {
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
			0, 5, nil, errIsNotRegular,
		},
		// One path segment length is > 255 chars long. - 4
		{
			volume, "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong,
		},
		// Path length is > 1024 chars long. - 5
		{
			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong,
		},
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
		err = xlStorage.AppendFile(t.Context(), volume, appendFile.fileName, []byte("hello, world"))
		if err != appendFile.expectedErr {
			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
		}
	}

	{
		buf := make([]byte, 5)
		// Test for negative offset.
		if _, err = xlStorage.ReadFile(t.Context(), volume, "myobject", -1, buf, v); err == nil {
			t.Fatalf("expected: error, got: <nil>")
		}
	}

	for range 2 {
		// Following block validates all ReadFile test cases.
		for i, testCase := range testCases {
			var n int64
			// Common read buffer.
			buf := make([]byte, testCase.bufSize)
			n, err = xlStorage.ReadFile(t.Context(), testCase.volume, testCase.fileName, testCase.offset, buf, v)
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
						if expectErrno == 0 || resultErrno == 0 || expectErrno != resultErrno {
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
			// Expected error returned, proceed further to validate the returned results.
			if err != nil && testCase.expectedErr == nil {
				t.Errorf("Case: %d %#v, expected: %s, got :%s", i+1, testCase, testCase.expectedErr, err)
			}
			if err == nil {
				if !bytes.Equal(testCase.expectedBuf, buf) {
					t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:testCase.bufSize]))
				}
				if n != int64(testCase.bufSize) {
					t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
				}
			}
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)

		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStoragePermStorage, err := newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		// Common read buffer.
		buf := make([]byte, 10)
		if _, err = xlStoragePermStorage.ReadFile(t.Context(), "mybucket", "myobject", 0, buf, v); err != errFileAccessDenied {
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
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	if err = xlStorage.MakeVol(t.Context(), volume); err != nil {
		t.Fatalf("Unable to create volume %s: %v", volume, err)
	}
	data := make([]byte, 8*1024)
	if _, err = io.ReadFull(rand.Reader, data); err != nil {
		t.Fatalf("Unable to create generate random data: %v", err)
	}
	if err = xlStorage.AppendFile(t.Context(), volume, object, data); err != nil {
		t.Fatalf("Unable to create object: %v", err)
	}

	for i, test := range xlStorageReadFileWithVerifyTests {
		h := test.algorithm.New()
		h.Write(data)
		if test.expError != nil {
			h.Write([]byte{0})
		}

		buffer := make([]byte, test.length)
		n, err := xlStorage.ReadFile(t.Context(), volume, test.file, int64(test.offset), buffer, NewBitrotVerifier(test.algorithm, h.Sum(nil)))

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
	volume := "fail-vol"
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	if err = xlStorage.MakeVol(t.Context(), volume); err != nil {
		t.Fatalf("MakeVol failed with %s", err)
	}

	// Change the format.json such that "this" is changed to "randomid".
	if err = os.WriteFile(pathJoin(xlStorage.String(), minioMetaBucket, formatConfigFile), []byte(`{"version":"1","format":"xl","id":"592a41c2-b7cc-4130-b883-c4b5cb15965b","xl":{"version":"3","this":"randomid","sets":[["e07285a6-8c73-4962-89c6-047fb939f803","33b8d431-482d-4376-b63c-626d229f0a29","cff6513a-4439-4dc1-bcaa-56c9e880c352","randomid","9c9f21d5-1f15-4737-bce6-835faa0d9626","0a59b346-1424-4fc2-9fa2-a2e80541d0c1","7924a3dc-b69a-4971-9a2e-014966d6aebb","4d2b8dd9-4e48-444b-bdca-c89194b26042"]],"distributionAlgo":"CRCMOD"}}`), 0o644); err != nil {
		t.Fatalf("ioutil.WriteFile failed with %s", err)
	}

	err = xlStorage.MakeVol(t.Context(), volume)
	if err != errVolumeExists {
		t.Fatalf("MakeVol expected to fail with errDiskNotFound but failed with %s", err)
	}
}

// TestXLStorage xlStorage.AppendFile()
func TestXLStorageAppendFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err = xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0o777); err != nil {
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
		if err = xlStorage.AppendFile(t.Context(), "success-vol", testCase.fileName, []byte("hello, world")); err != testCase.expectedErr {
			t.Errorf("Case: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestXLStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)

		var xlStoragePermStorage StorageAPI
		// Initialize xlStorage storage layer for permission denied error.
		_, err = newLocalXLStorage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0o755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		xlStoragePermStorage, err = newLocalXLStorage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize xlStorage, %s", err)
		}

		if err = xlStoragePermStorage.AppendFile(t.Context(), "mybucket", "myobject", []byte("hello, world")); err != errFileAccessDenied {
			t.Fatalf("expected: errFileAccessDenied error, got: %s", err)
		}
	}

	// TestXLStorage case with invalid volume name.
	// A valid volume name should be at least of size 3.
	err = xlStorage.AppendFile(t.Context(), "bn", "yes", []byte("hello, world"))
	if err != errVolumeNotFound {
		t.Fatalf("expected: \"Invalid argument error\", got: \"%s\"", err)
	}
}

// TestXLStorage xlStorage.RenameFile()
func TestXLStorageRenameFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err := xlStorage.MakeVol(t.Context(), "src-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.MakeVol(t.Context(), "dest-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.AppendFile(t.Context(), "src-vol", "file1", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile(t.Context(), "src-vol", "file2", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile(t.Context(), "src-vol", "file3", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile(t.Context(), "src-vol", "file4", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile(t.Context(), "src-vol", "file5", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err := xlStorage.AppendFile(t.Context(), "src-vol", "path/to/file1", []byte("Hello, world")); err != nil {
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
		// TestXLStorage case with invalid src volume name. Length should be at least 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "ab",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 13.
		// TestXLStorage case with invalid destination volume name. Length should be at least 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 14.
		// TestXLStorage case with invalid destination volume name. Length should be at least 3.
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
		if err := xlStorage.RenameFile(t.Context(), testCase.srcVol, testCase.srcPath, testCase.destVol, testCase.destPath); err != testCase.expectedErr {
			t.Fatalf("TestXLStorage %d:  Expected the error to be : \"%v\", got: \"%v\".", i+1, testCase.expectedErr, err)
		}
	}
}

// TestXLStorageDeleteVersion will test if version deletes and bulk deletes work as expected.
func TestXLStorageDeleteVersion(t *testing.T) {
	// create xlStorage test setup
	xl, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}
	ctx := t.Context()

	volume := "myvol-vol"
	object := "my-object"
	if err := xl.MakeVol(ctx, volume); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	var versions [50]string
	for i := range versions {
		versions[i] = uuid.New().String()
		fi := FileInfo{
			Name: object, Volume: volume, VersionID: versions[i], ModTime: UTCNow(), DataDir: "", Size: 10000,
			Erasure: ErasureInfo{
				Algorithm:    erasureAlgorithm,
				DataBlocks:   4,
				ParityBlocks: 4,
				BlockSize:    blockSizeV2,
				Index:        1,
				Distribution: []int{0, 1, 2, 3, 4, 5, 6, 7},
				Checksums:    nil,
			},
		}
		if err := xl.WriteMetadata(ctx, "", volume, object, fi); err != nil {
			t.Fatalf("Unable to create object, %s", err)
		}
	}
	var deleted [len(versions)]bool
	checkVerExist := func(t testing.TB) {
		t.Helper()
		for i := range versions {
			shouldExist := !deleted[i]
			fi, err := xl.ReadVersion(ctx, "", volume, object, versions[i], ReadOptions{})
			if shouldExist {
				if err != nil {
					t.Fatalf("Version %s should exist, but got err %v", versions[i], err)
				}
				return
			}
			if err != errFileVersionNotFound {
				t.Fatalf("Version %s should not exist, but returned: %#v", versions[i], fi)
			}
		}
	}

	// Delete version 0...
	checkVerExist(t)
	err = xl.DeleteVersion(ctx, volume, object, FileInfo{Name: object, Volume: volume, VersionID: versions[0]}, false, DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deleted[0] = true
	checkVerExist(t)

	// Delete 10 in bulk, including a non-existing.
	fis := []FileInfoVersions{{Name: object, Volume: volume}}
	for i := range versions[:10] {
		fis[0].Versions = append(fis[0].Versions, FileInfo{Name: object, Volume: volume, VersionID: versions[i]})
		deleted[i] = true
	}
	errs := xl.DeleteVersions(ctx, volume, fis, DeleteOptions{})
	if errs[0] != nil {
		t.Fatalf("expected nil error, got %v", errs[0])
	}
	checkVerExist(t)

	// Delete them all... (some again)
	fis[0].Versions = nil
	for i := range versions[:] {
		fis[0].Versions = append(fis[0].Versions, FileInfo{Name: object, Volume: volume, VersionID: versions[i]})
		deleted[i] = true
	}
	errs = xl.DeleteVersions(ctx, volume, fis, DeleteOptions{})
	if errs[0] != nil {
		t.Fatalf("expected nil error, got %v", errs[0])
	}
	checkVerExist(t)

	// Meta should be deleted now...
	fi, err := xl.ReadVersion(ctx, "", volume, object, "", ReadOptions{})
	if err != errFileNotFound {
		t.Fatalf("Object %s should not exist, but returned: %#v", object, fi)
	}
}

// TestXLStorage xlStorage.StatInfoFile()
func TestXLStorageStatInfoFile(t *testing.T) {
	// create xlStorage test setup
	xlStorage, _, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	// Setup test environment.
	if err := xlStorage.MakeVol(t.Context(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := xlStorage.AppendFile(t.Context(), "success-vol", pathJoin("success-file", xlStorageFormatFile), []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.AppendFile(t.Context(), "success-vol", pathJoin("path/to/success-file", xlStorageFormatFile), []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := xlStorage.MakeVol(t.Context(), "success-vol/path/to/"+xlStorageFormatFile); err != nil {
		t.Fatalf("Unable to create path, %s", err)
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
			expectedErr: errPathNotFound,
		},
		// TestXLStorage case - 4.
		// TestXLStorage case with non-existent file path.
		{
			srcVol:      "success-vol",
			srcPath:     "path/2/success-file",
			expectedErr: errPathNotFound,
		},
		// TestXLStorage case - 5.
		// TestXLStorage case with path being a directory.
		{
			srcVol:      "success-vol",
			srcPath:     "path",
			expectedErr: errPathNotFound,
		},
		// TestXLStorage case - 6.
		// TestXLStorage case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestXLStorage case - 7.
		// TestXLStorage case with file with directory.
		{
			srcVol:      "success-vol",
			srcPath:     "path/to",
			expectedErr: nil,
		},
	}

	for i, testCase := range testCases {
		_, err := xlStorage.StatInfoFile(t.Context(), testCase.srcVol, testCase.srcPath+"/"+xlStorageFormatFile, false)
		if err != testCase.expectedErr {
			t.Errorf("TestXLStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
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
	storage, path, err := newXLStorageTestSetup(t)
	if err != nil {
		t.Fatalf("Unable to create xlStorage test setup, %s", err)
	}

	volName := "testvol"
	fileName := "testfile"
	if err := storage.MakeVol(t.Context(), volName); err != nil {
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
	if err := storage.WriteAll(t.Context(), volName, fileName, data); err != nil {
		t.Fatal(err)
	}
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size, algo, hashBytes, 0); err != nil {
		t.Fatal(err)
	}

	// 2) Whole-file bitrot check on corrupted file
	if err := storage.AppendFile(t.Context(), volName, fileName, []byte("a")); err != nil {
		t.Fatal(err)
	}

	// Check if VerifyFile reports the incorrect file length (the correct length is `size+1`)
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size, algo, hashBytes, 0); err == nil {
		t.Fatal("expected to fail bitrot check")
	}

	// Check if bitrot fails
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size+1, algo, hashBytes, 0); err == nil {
		t.Fatal("expected to fail bitrot check")
	}

	if err := storage.Delete(t.Context(), volName, fileName, DeleteOptions{
		Recursive: false,
		Immediate: false,
	}); err != nil {
		t.Fatal(err)
	}

	// 3) Streaming bitrot check on proper file
	algo = HighwayHash256S
	shardSize := int64(1024 * 1024)
	shard := make([]byte, shardSize)
	w := newStreamingBitrotWriter(storage, "", volName, fileName, size, algo, shardSize)
	reader := bytes.NewReader(data)
	for {
		// Using io.Copy instead of this loop will not work for us as io.Copy
		// will use bytes.Reader.WriteTo() which will not do shardSize'ed writes
		// causing error.
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
	w.(io.Closer).Close()
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size, algo, nil, shardSize); err != nil {
		t.Fatal(err)
	}

	// 4) Streaming bitrot check on corrupted file
	filePath := pathJoin(storage.String(), volName, fileName)
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Replace first 256 with 'a'.
	if _, err := f.WriteString(strings.Repeat("a", 256)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size, algo, nil, shardSize); err == nil {
		t.Fatal("expected to fail bitrot check")
	}
	if err := storage.storage.bitrotVerify(t.Context(), pathJoin(path, volName, fileName), size+1, algo, nil, shardSize); err == nil {
		t.Fatal("expected to fail bitrot check")
	}
}

// TestXLStorageReadMetadata tests readMetadata
func TestXLStorageReadMetadata(t *testing.T) {
	volume, object := "test-vol", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tmpDir := t.TempDir()

	disk, err := newLocalXLStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	disk.MakeVol(t.Context(), volume)
	if _, err := disk.readMetadata(t.Context(), pathJoin(tmpDir, volume, object)); err != errFileNameTooLong {
		t.Fatalf("Unexpected error from readMetadata - expect %v: got %v", errFileNameTooLong, err)
	}
}
