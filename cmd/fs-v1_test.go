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

package cmd

import (
	"bytes"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
)

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that newFSObjects initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Setup to test errFSDiskFormat.
	disks := []string{}
	for i := 0; i < 6; i++ {
		xlDisk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		defer removeAll(xlDisk)
		disks = append(disks, xlDisk)
	}

	fsEndpoints, err := parseStorageEndpoints([]string{disk})
	if err != nil {
		t.Fatal("Uexpected error: ", err)
	}

	_, err = initStorageDisks(fsEndpoints, defaultRetryConfig)
	if err != nil {
		t.Fatal("Uexpected error: ", err)
	}

	endpoints, err := parseStorageEndpoints(disks)
	if err != nil {
		t.Fatal("Uexpected error: ", err)
	}

	// Initializes all disks with XL
	formattedDisks, err := waitForFormatDisks(true, endpoints)
	if err != nil {
		t.Fatalf("Unable to format XL %s", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err != nil {
		t.Fatalf("Unable to initialize XL object, %s", err)
	}

	testCases := []struct {
		endpoints   []*url.URL
		expectedErr error
	}{
		{fsEndpoints, nil},
		{endpoints, errors.New("Unsupported backend format [fs] found")},
	}

	for _, testCase := range testCases {
		testCase.endpoints[0] = fsEndpoints[0]
		if _, err = waitForFormatDisks(true, testCase.endpoints); err != nil {
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("expected: %s, got %s", testCase.expectedErr, err)
			}
		}
	}
	_, err = newFSObjects(nil)
	if err != errInvalidArgument {
		t.Errorf("Expecting error invalid argument, got %s", err)
	}

	_, err = newFSObjects(formattedDisks[0])
	if err != nil {
		errMsg := "Unable to recognize backend format, Disk is not in FS format."
		if err.Error() == errMsg {
			t.Errorf("Expecting %s, got %s", errMsg, err)
		}
	}
}

// TestFSShutdown - initialize a new FS object layer then calls
// Shutdown to check returned results
func TestFSShutdown(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(rootPath)

	bucketName := "testbucket"
	objectName := "object"
	// Create and return an fsObject with its path in the disk
	prepareTest := func() (fsObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(fsObjects)
		objectContent := "12345"
		obj.MakeBucket(bucketName)
		sha256sum := ""
		obj.PutObject(bucketName, objectName, int64(len(objectContent)), bytes.NewReader([]byte(objectContent)), nil, sha256sum)
		return fs, disk
	}

	// Test Shutdown with regular conditions
	fs, disk := prepareTest()
	if err := fs.Shutdown(); err != nil {
		t.Fatal("Cannot shutdown the FS object: ", err)
	}
	removeAll(disk)

	// Test Shutdown with faulty disk
	for i := 1; i <= 5; i++ {
		fs, disk := prepareTest()
		fs.DeleteObject(bucketName, objectName)
		fsStorage := fs.storage.(*posix)
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if err := fs.Shutdown(); errorCause(err) != errFaultyDisk {
			t.Fatal(i, ", Got unexpected fs shutdown error: ", err)
		}
		removeAll(disk)
	}
}

// TestFSLoadFormatFS - test loadFormatFS with healty and faulty disks
func TestFSLoadFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)

	// Regular format loading
	_, err := loadFormatFS(fs.storage)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	// Loading corrupted format file
	fs.storage.AppendFile(minioMetaBucket, fsFormatJSONFile, []byte{'b'})
	_, err = loadFormatFS(fs.storage)
	if err == nil {
		t.Fatal("Should return an error here")
	}
	// Loading format file from faulty disk
	fsStorage := fs.storage.(*posix)
	fs.storage = newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	_, err = loadFormatFS(fs.storage)
	if err != errFaultyDisk {
		t.Fatal("Should return faulty disk error")
	}
}

// TestFSGetBucketInfo - test GetBucketInfo with healty and faulty disks
func TestFSGetBucketInfo(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)
	bucketName := "bucket"

	obj.MakeBucket(bucketName)

	// Test with valid parameters
	info, err := fs.GetBucketInfo(bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != bucketName {
		t.Fatalf("wrong bucket name, expected: %s, found: %s", bucketName, info.Name)
	}

	// Test with inexistant bucket
	_, err = fs.GetBucketInfo("a")
	if !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("BucketNameInvalid error not returned")
	}

	// Loading format file from faulty disk
	fsStorage := fs.storage.(*posix)
	fs.storage = newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	_, err = fs.GetBucketInfo(bucketName)
	if errorCause(err) != errFaultyDisk {
		t.Fatal("errFaultyDisk error not returned")
	}

}

// TestFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestFSDeleteObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	sha256sum := ""
	obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)

	// Test with invalid bucket name
	if err := fs.DeleteObject("fo", objectName); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if err := fs.DeleteObject(bucketName, "\\"); !isSameType(errorCause(err), ObjectNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with inexist bucket/object
	if err := fs.DeleteObject("foobucket", "fooobject"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if err := fs.DeleteObject(bucketName, objectName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Loading format file from faulty disk
	fsStorage := fs.storage.(*posix)
	fs.storage = newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	if err := fs.DeleteObject(bucketName, objectName); errorCause(err) != errFaultyDisk {
		t.Fatal("Unexpected error: ", err)
	}

}

// TestFSDeleteBucket - tests for fs DeleteBucket
func TestFSDeleteBucket(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)
	bucketName := "bucket"

	err := obj.MakeBucket(bucketName)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an invalid bucket name
	if err := fs.DeleteBucket("fo"); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with an inexistant bucket
	if err := fs.DeleteBucket("foobucket"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err := fs.DeleteBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	obj.MakeBucket(bucketName)

	// Loading format file from faulty disk
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 2; i++ {
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if err := fs.DeleteBucket(bucketName); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected error: ", err)
		}
	}

}

// TestFSListBuckets - tests for fs ListBuckets
func TestFSListBuckets(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)

	bucketName := "bucket"
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Create a bucket with invalid name
	if err := fs.storage.MakeVol("vo^"); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test
	buckets, err := fs.ListBuckets()
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly")
	}

	// Test ListBuckets with faulty disks
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 2; i++ {
		fs.storage = newNaughtyDisk(fsStorage, nil, errFaultyDisk)
		if _, err := fs.ListBuckets(); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected error: ", err)
		}
	}

}

// TestFSHealObject - tests for fs HealObject
func TestFSHealObject(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	err := obj.HealObject("bucket", "object")
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSListObjectHeal - tests for fs ListObjectHeals
func TestFSListObjectsHeal(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.ListObjectsHeal("bucket", "prefix", "marker", "delimiter", 1000)
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}
