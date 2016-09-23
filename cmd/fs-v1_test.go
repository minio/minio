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
	"os"
	"path/filepath"
	"testing"
)

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that newFSObjects initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	// Setup to test errFSDiskFormat.
	disks := []string{}
	for i := 0; i < 6; i++ {
		xlDisk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
		defer removeAll(xlDisk)
		disks = append(disks, xlDisk)
	}

	// Initializes all disks with XL
	err := formatDisks(disks, nil)
	if err != nil {
		t.Fatalf("Unable to format XL %s", err)
	}
	_, err = newXLObjects(disks, nil)
	if err != nil {
		t.Fatalf("Unable to initialize XL object, %s", err)
	}

	testCases := []struct {
		disk        string
		expectedErr error
	}{
		{disk, nil},
		{disks[0], errFSDiskFormat},
	}

	for _, testCase := range testCases {
		if _, err := newFSObjects(testCase.disk); err != testCase.expectedErr {
			t.Fatalf("expected: %s, got: %s", testCase.expectedErr, err)
		}
	}
}

// TestFSShutdown - initialize a new FS object layer then calls Shutdown
// to check returned results
func TestFSShutdown(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}

	fs := obj.(fsObjects)
	fsStorage := fs.storage.(*posix)

	bucketName := "testbucket"
	objectName := "object"
	objectContent := "12345"

	obj.MakeBucket(bucketName)
	obj.PutObject(bucketName, objectName, int64(len(objectContent)), bytes.NewReader([]byte(objectContent)), nil)

	// Test Shutdown with regular conditions
	if err := fs.Shutdown(); err != nil {
		t.Fatal("Cannot shutdown the FS object: ", err)
	}

	// Test Shutdown with faulty disks
	for i := 1; i <= 5; i++ {
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if err := fs.Shutdown(); errorCause(err) != errFaultyDisk {
			t.Fatal(i, ", Got unexpected fs shutdown error: ", err)
		}
	}

}

// TestFSLoadFormatFS - test loadFormatFS with healty and faulty disks
func TestFSLoadFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	fs := obj.(fsObjects)

	// Regular format loading
	_, err = loadFormatFS(fs.storage)
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
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal(err)
	}

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
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, _ := newFSObjects(disk)
	fs := obj.(fsObjects)
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil)

	// Test with invalid bucket name
	if err := fs.DeleteObject("fo", objectName); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if err := fs.DeleteObject(bucketName, "^"); !isSameType(errorCause(err), ObjectNameInvalid{}) {
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
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, _ := newFSObjects(disk)
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
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, _ := newFSObjects(disk)
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
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}
	err = obj.HealObject("bucket", "object")
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSListObjectHeal - tests for fs ListObjectHeals
func TestFSListObjectsHeal(t *testing.T) {
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}
	_, err = obj.ListObjectsHeal("bucket", "prefix", "marker", "delimiter", 1000)
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSHealDiskMetadata - tests for fs HealDiskMetadata
func TestFSHealDiskMetadata(t *testing.T) {
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}
	err = obj.HealDiskMetadata()
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}
