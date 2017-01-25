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
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that newFSObjectLayer initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	_, err := newFSObjectLayer("")
	if err != errInvalidArgument {
		t.Errorf("Expecting error invalid argument, got %s", err)
	}
	_, err = newFSObjectLayer(disk)
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
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(rootPath)

	bucketName := "testbucket"
	objectName := "object"
	// Create and return an fsObject with its path in the disk
	prepareTest := func() (*fsObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(*fsObjects)
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
		removeAll(disk)
		if err := fs.Shutdown(); err != nil {
			t.Fatal(i, ", Got unexpected fs shutdown error: ", err)
		}
	}
}

// TestFSLoadFormatFS - test loadFormatFS with healty and faulty disks
func TestFSLoadFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	if err := saveFormatFS(preparePath(fsFormatPath), newFSFormatV1()); err != nil {
		t.Fatal("Should not fail here", err)
	}
	_, err := loadFormatFS(disk)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	// Loading corrupted format file
	file, err := os.OpenFile(preparePath(fsFormatPath), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	file.Write([]byte{'b'})
	file.Close()
	_, err = loadFormatFS(disk)
	if err == nil {
		t.Fatal("Should return an error here")
	}
	// Loading format file from disk not found.
	removeAll(disk)
	_, err = loadFormatFS(disk)
	if err != nil && err != errUnformattedDisk {
		t.Fatal("Should return unformatted disk, but got", err)
	}
}

// TestFSGetBucketInfo - test GetBucketInfo with healty and faulty disks
func TestFSGetBucketInfo(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
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

	// Check for buckets and should get disk not found.
	removeAll(disk)
	_, err = fs.GetBucketInfo(bucketName)
	if !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}
}

// TestFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestFSDeleteObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	sha256sum := ""
	obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)

	// Test with invalid bucket name
	if err := fs.DeleteObject("fo", objectName); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with bucket does not exist
	if err := fs.DeleteObject("foobucket", "fooobject"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if err := fs.DeleteObject(bucketName, "\\"); !isSameType(errorCause(err), ObjectNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with object does not exist.
	if err := fs.DeleteObject(bucketName, "foooobject"); !isSameType(errorCause(err), ObjectNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if err := fs.DeleteObject(bucketName, objectName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete object should err disk not found.
	removeAll(disk)
	if err := fs.DeleteObject(bucketName, objectName); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}

}

// TestFSDeleteBucket - tests for fs DeleteBucket
func TestFSDeleteBucket(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"

	err := obj.MakeBucket(bucketName)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an invalid bucket name
	if err = fs.DeleteBucket("fo"); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with an inexistant bucket
	if err = fs.DeleteBucket("foobucket"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err = fs.DeleteBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	obj.MakeBucket(bucketName)

	// Delete bucker should get error disk not found.
	removeAll(disk)
	if err = fs.DeleteBucket(bucketName); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
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
	fs := obj.(*fsObjects)

	bucketName := "bucket"
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Create a bucket with invalid name
	if err := mkdirAll(pathJoin(fs.fsPath, "vo^"), 0777); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f, err := os.Create(pathJoin(fs.fsPath, "test"))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f.Close()

	// Test list buckets to have only one entry.
	buckets, err := fs.ListBuckets()
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly", buckets)
	}

	// Test ListBuckets with disk not found.
	removeAll(disk)

	if _, err := fs.ListBuckets(); err != nil {
		if errorCause(err) != errDiskNotFound {
			t.Fatal("Unexpected error: ", err)
		}
	}

	longPath := fmt.Sprintf("%0256d", 1)
	fs.fsPath = longPath
	if _, err := fs.ListBuckets(); err != nil {
		if errorCause(err) != errFileNameTooLong {
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
