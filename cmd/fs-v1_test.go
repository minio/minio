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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Tests for if parent directory is object
func TestFSParentDirIsObject(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	obj, disk, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(disk)

	bucketName := "testbucket"
	objectName := "object"

	if err = obj.MakeBucketWithLocation(context.Background(), bucketName, ""); err != nil {
		t.Fatal(err)
	}
	objectContent := "12345"
	objInfo, err := obj.PutObject(context.Background(), bucketName, objectName,
		mustGetHashReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	if objInfo.Name != objectName {
		t.Fatalf("Unexpected object name returned got %s, expected %s", objInfo.Name, objectName)
	}

	fs := obj.(*FSObjects)
	testCases := []struct {
		parentIsObject bool
		objectName     string
	}{
		// parentIsObject is true if object is available.
		{
			parentIsObject: true,
			objectName:     objectName,
		},
		{
			parentIsObject: false,
			objectName:     "",
		},
		{
			parentIsObject: false,
			objectName:     ".",
		},
		// Should not cause infinite loop.
		{
			parentIsObject: false,
			objectName:     "/",
		},
		{
			parentIsObject: false,
			objectName:     "\\",
		},
		// Should not cause infinite loop with double forward slash.
		{
			parentIsObject: false,
			objectName:     "//",
		},
	}
	for i, testCase := range testCases {
		gotValue := fs.parentDirIsObject(context.Background(), bucketName, testCase.objectName)
		if testCase.parentIsObject != gotValue {
			t.Errorf("Test %d: Unexpected value returned got %t, expected %t", i+1, gotValue, testCase.parentIsObject)
		}
	}
}

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that NewFSObjectLayer initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	_, err := NewFSObjectLayer("")
	if err != errInvalidArgument {
		t.Errorf("Expecting error invalid argument, got %s", err)
	}
	_, err = NewFSObjectLayer(disk)
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
	defer os.RemoveAll(rootPath)

	bucketName := "testbucket"
	objectName := "object"
	// Create and return an fsObject with its path in the disk
	prepareTest := func() (*FSObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(*FSObjects)
		objectContent := "12345"
		obj.MakeBucketWithLocation(context.Background(), bucketName, "")
		obj.PutObject(context.Background(), bucketName, objectName, mustGetHashReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), nil)
		return fs, disk
	}

	// Test Shutdown with regular conditions
	fs, disk := prepareTest()
	if err := fs.Shutdown(context.Background()); err != nil {
		t.Fatal("Cannot shutdown the FS object: ", err)
	}
	os.RemoveAll(disk)

	// Test Shutdown with faulty disk
	fs, disk = prepareTest()
	fs.DeleteObject(context.Background(), bucketName, objectName)
	os.RemoveAll(disk)
	if err := fs.Shutdown(context.Background()); err != nil {
		t.Fatal("Got unexpected fs shutdown error: ", err)
	}
}

// TestFSGetBucketInfo - test GetBucketInfo with healty and faulty disks
func TestFSGetBucketInfo(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)
	bucketName := "bucket"

	err := obj.MakeBucketWithLocation(context.Background(), "a", "")
	if !isSameType(err, BucketNameInvalid{}) {
		t.Fatal("BucketNameInvalid error not returned")
	}

	err = obj.MakeBucketWithLocation(context.Background(), bucketName, "")
	if err != nil {
		t.Fatal(err)
	}

	// Test with valid parameters
	info, err := fs.GetBucketInfo(context.Background(), bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != bucketName {
		t.Fatalf("wrong bucket name, expected: %s, found: %s", bucketName, info.Name)
	}

	// Test with non-existent bucket
	_, err = fs.GetBucketInfo(context.Background(), "a")
	if !isSameType(err, BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}

	// Check for buckets and should get disk not found.
	fs.fsPath = filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())

	_, err = fs.GetBucketInfo(context.Background(), bucketName)
	if !isSameType(err, BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}
}

func TestFSPutObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	bucketName := "bucket"
	objectName := "1/2/3/4/object"

	if err := obj.MakeBucketWithLocation(context.Background(), bucketName, ""); err != nil {
		t.Fatal(err)
	}

	// With a regular object.
	_, err := obj.PutObject(context.Background(), bucketName+"non-existent", objectName, mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	// With a directory object.
	_, err = obj.PutObject(context.Background(), bucketName+"non-existent", objectName+"/", mustGetHashReader(t, bytes.NewReader([]byte("abcd")), 0, "", ""), nil)
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = obj.PutObject(context.Background(), bucketName, objectName+"/1", mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
	if err == nil {
		t.Fatal("Unexpected should fail here, backend corruption occurred")
	}
	if nerr, ok := err.(PrefixAccessDenied); !ok {
		t.Fatalf("Expected PrefixAccessDenied, got %#v", err)
	} else {
		if nerr.Bucket != "bucket" {
			t.Fatalf("Expected 'bucket', got %s", nerr.Bucket)
		}
		if nerr.Object != "1/2/3/4/object/1" {
			t.Fatalf("Expected '1/2/3/4/object/1', got %s", nerr.Object)
		}
	}

	_, err = obj.PutObject(context.Background(), bucketName, objectName+"/1/", mustGetHashReader(t, bytes.NewReader([]byte("abcd")), 0, "", ""), nil)
	if err == nil {
		t.Fatal("Unexpected should fail here, backned corruption occurred")
	}
	if nerr, ok := err.(PrefixAccessDenied); !ok {
		t.Fatalf("Expected PrefixAccessDenied, got %#v", err)
	} else {
		if nerr.Bucket != "bucket" {
			t.Fatalf("Expected 'bucket', got %s", nerr.Bucket)
		}
		if nerr.Object != "1/2/3/4/object/1/" {
			t.Fatalf("Expected '1/2/3/4/object/1/', got %s", nerr.Object)
		}
	}
}

// TestFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestFSDeleteObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucketWithLocation(context.Background(), bucketName, "")
	obj.PutObject(context.Background(), bucketName, objectName, mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)

	// Test with invalid bucket name
	if err := fs.DeleteObject(context.Background(), "fo", objectName); !isSameType(err, BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with bucket does not exist
	if err := fs.DeleteObject(context.Background(), "foobucket", "fooobject"); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if err := fs.DeleteObject(context.Background(), bucketName, "\\"); !isSameType(err, ObjectNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with object does not exist.
	if err := fs.DeleteObject(context.Background(), bucketName, "foooobject"); !isSameType(err, ObjectNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if err := fs.DeleteObject(context.Background(), bucketName, objectName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete object should err disk not found.
	fs.fsPath = filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	if err := fs.DeleteObject(context.Background(), bucketName, objectName); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}

}

// TestFSDeleteBucket - tests for fs DeleteBucket
func TestFSDeleteBucket(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)
	bucketName := "bucket"

	err := obj.MakeBucketWithLocation(context.Background(), bucketName, "")
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an invalid bucket name
	if err = fs.DeleteBucket(context.Background(), "fo"); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an inexistant bucket
	if err = fs.DeleteBucket(context.Background(), "foobucket"); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err = fs.DeleteBucket(context.Background(), bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	obj.MakeBucketWithLocation(context.Background(), bucketName, "")

	// Delete bucket should get error disk not found.
	fs.fsPath = filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	if err = fs.DeleteBucket(context.Background(), bucketName); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestFSListBuckets - tests for fs ListBuckets
func TestFSListBuckets(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)

	bucketName := "bucket"
	if err := obj.MakeBucketWithLocation(context.Background(), bucketName, ""); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Create a bucket with invalid name
	if err := os.MkdirAll(pathJoin(fs.fsPath, "vo^"), 0777); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f, err := os.Create(pathJoin(fs.fsPath, "test"))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f.Close()

	// Test list buckets to have only one entry.
	buckets, err := fs.ListBuckets(context.Background())
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly", buckets)
	}

	// Test ListBuckets with disk not found.
	fs.fsPath = filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())

	if _, err := fs.ListBuckets(context.Background()); err != nil {
		if err != errDiskNotFound {
			t.Fatal("Unexpected error: ", err)
		}
	}

	longPath := fmt.Sprintf("%0256d", 1)
	fs.fsPath = longPath
	if _, err := fs.ListBuckets(context.Background()); err != nil {
		if err != errFileNameTooLong {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestFSHealObject - tests for fs HealObject
func TestFSHealObject(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.HealObject(context.Background(), "bucket", "object", false)
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSListObjectHeal - tests for fs ListObjectHeals
func TestFSListObjectsHeal(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.ListObjectsHeal(context.Background(), "bucket", "prefix", "marker", "delimiter", 1000)
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}
