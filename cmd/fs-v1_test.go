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
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/madmin-go"
)

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
	bucketName := "testbucket"
	objectName := "object"
	// Create and return an fsObject with its path in the disk
	prepareTest := func() (*FSObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(*FSObjects)

		objectContent := "12345"
		obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{})
		obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), ObjectOptions{})
		return fs, disk
	}

	// Test Shutdown with regular conditions
	fs, disk := prepareTest()
	if err := fs.Shutdown(GlobalContext); err != nil {
		t.Fatal("Cannot shutdown the FS object: ", err)
	}
	os.RemoveAll(disk)

	// Test Shutdown with faulty disk
	fs, disk = prepareTest()
	fs.DeleteObject(GlobalContext, bucketName, objectName, ObjectOptions{})
	os.RemoveAll(disk)
	if err := fs.Shutdown(GlobalContext); err != nil {
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

	err := obj.MakeBucketWithLocation(GlobalContext, "a", BucketOptions{})
	if !isSameType(err, BucketNameInvalid{}) {
		t.Fatal("BucketNameInvalid error not returned")
	}

	err = obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Test with valid parameters
	info, err := fs.GetBucketInfo(GlobalContext, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != bucketName {
		t.Fatalf("wrong bucket name, expected: %s, found: %s", bucketName, info.Name)
	}

	// Test with non-existent bucket
	_, err = fs.GetBucketInfo(GlobalContext, "a")
	if !isSameType(err, BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}

	// Check for buckets and should get disk not found.
	os.RemoveAll(disk)

	if _, err = fs.GetBucketInfo(GlobalContext, bucketName); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("BucketNotFound error not returned")
		}
	}
}

func TestFSPutObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	bucketName := "bucket"
	objectName := "1/2/3/4/object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal(err)
	}

	// With a regular object.
	_, err := obj.PutObject(GlobalContext, bucketName+"non-existent", objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	// With a directory object.
	_, err = obj.PutObject(GlobalContext, bucketName+"non-existent", objectName+SlashSeparator, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), 0, "", ""), ObjectOptions{})
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	_, err = obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
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

	obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{})
	obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})

	// Test with invalid bucket name
	if _, err := fs.DeleteObject(GlobalContext, "fo", objectName, ObjectOptions{}); !isSameType(err, BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with bucket does not exist
	if _, err := fs.DeleteObject(GlobalContext, "foobucket", "fooobject", ObjectOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if _, err := fs.DeleteObject(GlobalContext, bucketName, "\\", ObjectOptions{}); !(isSameType(err, ObjectNotFound{}) || isSameType(err, ObjectNameInvalid{})) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with object does not exist.
	if _, err := fs.DeleteObject(GlobalContext, bucketName, "foooobject", ObjectOptions{}); !isSameType(err, ObjectNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if _, err := fs.DeleteObject(GlobalContext, bucketName, objectName, ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete object should err disk not found.
	os.RemoveAll(disk)
	if _, err := fs.DeleteObject(GlobalContext, bucketName, objectName, ObjectOptions{}); err != nil {
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

	err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{})
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an invalid bucket name
	if err = fs.DeleteBucket(GlobalContext, "fo", DeleteBucketOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an inexistant bucket
	if err = fs.DeleteBucket(GlobalContext, "foobucket", DeleteBucketOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err = fs.DeleteBucket(GlobalContext, bucketName, DeleteBucketOptions{}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{})

	// Delete bucket should get error disk not found.
	os.RemoveAll(disk)
	if err = fs.DeleteBucket(GlobalContext, bucketName, DeleteBucketOptions{}); err != nil {
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
	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
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
	buckets, err := fs.ListBuckets(GlobalContext)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly", buckets)
	}

	// Test ListBuckets with disk not found.
	os.RemoveAll(disk)
	if _, err := fs.ListBuckets(GlobalContext); err != nil {
		if err != errDiskNotFound {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestFSHealObject - tests for fs HealObject
func TestFSHealObject(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.HealObject(GlobalContext, "bucket", "object", "", madmin.HealOpts{})
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSHealObjects - tests for fs HealObjects to return not implemented.
func TestFSHealObjects(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	err := obj.HealObjects(GlobalContext, "bucket", "prefix", madmin.HealOpts{}, nil)
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}
