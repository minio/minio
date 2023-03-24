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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/madmin-go"
)

// TestNewPANFS - tests initialization of all input disks
// and constructs a valid `PANFS` object layer.
func TestNewPANFS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Do not attempt to create this path, the test validates
	// so that NewPANFSObjectLayer initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	_, err := NewPANFSObjectLayer(ctx, "")
	if err != errInvalidArgument {
		t.Errorf("Expecting error invalid argument, got %s", err)
	}
	_, err = NewPANFSObjectLayer(ctx, disk)
	if err != nil {
		errMsg := "Unable to recognize backend format, Drive is not in FS format."
		if err.Error() == errMsg {
			t.Errorf("Expecting %s, got %s", errMsg, err)
		}
	}
}

// TestFSShutdown - initialize a new FS object layer then calls
// Shutdown to check returned results
func TestPANFSShutdown(t *testing.T) {
	t.Skip()

	bucketName := "testbucket"
	objectName := "object"
	// Create and return an panfsObject with its path in the disk
	prepareTest := func() (*PANFSObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(*PANFSObjects)

		objectContent := "12345"
		obj.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{})
		obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), ObjectOptions{})
		return fs, disk
	}

	// Test Shutdown with regular conditions
	fs, disk := prepareTest()
	if err := fs.Shutdown(GlobalContext); err != nil {
		t.Fatal("Cannot shutdown the PANFS object: ", err)
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

// TestPANFSGetBucketInfo - test GetBucketInfo with healty and faulty disks
func TestPANFSGetBucketInfo(t *testing.T) {
	// Prepare for testing
	bucketName := getRandomBucketName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	fs := obj.(*PANFSObjects)

	err := obj.MakeBucketWithLocation(GlobalContext, "a", MakeBucketOptions{})
	if !isSameType(err, BucketNameInvalid{}) {
		t.Fatalf("Expecting error %v, got %s\"", BucketNameInvalid{}, err)
	}

	info, err := fs.GetBucketInfo(GlobalContext, bucketName, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	expectedPanFSBucketPath := pathJoin(disk, bucketName+"/")
	if info.PanFSPath != expectedPanFSBucketPath {
		t.Fatalf("wrong bucket panfs path, expected: %s, found: %s", expectedPanFSBucketPath, info.PanFSPath)
	}
	// Test with non-existent bucket
	_, err = fs.GetBucketInfo(GlobalContext, "a", BucketOptions{})
	if !isSameType(err, BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}

	// Check for buckets and should get disk not found.
	os.RemoveAll(disk)

	if _, err = fs.GetBucketInfo(GlobalContext, bucketName, BucketOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("BucketNotFound error not returned")
		}
	}
}

func TestPANFSPutObject(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	fs := obj.(*PANFSObjects)

	// With a regular object.
	_, err := fs.PutObject(GlobalContext, bucketName+"non-existent", objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	// With a directory object.
	_, err = fs.PutObject(GlobalContext, bucketName+"non-existent", objectName+SlashSeparator, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), 0, "", ""), ObjectOptions{})
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := err.(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	_, err = fs.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// With invalid object name .s3
	_, err = fs.PutObject(GlobalContext, bucketName, ".s3", mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err == nil {
		t.Fatalf("Expected error type ObjectNameInvalid, got %#v", err)
	}
}

// TestPANFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestPANFSDeleteObject(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)

	obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})

	// Test with invalid bucket name
	if _, err := obj.DeleteObject(GlobalContext, "fo", objectName, ObjectOptions{}); !isSameType(err, BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with bucket does not exist
	if _, err := obj.DeleteObject(GlobalContext, "foobucket", "fooobject", ObjectOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if _, err := obj.DeleteObject(GlobalContext, bucketName, "\\", ObjectOptions{}); !(isSameType(err, ObjectNotFound{}) || isSameType(err, ObjectNameInvalid{})) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with object does not exist.
	if _, err := obj.DeleteObject(GlobalContext, bucketName, "foooobject", ObjectOptions{}); !isSameType(err, ObjectNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if _, err := obj.DeleteObject(GlobalContext, bucketName, objectName, ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete object should err disk not found.
	os.RemoveAll(disk)
	if _, err := obj.DeleteObject(GlobalContext, bucketName, objectName, ObjectOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestPANFSDeleteBucket - tests for fs DeleteBucket
func TestPANFSDeleteBucket(t *testing.T) {
	// Prepare for testing
	bucketName := getRandomBucketName()
	obj, disk := initPanFSWithBucket(bucketName, t)
	defer os.RemoveAll(disk)
	fs := obj.(*PANFSObjects)

	// Test with an invalid bucket name
	if err := fs.DeleteBucket(GlobalContext, "fo", DeleteBucketOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an not existing bucket
	if err := fs.DeleteBucket(GlobalContext, "foobucket", DeleteBucketOptions{}); !isSameType(err, BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err := fs.DeleteBucket(GlobalContext, bucketName, DeleteBucketOptions{}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	// Make sure that panfs bucket dir is not deleted
	panfsBucketDir := pathJoin(disk, bucketName)
	if _, err := fsStatDir(GlobalContext, panfsBucketDir); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete bucket should get error disk not found.
	os.RemoveAll(disk)
	if err := fs.DeleteBucket(GlobalContext, bucketName, DeleteBucketOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestPANFSListBuckets - tests for fs ListBuckets
func TestPANFSListBuckets(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj, err := initPanFSObjects(disk)
	if err != nil {
		t.Fatal(err)
	}
	fs := obj.(*PANFSObjects)

	bucketName := "bucket"
	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{PanFSBucketPath: disk}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Create a bucket with invalid name
	if err := os.MkdirAll(pathJoin(fs.fsPath, "vo^"), 0o777); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f, err := os.Create(pathJoin(fs.fsPath, "test"))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f.Close()

	// Test list buckets to have only one entry.
	buckets, err := fs.ListBuckets(GlobalContext, BucketOptions{})
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly", buckets)
	}

	// Test ListBuckets with disk not found.
	os.RemoveAll(disk)
	if _, err := fs.ListBuckets(GlobalContext, BucketOptions{}); err != nil {
		if err != errDiskNotFound {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestPANFSHealObject - tests for fs HealObject
func TestPANFSHealObject(t *testing.T) {
	t.Skip()
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.HealObject(GlobalContext, "bucket", "object", "", madmin.HealOpts{})
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestPANFSHealObjects - tests for fs HealObjects to return not implemented.
func TestPANFSHealObjects(t *testing.T) {
	t.Skip()
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	err := obj.HealObjects(GlobalContext, "bucket", "prefix", madmin.HealOpts{}, nil)
	if err == nil || !isSameType(err, NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestCheckForS3Prefix
func TestDotS3PrefixCheck(t *testing.T) {
	testCases := []struct {
		input         []string
		shouldPass    bool
		expectedError error
	}{
		{[]string{"valid", ""}, true, nil},
		{[]string{}, true, nil},
		{[]string{"file", ".s3file"}, true, nil},
		{[]string{"file", ".s3/file"}, false, PanFSS3InvalidName{}},
		{[]string{"file", ".s3/file", ".s3/nextfile"}, false, PanFSS3InvalidName{}},
	}

	for _, tc := range testCases {
		err := dotS3PrefixCheck(tc.input...)
		if err != nil && !tc.shouldPass {
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("Expected error %v, got %v. Input args: %v", tc.expectedError, err, tc.input)
			}
		}

		if err == nil && !tc.shouldPass {
			t.Fatalf("Expecting an error %v. Input args: %v", tc.expectedError, tc.input)
		}

		if err != nil && tc.shouldPass {
			t.Fatalf("Unexpected error %v. Input args: %v", err, tc.input)
		}
	}
}

// // TestPANFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestPANFSMakeBucket(t *testing.T) {
	// Prepare for tests
	bucketName := getRandomBucketName()
	obj, disk := initPanFSWithBucket("", t)
	defer os.RemoveAll(disk)

	fs := obj.(*PANFSObjects)

	// Create bucket using non-existent panfs path
	nonExistentBucketPath := pathJoin(disk, "nonExistentBucketPath")
	err := fs.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{PanFSBucketPath: nonExistentBucketPath})
	if !errors.Is(err, errDiskAccessDenied) {
		t.Fatalf("Expected error %v, got %v", errDiskAccessDenied, err)
	}

	// Valid case
	bucketPath := pathJoin(disk, bucketName)
	if err := fsMkdir(GlobalContext, bucketPath); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if err := fs.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{PanFSBucketPath: bucketPath}); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Attempt to create another bucket with the same panfs path
	err = fs.MakeBucketWithLocation(GlobalContext, getRandomBucketName(), MakeBucketOptions{PanFSBucketPath: bucketPath})
	if !errors.Is(err, PanFSInvalidBucketPath{BucketPath: bucketPath}) {
		t.Fatalf("Expected error %v, got %v", PanFSInvalidBucketPath{BucketPath: bucketPath}, err)
	}

	// Attempt to create bucket with the same name but different panfs path
	anotherBucketPath := pathJoin(disk, nextSuffix())
	if err := fsMkdir(GlobalContext, anotherBucketPath); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	err = fs.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{PanFSBucketPath: anotherBucketPath})
	if !errors.Is(err, BucketExists{Bucket: bucketName}) {
		t.Fatalf("Expected error %v, got %v", BucketExists{Bucket: bucketName}, err)
	}

	// Attempt to create another bucket inside previously created bucket
	innerBucketPath := pathJoin(bucketPath, "innerDir")
	if err := fsMkdir(GlobalContext, innerBucketPath); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	err = fs.MakeBucketWithLocation(GlobalContext, getRandomBucketName(), MakeBucketOptions{PanFSBucketPath: innerBucketPath})
	if !errors.Is(err, PanFSInvalidBucketPath{BucketPath: innerBucketPath}) {
		t.Fatalf("Expected error %v, got %v", PanFSInvalidBucketPath{BucketPath: innerBucketPath}, err)
	}

	// Attempt to create a bucket on the top level of the existing one
	err = fs.MakeBucketWithLocation(GlobalContext, getRandomBucketName(), MakeBucketOptions{PanFSBucketPath: disk})
	if !errors.Is(err, PanFSInvalidBucketPath{BucketPath: disk}) {
		t.Fatalf("Expected error %v, got %v", PanFSInvalidBucketPath{BucketPath: disk}, err)
	}
}
