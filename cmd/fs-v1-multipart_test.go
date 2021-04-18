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
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Tests cleanup multipart uploads for filesystem backend.
func TestFSCleanupMultipartUploadsInRoutine(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)

	bucketName := "bucket"
	objectName := "object"

	// Create a context we can cancel.
	ctx, cancel := context.WithCancel(GlobalContext)
	obj.MakeBucketWithLocation(ctx, bucketName, BucketOptions{})

	uploadID, err := obj.NewMultipartUpload(ctx, bucketName, objectName, ObjectOptions{})
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	var cleanupWg sync.WaitGroup
	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()
		fs.cleanupStaleUploads(ctx, time.Millisecond, 0)
	}()

	// Wait for 100ms such that - we have given enough time for
	// cleanup routine to kick in. Flaky on slow systems...
	time.Sleep(100 * time.Millisecond)
	cancel()
	cleanupWg.Wait()

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, uploadID, ObjectOptions{}); err != nil {
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	} else {
		t.Error("Item was not cleaned up.")
	}
}

// TestNewMultipartUploadFaultyDisk - test NewMultipartUpload with faulty disks
func TestNewMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)
	obj := initFSObjects(disk, t)

	fs := obj.(*FSObjects)
	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	// Test with disk removed.
	os.RemoveAll(disk)
	if _, err := fs.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestPutObjectPartFaultyDisk - test PutObjectPart with faulty disks
func TestPutObjectPartFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)
	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")
	dataLen := int64(len(data))

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)
	sha256sum := ""

	newDisk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(newDisk)
	obj = initFSObjects(newDisk, t)
	if _, err = obj.PutObjectPart(GlobalContext, bucketName, objectName, uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), dataLen, md5Hex, sha256sum), ObjectOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestCompleteMultipartUploadFaultyDisk - test CompleteMultipartUpload with faulty disks
func TestCompleteMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)
	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)

	parts := []CompletePart{{PartNumber: 1, ETag: md5Hex}}
	newDisk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(newDisk)
	obj = initFSObjects(newDisk, t)
	if _, err := obj.CompleteMultipartUpload(GlobalContext, bucketName, objectName, uploadID, parts, ObjectOptions{}); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestCompleteMultipartUpload - test CompleteMultipartUpload
func TestCompleteMultipartUpload(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)
	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)

	if _, err := obj.PutObjectPart(GlobalContext, bucketName, objectName, uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), 5, md5Hex, ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error ", err)
	}

	parts := []CompletePart{{PartNumber: 1, ETag: md5Hex}}
	if _, err := obj.CompleteMultipartUpload(GlobalContext, bucketName, objectName, uploadID, parts, ObjectOptions{}); err != nil {
		t.Fatal("Unexpected error ", err)
	}
}

// TestCompleteMultipartUpload - test CompleteMultipartUpload
func TestAbortMultipartUpload(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		// Concurrent AbortMultipartUpload() fails on windows
		t.Skip()
	}

	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)
	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)

	opts := ObjectOptions{}
	if _, err := obj.PutObjectPart(GlobalContext, bucketName, objectName, uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(data), 5, md5Hex, ""), opts); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	if err := obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, uploadID, opts); err != nil {
		t.Fatal("Unexpected error ", err)
	}
}

// TestListMultipartUploadsFaultyDisk - test ListMultipartUploads with faulty disks
func TestListMultipartUploadsFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	_, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-xid": "3f"}})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	newDisk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(newDisk)
	obj = initFSObjects(newDisk, t)
	if _, err := obj.ListMultipartUploads(GlobalContext, bucketName, objectName, "", "", "", 1000); err != nil {
		if !isSameType(err, BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}
