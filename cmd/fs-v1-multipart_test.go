/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"os"
	"path/filepath"
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
		fs.cleanupStaleMultipartUploads(ctx, time.Millisecond, 0)
	}()

	// Wait for 100ms such that - we have given enough time for
	// cleanup routine to kick in. Flaky on slow systems...
	time.Sleep(100 * time.Millisecond)
	cancel()
	cleanupWg.Wait()

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, uploadID); err != nil {
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
	time.Sleep(time.Second) // Without Sleep on windows, the fs.AbortMultipartUpload() fails with "The process cannot access the file because it is being used by another process."
	if err := obj.AbortMultipartUpload(GlobalContext, bucketName, objectName, uploadID); err != nil {
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
