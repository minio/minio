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
	"path/filepath"
	"testing"
)

// TestNewMultipartUploadFaultyDisk - test NewMultipartUpload with faulty disks
func TestNewMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)

	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	// Test with disk removed.
	removeAll(disk) // remove disk.
	if _, err := fs.NewMultipartUpload(bucketName, objectName, map[string]string{"X-Amz-Meta-xid": "3f"}); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestPutObjectPartFaultyDisk - test PutObjectPart with faulty disks
func TestPutObjectPartFaultyDisk(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")
	dataLen := int64(len(data))

	if err = obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := fs.NewMultipartUpload(bucketName, objectName, map[string]string{"X-Amz-Meta-xid": "3f"})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)
	sha256sum := ""

	removeAll(disk) // Disk not found.
	_, err = fs.PutObjectPart(bucketName, objectName, uploadID, 1, dataLen, bytes.NewReader(data), md5Hex, sha256sum)
	if !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error ", err)
	}
}

// TestCompleteMultipartUploadFaultyDisk - test CompleteMultipartUpload with faulty disks
func TestCompleteMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)

	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := fs.NewMultipartUpload(bucketName, objectName, map[string]string{"X-Amz-Meta-xid": "3f"})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)
	sha256sum := ""

	if _, err := fs.PutObjectPart(bucketName, objectName, uploadID, 1, 5, bytes.NewReader(data), md5Hex, sha256sum); err != nil {
		t.Fatal("Unexpected error ", err)
	}

	parts := []completePart{{PartNumber: 1, ETag: md5Hex}}

	removeAll(disk) // Disk not found.
	if _, err := fs.CompleteMultipartUpload(bucketName, objectName, uploadID, parts); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}

// TestListMultipartUploadsFaultyDisk - test ListMultipartUploads with faulty disks
func TestListMultipartUploadsFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)

	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"
	data := []byte("12345")

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	uploadID, err := fs.NewMultipartUpload(bucketName, objectName, map[string]string{"X-Amz-Meta-xid": "3f"})
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	md5Hex := getMD5Hash(data)
	sha256sum := ""

	if _, err := fs.PutObjectPart(bucketName, objectName, uploadID, 1, 5, bytes.NewReader(data), md5Hex, sha256sum); err != nil {
		t.Fatal("Unexpected error ", err)
	}

	removeAll(disk) // Disk not found.
	if _, err := fs.ListMultipartUploads(bucketName, objectName, "", "", "", 1000); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error ", err)
		}
	}
}
