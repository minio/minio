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
	"reflect"
	"testing"
)

// TestNewMultipartUploadFaultyDisk - test NewMultipartUpload with faulty disks
func TestNewMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)

	fs := obj.(fsObjects)
	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	// Test with faulty disk
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 5; i++ {
		// Faulty disk generates errFaultyDisk at 'i' storage api call number
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if _, err := fs.NewMultipartUpload(bucketName, objectName, map[string]string{"X-Amz-Meta-xid": "3f"}); errorCause(err) != errFaultyDisk {
			switch i {
			case 1:
				if !isSameType(errorCause(err), BucketNotFound{}) {
					t.Fatal("Unexpected error ", err)
				}
			default:
				t.Fatal("Unexpected error ", err)
			}
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
	fs := obj.(fsObjects)
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

	// Test with faulty disk
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 7; i++ {
		// Faulty disk generates errFaultyDisk at 'i' storage api call number
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		md5sum, err := fs.PutObjectPart(bucketName, objectName, uploadID, 1, dataLen, bytes.NewReader(data), md5Hex, sha256sum)
		if errorCause(err) != errFaultyDisk {
			if errorCause(err) == nil {
				t.Fatalf("Test %d shouldn't succeed, md5sum = %s\n", i, md5sum)
			}
			switch i {
			case 1:
				if !isSameType(errorCause(err), BucketNotFound{}) {
					t.Fatal("Unexpected error ", err)
				}
			case 3:
			case 2, 4, 5, 6:
				if !isSameType(errorCause(err), InvalidUploadID{}) {
					t.Fatal("Unexpected error ", err)
				}
			default:
				t.Fatal("Unexpected error ", i, err, reflect.TypeOf(errorCause(err)), reflect.TypeOf(errFaultyDisk))
			}
		}
	}
}

// TestCompleteMultipartUploadFaultyDisk - test CompleteMultipartUpload with faulty disks
func TestCompleteMultipartUploadFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)

	fs := obj.(fsObjects)
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

	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 3; i++ {
		// Faulty disk generates errFaultyDisk at 'i' storage api call number
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if _, err := fs.CompleteMultipartUpload(bucketName, objectName, uploadID, parts); errorCause(err) != errFaultyDisk {
			switch i {
			case 1:
				if !isSameType(errorCause(err), BucketNotFound{}) {
					t.Fatal("Unexpected error ", err)
				}
			case 2:
				if !isSameType(errorCause(err), InvalidUploadID{}) {
					t.Fatal("Unexpected error ", err)
				}
			default:
				t.Fatal("Unexpected error ", i, err, reflect.TypeOf(errorCause(err)), reflect.TypeOf(errFaultyDisk))
			}
		}
	}
}

// TestListMultipartUploadsFaultyDisk - test ListMultipartUploads with faulty disks
func TestListMultipartUploadsFaultyDisk(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)
	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)
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

	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 4; i++ {
		// Faulty disk generates errFaultyDisk at 'i' storage api call number
		fs.storage = newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		if _, err := fs.ListMultipartUploads(bucketName, objectName, "", "", "", 1000); errorCause(err) != errFaultyDisk {
			switch i {
			case 1:
				if !isSameType(errorCause(err), BucketNotFound{}) {
					t.Fatal("Unexpected error ", err)
				}
			case 2:
				if !isSameType(errorCause(err), InvalidUploadID{}) {
					t.Fatal("Unexpected error ", err)
				}
			case 3:
				if errorCause(err) != errFileNotFound {
					t.Fatal("Unexpected error ", err)
				}
			default:
				t.Fatal("Unexpected error ", i, err, reflect.TypeOf(errorCause(err)), reflect.TypeOf(errFaultyDisk))
			}
		}
	}
}
