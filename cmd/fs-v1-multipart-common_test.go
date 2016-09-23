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
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFSIsBucketExist - complete test of isBucketExist
func TestFSIsBucketExist(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}

	fs := obj.(fsObjects)
	bucketName := "bucket"

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Cannot create bucket, err: ", err)
	}

	// Test with a valid bucket
	if found := fs.isBucketExist(bucketName); !found {
		t.Fatal("isBucketExist should true")
	}

	// Test with a inexistant bucket
	if found := fs.isBucketExist("foo"); found {
		t.Fatal("isBucketExist should false")
	}

	// Using a faulty disk
	fsStorage := fs.storage.(*posix)
	naughty := newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	fs.storage = naughty
	if found := fs.isBucketExist(bucketName); found {
		t.Fatal("isBucketExist should return false because it is wired to a corrupted disk")
	}
}

// TestFSIsUploadExists - complete test with valid and invalid cases
func TestFSIsUploadExists(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}

	fs := obj.(fsObjects)

	var uploadID string
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	uploadID, err = obj.NewMultipartUpload(bucketName, objectName, nil)

	if err != nil {
		t.Fatal("Cannot create a new FS object: ", err)
	}
	// Test with valid upload id
	if exists := fs.isUploadIDExists(bucketName, objectName, uploadID); !exists {
		t.Fatal("Wrong result, expected: ", exists)
	}

	// Test with inexistant bucket/object names
	if exists := fs.isUploadIDExists("bucketfoo", "objectfoo", uploadID); exists {
		t.Fatal("Wrong result, expected: ", !exists)
	}

	// Test with inexistant upload ID
	if exists := fs.isUploadIDExists(bucketName, objectName, uploadID+"-ff"); exists {
		t.Fatal("Wrong result, expected: ", !exists)
	}

	// isUploadIdExists with a faulty disk should return false
	fsStorage := fs.storage.(*posix)
	naughty := newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	fs.storage = naughty
	if exists := fs.isUploadIDExists(bucketName, objectName, uploadID); exists {
		t.Fatal("Wrong result, expected: ", !exists)
	}
}

// TestFSWriteUploadJSON - tests for writeUploadJSON for FS
func TestFSWriteUploadJSON(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	fs := obj.(fsObjects)

	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	uploadID, err := obj.NewMultipartUpload(bucketName, objectName, nil)

	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	if err := fs.writeUploadJSON(bucketName, objectName, uploadID, time.Now().UTC()); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// isUploadIdExists with a faulty disk should return false
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 3; i++ {
		naughty := newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		fs.storage = naughty
		if err := fs.writeUploadJSON(bucketName, objectName, uploadID, time.Now().UTC()); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected err: ", err)
		}
	}
}

// TestFSUpdateUploadsJSON - tests for updateUploadsJSON for FS
func TestFSUpdateUploadsJSON(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	fs := obj.(fsObjects)

	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)

	if err := fs.updateUploadsJSON(bucketName, objectName, uploadsV1{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// isUploadIdExists with a faulty disk should return false
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 2; i++ {
		naughty := newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk}, nil)
		fs.storage = naughty
		if err := fs.updateUploadsJSON(bucketName, objectName, uploadsV1{}); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected err: ", err)
		}
	}
}
