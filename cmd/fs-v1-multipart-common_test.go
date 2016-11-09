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
	"path/filepath"
	"testing"
)

// TestFSIsUploadExists - complete test with valid and invalid cases
func TestFSIsUploadExists(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(fsObjects)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	uploadID, err := obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
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

	removeAll(disk) // Remove disk.
	// isUploadIdExists with a disk not found. should return false
	if exists := fs.isUploadIDExists(bucketName, objectName, uploadID); exists {
		t.Fatal("Wrong result, expected: ", !exists)
	}
}

// TestFSWriteUploadJSON - tests for writeUploadJSON for FS
func TestFSWriteUploadJSON(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)

	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucket(bucketName)
	_, err := obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// newMultipartUpload will fail.
	removeAll(disk) // Remove disk.
	_, err = obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		if _, ok := errorCause(err).(BucketNotFound); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	}
}
