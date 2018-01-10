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
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio/pkg/errors"
)

// Tests undoes and validates if the undoing completes successfully.
func TestUndoMakeBucket(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// Remove format.json on 16 disks.
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucketName := getRandomBucketName()
	if err = obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)
	undoMakeBucket(xl.storageDisks, bucketName)

	// Validate if bucket was deleted properly.
	_, err = obj.GetBucketInfo(bucketName)
	if err != nil {
		err = errors.Cause(err)
		switch err.(type) {
		case BucketNotFound:
		default:
			t.Fatal(err)
		}
	}
}

// Tests healing of object.
func TestHealObjectXL(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	data := bytes.Repeat([]byte("a"), 5*1024*1024)

	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	uploadID, err := obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := obj.PutObjectPart(bucket, object, uploadID, partID, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""))
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = obj.CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Remove the object backend files from the first disk.
	xl := obj.(*xlObjects)
	firstDisk := xl.storageDisks[0]
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = obj.HealObject(bucket, object, false)
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	_, err = firstDisk.StatFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Errorf("Expected xl.json file to be present but stat failed - %v", err)
	}

	// Nil more than half the disks, to remove write quorum.
	for i := 0; i <= len(xl.storageDisks)/2; i++ {
		xl.storageDisks[i] = nil
	}

	// Try healing now, expect to receive errDiskNotFound.
	_, err = obj.HealObject(bucket, object, false)
	// since majority of xl.jsons are not available, object quorum can't be read properly and error will be errXLReadQuorum
	if errors.Cause(err) != errXLReadQuorum {
		t.Errorf("Expected %v but received %v", errDiskNotFound, err)
	}
}
