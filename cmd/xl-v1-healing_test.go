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
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Tests healing of format XL.
func TestHealFormatXL(t *testing.T) {
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

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl := obj.(*xlObjects)
	if err = healFormatXL(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}

	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Disks 0..15 are nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		xl.storageDisks[i] = nil
	}

	if err = healFormatXL(xl.storageDisks); err != errXLReadQuorum {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := range xl.storageDisks {
		posixDisk, ok := xl.storageDisks[i].(*retryStorage)
		if !ok {
			t.Fatal("storage disk is not *retryStorage type")
		}
		xl.storageDisks[i] = newNaughtyDisk(posixDisk, nil, errDiskFull)
	}
	if err = healFormatXL(xl.storageDisks); err != errXLReadQuorum {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return
	// error for offline disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	if err = healFormatXL(xl.storageDisks); err != nil && err.Error() != "cannot proceed with heal as some disks are offline" {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json of all disks
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXL(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupted format json in one disk
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].AppendFile(minioMetaBucket, formatConfigFile, []byte("corrupted data")); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXL(xl.storageDisks); err == nil {
		t.Fatal("Should get a json parsing error, ")
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json on 3 disks.
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXL(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			t.Fatal(err)
		}
	}
	posixDisk, ok := xl.storageDisks[3].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[3] = newNaughtyDisk(posixDisk, nil, errDiskNotFound)
	expectedErr := fmt.Errorf("cannot proceed with heal as %s", errSomeDiskOffline)
	if err = healFormatXL(xl.storageDisks); err != nil {
		if err.Error() != expectedErr.Error() {
			t.Fatal("Got an unexpected error: ", err)
		}
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk has access denied error, heal should return
	// appropriate error
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			t.Fatal(err)
		}
	}
	posixDisk, ok = xl.storageDisks[3].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[3] = newNaughtyDisk(posixDisk, nil, errDiskAccessDenied)
	expectedErr = fmt.Errorf("cannot proceed with heal as some disks had unhandled errors")
	if err = healFormatXL(xl.storageDisks); err != nil {
		if err.Error() != expectedErr.Error() {
			t.Fatal("Got an unexpected error: ", err)
		}
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	if err = obj.MakeBucketWithLocation(getRandomBucketName(), ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(minioMetaBucket, formatConfigFile); err != nil {
			t.Fatal(err)
		}
	}
	if err = healFormatXL(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)
}

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
		err = errorCause(err)
		switch err.(type) {
		case BucketNotFound:
		default:
			t.Fatal(err)
		}
	}
}

// Tests quick healing of bucket and bucket metadata.
func TestQuickHeal(t *testing.T) {
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
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteVol(bucketName); err != nil {
			t.Fatal(err)
		}
	}

	// Heal the missing buckets.
	if err = quickHeal(xl.storageDisks, xl.writeQuorum, xl.readQuorum); err != nil {
		t.Fatal(err)
	}

	// Validate if buckets were indeed healed.
	for i := 0; i <= 2; i++ {
		if _, err = xl.storageDisks[i].StatVol(bucketName); err != nil {
			t.Fatal(err)
		}
	}

	// Corrupt one of the disks to return unformatted disk.
	posixDisk, ok := xl.storageDisks[0].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errUnformattedDisk)
	if err = quickHeal(xl.storageDisks, xl.writeQuorum, xl.readQuorum); err != errUnformattedDisk {
		t.Fatal(err)
	}

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	if err = quickHeal(xl.storageDisks, xl.writeQuorum, xl.readQuorum); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	// Corrupt one of the disks to return unformatted disk.
	posixDisk, ok = xl.storageDisks[0].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[0] = newNaughtyDisk(posixDisk, nil, errDiskNotFound)
	if err = quickHeal(xl.storageDisks, xl.writeQuorum, xl.readQuorum); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
}

// TestListBucketsHeal lists buckets heal result
func TestListBucketsHeal(t *testing.T) {
	root, err := newTestConfig("us-east-1")
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

	obj, _, err := initObjectLayer(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	// Create a bucket that won't get corrupted
	saneBucket := "sanebucket"
	if err = obj.MakeBucketWithLocation(saneBucket, ""); err != nil {
		t.Fatal(err)
	}

	// Create a bucket that will be removed in some disks
	corruptedBucketName := getRandomBucketName()
	if err = obj.MakeBucketWithLocation(corruptedBucketName, ""); err != nil {
		t.Fatal(err)
	}

	xl := obj.(*xlObjects)

	// Remove bucket in disk 0, 1 and 2
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteVol(corruptedBucketName); err != nil {
			t.Fatal(err)
		}
	}

	// List the missing buckets.
	buckets, err := xl.ListBucketsHeal()
	if err != nil {
		t.Fatal(err)
	}

	// Check the number of buckets in list buckets heal result
	if len(buckets) != 1 {
		t.Fatalf("Length of missing buckets is incorrect, expected: 1, found: %d", len(buckets))
	}

	// Check the name of bucket in list buckets heal result
	if buckets[0].Name != corruptedBucketName {
		t.Fatalf("Name of missing bucket is incorrect, expected: %s, found: %s", corruptedBucketName, buckets[0].Name)
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

	var uploadedParts []completePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := obj.PutObjectPart(bucket, object, uploadID, partID, NewHashReader(bytes.NewReader(data), int64(len(data)), "", ""))
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, completePart{
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

	_, _, err = obj.HealObject(bucket, object)
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
	_, _, err = obj.HealObject(bucket, object)
	if errorCause(err) != errDiskNotFound {
		t.Errorf("Expected %v but received %v", errDiskNotFound, err)
	}
}
