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
	"path/filepath"
	"testing"
)

// Tests healing of format XL.
func TestHealFormatXL(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(endpoints)
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Disks 0..15 are nil
	obj, _, err = initObjectLayer(endpoints)
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk returns Faulty Disk
	obj, _, err = initObjectLayer(endpoints)
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	xl.storageDisks[0] = nil
	if err = healFormatXL(xl.storageDisks); err != nil {
		t.Fatal("Got an unexpected error: ", err)
	}
	removeRoots(fsDirs)

	fsDirs, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json of all disks
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupted format json in one disk
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 15; i++ {
		if err = xl.storageDisks[i].AppendFile(".minio.sys", "format.json", []byte("corrupted data")); err != nil {
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json on 3 disks.
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
			t.Fatal(err)
		}
	}
	posixDisk, ok := xl.storageDisks[3].(*retryStorage)
	if !ok {
		t.Fatal("storage disk is not *retryStorage type")
	}
	xl.storageDisks[3] = newNaughtyDisk(posixDisk, nil, errDiskNotFound)
	expectedErr := fmt.Errorf("Unable to initialize format %s and %s", errSomeDiskOffline, errSomeDiskUnformatted)
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	xl = obj.(*xlObjects)
	if err = obj.MakeBucket(getRandomBucketName()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= 2; i++ {
		if err = xl.storageDisks[i].DeleteFile(".minio.sys", "format.json"); err != nil {
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
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json on 16 disks.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := getRandomBucketName()
	if err = obj.MakeBucket(bucketName); err != nil {
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
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Remove format.json on 16 disks.
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := getRandomBucketName()
	if err = obj.MakeBucket(bucketName); err != nil {
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
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

	endpoints, err = parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// One disk is not found, heal corrupted disks should return nil
	obj, _, err = initObjectLayer(endpoints)
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
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}

	// Create a bucket that won't get corrupted
	saneBucket := "sanebucket"
	if err = obj.MakeBucket(saneBucket); err != nil {
		t.Fatal(err)
	}

	// Create a bucket that will be removed in some disks
	corruptedBucketName := getRandomBucketName()
	if err = obj.MakeBucket(corruptedBucketName); err != nil {
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
	defer removeAll(root)

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatal(err)
	}

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(endpoints)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	data := []byte("hello")
	err = obj.MakeBucket(bucket)
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	_, err = obj.PutObject(bucket, object, int64(len(data)), bytes.NewReader(data), nil, "")
	if err != nil {
		t.Fatalf("Failed to put an object - %v", err)
	}

	// Remove the object backend files from the first disk.
	xl := obj.(*xlObjects)
	firstDisk := xl.storageDisks[0]
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	err = obj.HealObject(bucket, object)
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
	err = obj.HealObject(bucket, object)
	if errorCause(err) != errDiskNotFound {
		t.Errorf("Expected %v but received %v", errDiskNotFound, err)
	}
}
