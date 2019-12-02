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
	"path/filepath"
	"testing"

	"github.com/minio/minio/pkg/madmin"
)

// Tests undoes and validates if the undoing completes successfully.
func TestUndoMakeBucket(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// Remove format.json on 16 disks.
	obj, _, err := initObjectLayer(mustGetZoneEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucketName := getRandomBucketName()
	if err = obj.MakeBucketWithLocation(context.Background(), bucketName, ""); err != nil {
		t.Fatal(err)
	}
	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]
	undoMakeBucket(xl.getDisks(), bucketName)

	// Validate if bucket was deleted properly.
	_, err = obj.GetBucketInfo(context.Background(), bucketName)
	if err != nil {
		switch err.(type) {
		case BucketNotFound:
		default:
			t.Fatal(err)
		}
	}
}

func TestHealObjectCorrupted(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	objLayer, _, err := initObjectLayer(mustGetZoneEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = objLayer.MakeBucketWithLocation(context.Background(), bucket, "")
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	uploadID, err := objLayer.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := objLayer.PutObjectPart(context.Background(), bucket, object, uploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = objLayer.CompleteMultipartUpload(context.Background(), bucket, object, uploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Test 1: Remove the object backend files from the first disk.
	z := objLayer.(*xlZones)
	xl := z.zones[0].sets[0]
	firstDisk := xl.getDisks()[0]
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = objLayer.HealObject(context.Background(), bucket, object, false, false, madmin.HealNormalScan)
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	_, err = firstDisk.StatFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Errorf("Expected xl.json file to be present but stat failed - %v", err)
	}

	// Test 2: Heal when part.1 is empty
	partSt1, err := firstDisk.StatFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Expected part.1 file to be present but stat failed - %v", err)
	}
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Failure during deleting part.1 - %v", err)
	}
	err = firstDisk.WriteAll(bucket, filepath.Join(object, "part.1"), bytes.NewReader([]byte{}))
	if err != nil {
		t.Errorf("Failure during creating part.1 - %v", err)
	}
	_, err = objLayer.HealObject(context.Background(), bucket, object, false, true, madmin.HealDeepScan)
	if err != nil {
		t.Errorf("Expected nil but received %v", err)
	}
	partSt2, err := firstDisk.StatFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Expected from part.1 file to be present but stat failed - %v", err)
	}
	if partSt1.Size != partSt2.Size {
		t.Errorf("part.1 file size is not the same before and after heal")
	}

	// Test 3: Heal when part.1 is correct in size but corrupted
	partSt1, err = firstDisk.StatFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Expected part.1 file to be present but stat failed - %v", err)
	}
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Failure during deleting part.1 - %v", err)
	}
	bdata := bytes.Repeat([]byte("b"), int(partSt1.Size))
	err = firstDisk.WriteAll(bucket, filepath.Join(object, "part.1"), bytes.NewReader(bdata))
	if err != nil {
		t.Errorf("Failure during creating part.1 - %v", err)
	}
	_, err = objLayer.HealObject(context.Background(), bucket, object, false, true, madmin.HealDeepScan)
	if err != nil {
		t.Errorf("Expected nil but received %v", err)
	}
	partSt2, err = firstDisk.StatFile(bucket, filepath.Join(object, "part.1"))
	if err != nil {
		t.Errorf("Expected from part.1 file to be present but stat failed - %v", err)
	}
	if partSt1.Size != partSt2.Size {
		t.Errorf("part.1 file size is not the same before and after heal")
	}

	// Test 4: checks if HealObject returns an error when xl.json is not found
	// in more than read quorum number of disks, to create a corrupted situation.

	for i := 0; i <= len(xl.getDisks())/2; i++ {
		xl.getDisks()[i].DeleteFile(bucket, filepath.Join(object, xlMetaJSONFile))
	}

	// Try healing now, expect to receive errFileNotFound.
	_, err = objLayer.HealObject(context.Background(), bucket, object, false, true, madmin.HealDeepScan)
	if err != nil {
		if _, ok := err.(ObjectNotFound); !ok {
			t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
		}
	}

	// since majority of xl.jsons are not available, object should be successfully deleted.
	_, err = objLayer.GetObjectInfo(context.Background(), bucket, object, ObjectOptions{})
	if _, ok := err.(ObjectNotFound); !ok {
		t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
	}
}

// Tests healing of object.
func TestHealObjectXL(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetZoneEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = obj.MakeBucketWithLocation(context.Background(), bucket, "")
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	uploadID, err := obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := obj.PutObjectPart(context.Background(), bucket, object, uploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = obj.CompleteMultipartUpload(context.Background(), bucket, object, uploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Remove the object backend files from the first disk.
	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]
	firstDisk := xl.getDisks()[0]
	err = firstDisk.DeleteFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = obj.HealObject(context.Background(), bucket, object, false, false, madmin.HealNormalScan)
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	_, err = firstDisk.StatFile(bucket, filepath.Join(object, xlMetaJSONFile))
	if err != nil {
		t.Errorf("Expected xl.json file to be present but stat failed - %v", err)
	}

	xlDisks := xl.getDisks()
	xl.getDisks = func() []StorageAPI {
		// Nil more than half the disks, to remove write quorum.
		for i := 0; i <= len(xlDisks)/2; i++ {
			xlDisks[i] = nil
		}
		return xlDisks
	}

	// Try healing now, expect to receive errDiskNotFound.
	_, err = obj.HealObject(context.Background(), bucket, object, false, false, madmin.HealDeepScan)
	// since majority of xl.jsons are not available, object quorum can't be read properly and error will be errXLReadQuorum
	if _, ok := err.(InsufficientReadQuorum); !ok {
		t.Errorf("Expected %v but received %v", InsufficientReadQuorum{}, err)
	}
}

// Tests healing of empty directories
func TestHealEmptyDirectoryXL(t *testing.T) {
	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(mustGetZoneEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "empty-dir/"
	var opts ObjectOptions

	err = obj.MakeBucketWithLocation(context.Background(), bucket, "")
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Upload an empty directory
	_, err = obj.PutObject(context.Background(), bucket, object, mustGetPutObjReader(t,
		bytes.NewReader([]byte{}), 0, "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the object backend files from the first disk.
	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]
	firstDisk := xl.getDisks()[0]
	err = firstDisk.DeleteFile(bucket, object)
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	// Heal the object
	hr, err := obj.HealObject(context.Background(), bucket, object, false, false, madmin.HealNormalScan)
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	// Check if the empty directory is restored in the first disk
	_, err = firstDisk.StatVol(pathJoin(bucket, object))
	if err != nil {
		t.Fatalf("Expected object to be present but stat failed - %v", err)
	}

	// Check the state of the object in the first disk (should be missing)
	if hr.Before.Drives[0].State != madmin.DriveStateMissing {
		t.Fatalf("Unexpected drive state: %v", hr.Before.Drives[0].State)
	}

	// Check the state of all other disks (should be ok)
	for i, h := range append(hr.Before.Drives[1:], hr.After.Drives...) {
		if h.State != madmin.DriveStateOk {
			t.Fatalf("Unexpected drive state (%d): %v", i+1, h.State)
		}
	}

	// Heal the same object again
	hr, err = obj.HealObject(context.Background(), bucket, object, false, false, madmin.HealNormalScan)
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	// Check that Before & After states are all okay
	for i, h := range append(hr.Before.Drives, hr.After.Drives...) {
		if h.State != madmin.DriveStateOk {
			t.Fatalf("Unexpected drive state (%d): %v", i+1, h.State)
		}
	}
}
