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
	"time"
)

// validates functionality provided to find most common
// time occurrence from a list of time.
func TestCommonTime(t *testing.T) {
	// List of test cases for common modTime.
	testCases := []struct {
		times []time.Time
		time  time.Time
	}{
		{
			// 1. Tests common times when slice has varying time elements.
			[]time.Time{
				time.Unix(0, 1).UTC(),
				time.Unix(0, 2).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 2).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 1).UTC(),
			}, time.Unix(0, 3).UTC(),
		},
		{
			// 2. Tests common time obtained when all elements are equal.
			[]time.Time{
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
			}, time.Unix(0, 3).UTC(),
		},
		{
			// 3. Tests common time obtained when elements have a mixture
			// of sentinel values.
			[]time.Time{
				time.Unix(0, 3).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 2).UTC(),
				time.Unix(0, 1).UTC(),
				time.Unix(0, 3).UTC(),
				time.Unix(0, 4).UTC(),
				time.Unix(0, 3).UTC(),
				timeSentinel,
				timeSentinel,
				timeSentinel,
			}, time.Unix(0, 3).UTC(),
		},
	}

	// Tests all the testcases, and validates them against expected
	// common modtime. Tests fail if modtime does not match.
	for i, testCase := range testCases {
		// Obtain a common mod time from modTimes slice.
		ctime, _ := commonTime(testCase.times)
		if testCase.time != ctime {
			t.Fatalf("Test case %d, expect to pass but failed. Wanted modTime: %s, got modTime: %s\n", i+1, testCase.time, ctime)
		}
	}
}

// partsMetaFromModTimes - returns slice of modTimes given metadata of
// an object part.
func partsMetaFromModTimes(modTimes []time.Time, algorithm BitrotAlgorithm, checksums []ChecksumInfo) []xlMetaV1 {
	var partsMetadata []xlMetaV1
	for _, modTime := range modTimes {
		partsMetadata = append(partsMetadata, xlMetaV1{
			Erasure: ErasureInfo{
				Checksums: checksums,
			},
			Stat: statInfo{
				ModTime: modTime,
			},
			Parts: []objectPartInfo{
				{
					Name: "part.1",
				},
			},
		})
	}
	return partsMetadata
}

// toPosix - fetches *posix object from StorageAPI.
func toPosix(disk StorageAPI) *posix {
	retryDisk, ok := disk.(*retryStorage)
	if !ok {
		return nil
	}
	pDisk, ok := retryDisk.remoteStorage.(*posix)
	if !ok {
		return nil
	}
	return pDisk

}

// TestListOnlineDisks - checks if listOnlineDisks and outDatedDisks
// are consistent with each other.
func TestListOnlineDisks(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Failed to initialize config - %v", err)
	}
	defer os.RemoveAll(rootPath)

	obj, disks, err := prepareXL16()
	if err != nil {
		t.Fatalf("Prepare XL backend failed - %v", err)
	}
	defer removeRoots(disks)

	type tamperKind int
	const (
		noTamper    tamperKind = iota
		deletePart  tamperKind = iota
		corruptPart tamperKind = iota
	)
	threeNanoSecs := time.Unix(0, 3).UTC()
	fourNanoSecs := time.Unix(0, 4).UTC()
	modTimesThreeNone := []time.Time{
		threeNanoSecs, threeNanoSecs, threeNanoSecs, threeNanoSecs,
		threeNanoSecs, threeNanoSecs, threeNanoSecs,
		timeSentinel, timeSentinel, timeSentinel, timeSentinel,
		timeSentinel, timeSentinel, timeSentinel, timeSentinel,
		timeSentinel,
	}
	modTimesThreeFour := []time.Time{
		threeNanoSecs, threeNanoSecs, threeNanoSecs, threeNanoSecs,
		threeNanoSecs, threeNanoSecs, threeNanoSecs, threeNanoSecs,
		fourNanoSecs, fourNanoSecs, fourNanoSecs, fourNanoSecs,
		fourNanoSecs, fourNanoSecs, fourNanoSecs, fourNanoSecs,
	}
	testCases := []struct {
		modTimes       []time.Time
		expectedTime   time.Time
		errs           []error
		_tamperBackend tamperKind
	}{
		{
			modTimes:     modTimesThreeFour,
			expectedTime: fourNanoSecs,
			errs: []error{
				nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil,
			},
			_tamperBackend: noTamper,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.json.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.json.
				errFileNotFound, errFileNotFound,
				errFileNotFound, errFileNotFound,
				errFileNotFound, errDiskAccessDenied,
				errDiskNotFound, errFileNotFound,
				errFileNotFound,
			},
			_tamperBackend: deletePart,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.json.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.json.
				errFileNotFound, errFileNotFound,
				errFileNotFound, errFileNotFound,
				errFileNotFound, errDiskAccessDenied,
				errDiskNotFound, errFileNotFound,
				errFileNotFound,
			},
			_tamperBackend: corruptPart,
		},
	}

	bucket := "bucket"
	object := "object"
	data := bytes.Repeat([]byte("a"), 1024)
	xlDisks := obj.(*xlObjects).storageDisks
	for i, test := range testCases {
		// Prepare bucket/object backend for the tests below.

		// Cleanup from previous test.
		obj.DeleteObject(bucket, object)
		obj.DeleteBucket(bucket)

		err = obj.MakeBucketWithLocation("bucket", "")
		if err != nil {
			t.Fatalf("Failed to make a bucket %v", err)
		}

		_, err = obj.PutObject(bucket, object, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), nil)
		if err != nil {
			t.Fatalf("Failed to putObject %v", err)
		}

		// Fetch xl.json from first disk to construct partsMetadata for the tests.
		xlMeta, err := readXLMeta(xlDisks[0], bucket, object)
		if err != nil {
			t.Fatalf("Test %d: Failed to read xl.json %v", i+1, err)
		}

		tamperedIndex := -1
		switch test._tamperBackend {
		case deletePart:
			for index, err := range test.errs {
				if err != nil {
					continue
				}
				// Remove a part from a disk
				// which has a valid xl.json,
				// and check if that disk
				// appears in outDatedDisks.
				tamperedIndex = index
				dErr := xlDisks[index].DeleteFile(bucket, filepath.Join(object, "part.1"))
				if dErr != nil {
					t.Fatalf("Test %d: Failed to delete %s - %v", i+1,
						filepath.Join(object, "part.1"), dErr)
				}
				break
			}
		case corruptPart:
			for index, err := range test.errs {
				if err != nil {
					continue
				}
				// Corrupt a part from a disk
				// which has a valid xl.json,
				// and check if that disk
				// appears in outDatedDisks.
				tamperedIndex = index
				dErr := xlDisks[index].AppendFile(bucket, filepath.Join(object, "part.1"), []byte("corruption"))
				if dErr != nil {
					t.Fatalf("Test %d: Failed to append corrupting data at the end of file %s - %v",
						i+1, filepath.Join(object, "part.1"), dErr)
				}
				break
			}

		}

		partsMetadata := partsMetaFromModTimes(test.modTimes, DefaultBitrotAlgorithm, xlMeta.Erasure.Checksums)

		onlineDisks, modTime := listOnlineDisks(xlDisks, partsMetadata, test.errs)
		availableDisks, newErrs, _ := disksWithAllParts(onlineDisks, partsMetadata, test.errs, bucket, object)
		test.errs = newErrs
		outdatedDisks := outDatedDisks(xlDisks, availableDisks, test.errs, partsMetadata, bucket, object)
		if modTime.Equal(timeSentinel) {
			t.Fatalf("Test %d: modTime should never be equal to timeSentinel, but found equal",
				i+1)
		}

		if test._tamperBackend != noTamper {
			if tamperedIndex != -1 && outdatedDisks[tamperedIndex] == nil {
				t.Fatalf("Test %d: disk (%v) with part.1 missing is an outdated disk, but wasn't listed by outDatedDisks",
					i+1, xlDisks[tamperedIndex])
			}

		}

		if !modTime.Equal(test.expectedTime) {
			t.Fatalf("Test %d: Expected modTime to be equal to %v but was found to be %v",
				i+1, test.expectedTime, modTime)
		}

		// Check if a disk is considered both online and outdated,
		// which is a contradiction, except if parts are missing.
		overlappingDisks := make(map[string]*posix)
		for _, availableDisk := range availableDisks {
			if availableDisk == nil {
				continue
			}
			pDisk := toPosix(availableDisk)
			overlappingDisks[pDisk.diskPath] = pDisk
		}

		for index, outdatedDisk := range outdatedDisks {
			// ignore the intentionally tampered disk,
			// this is expected to appear as outdated
			// disk, since it doesn't have all the parts.
			if index == tamperedIndex {
				continue
			}

			if outdatedDisk == nil {
				continue
			}

			pDisk := toPosix(outdatedDisk)
			if _, ok := overlappingDisks[pDisk.diskPath]; ok {
				t.Errorf("Test %d: Outdated disk %v was also detected as an online disk - %v %v",
					i+1, pDisk, availableDisks, outdatedDisks)
			}

			// errors other than errFileNotFound doesn't imply that the disk is outdated.
			if test.errs[index] != nil && test.errs[index] != errFileNotFound && outdatedDisk != nil {
				t.Errorf("Test %d: error (%v) other than errFileNotFound doesn't imply that the disk (%v) could be outdated",
					i+1, test.errs[index], pDisk)
			}
		}
	}
}

func TestDisksWithAllParts(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Failed to initialize config - %v", err)
	}
	defer os.RemoveAll(rootPath)

	obj, disks, err := prepareXL16()
	if err != nil {
		t.Fatalf("Prepare XL backend failed - %v", err)
	}
	defer removeRoots(disks)

	bucket := "bucket"
	object := "object"
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), int(globalPutPartSize)*partCount)
	xl := obj.(*xlObjects)
	xlDisks := xl.storageDisks

	err = obj.MakeBucketWithLocation("bucket", "")
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	_, err = obj.PutObject(bucket, object, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""), nil)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	partsMetadata, errs := readAllXLMetadata(xlDisks, bucket, object)
	readQuorum := len(xl.storageDisks) / 2
	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		t.Fatalf("Failed to read xl meta data %v", reducedErr)
	}

	diskFailures := make(map[int]string)
	// key = disk index, value = part name with hash mismatch
	diskFailures[0] = "part.3"
	diskFailures[3] = "part.1"
	diskFailures[15] = "part.2"

	for diskIndex, partName := range diskFailures {
		for index, info := range partsMetadata[diskIndex].Erasure.Checksums {
			if info.Name == partName {
				partsMetadata[diskIndex].Erasure.Checksums[index].Hash[0]++
			}
		}
	}

	errs = make([]error, len(xlDisks))
	filteredDisks, errs, err := disksWithAllParts(xlDisks, partsMetadata, errs, bucket, object)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if len(filteredDisks) != len(xlDisks) {
		t.Errorf("Unexpected number of disks: %d", len(filteredDisks))
	}

	for diskIndex, disk := range filteredDisks {
		if _, ok := diskFailures[diskIndex]; ok {
			if disk != nil {
				t.Errorf("Disk not filtered as expected, disk: %d", diskIndex)
			}
			if errs[diskIndex] == nil {
				t.Errorf("Expected error not received, diskIndex: %d", diskIndex)
			}
		} else {
			if disk == nil {
				t.Errorf("Disk erroneously filtered, diskIndex: %d", diskIndex)
			}
			if errs[diskIndex] != nil {
				t.Errorf("Unexpected error, %s, diskIndex: %d", errs[diskIndex], diskIndex)
			}

		}
	}

	// Test that all disks are returned without any failures with unmodified
	// meta data
	partsMetadata, errs = readAllXLMetadata(xlDisks, bucket, object)
	if err != nil {
		t.Fatalf("Failed to read xl meta data %v", err)
	}

	filteredDisks, errs, err = disksWithAllParts(xlDisks, partsMetadata, errs, bucket, object)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if len(filteredDisks) != len(xlDisks) {
		t.Errorf("Unexpected number of disks: %d", len(filteredDisks))
	}

	for diskIndex, disk := range filteredDisks {
		if errs[diskIndex] != nil {
			t.Errorf("Unexpected error %s", errs[diskIndex])
		}

		if disk == nil {
			t.Errorf("Disk erroneously filtered, diskIndex: %d", diskIndex)
		}
	}

}
