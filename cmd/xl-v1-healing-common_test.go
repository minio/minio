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
func partsMetaFromModTimes(modTimes []time.Time) []xlMetaV1 {
	var partsMetadata []xlMetaV1
	for _, modTime := range modTimes {
		partsMetadata = append(partsMetadata, xlMetaV1{
			Stat: statInfo{
				ModTime: modTime,
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
	defer removeAll(rootPath)

	obj, disks, err := prepareXL()
	if err != nil {
		t.Fatalf("Prepare XL backend failed - %v", err)
	}
	defer removeRoots(disks)

	threeNanoSecs := time.Unix(0, 3).UTC()
	fourNanoSecs := time.Unix(0, 4).UTC()
	testCases := []struct {
		modTimes     []time.Time
		expectedTime time.Time
		errs         []error
	}{
		{
			modTimes: []time.Time{
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
				fourNanoSecs,
			},
			expectedTime: fourNanoSecs,
			errs: []error{
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			},
		},
		{
			modTimes: []time.Time{
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				threeNanoSecs,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
				timeSentinel,
			},
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.json.
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				// Majority of disks don't have xl.json.
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
				errDiskAccessDenied,
				errDiskNotFound,
				errFileNotFound,
				errFileNotFound,
			},
		},
	}

	xlDisks := obj.(*xlObjects).storageDisks
	for i, test := range testCases {
		partsMetadata := partsMetaFromModTimes(test.modTimes)

		onlineDisks, modTime := listOnlineDisks(xlDisks, partsMetadata, test.errs)
		outdatedDisks := outDatedDisks(xlDisks, onlineDisks, partsMetadata, test.errs)
		if modTime.Equal(timeSentinel) {
			t.Fatalf("Test %d: modTime should never be equal to timeSentinel, but found equal",
				i+1)
		}
		if !modTime.Equal(test.expectedTime) {
			t.Fatalf("Test %d: Expected modTime to be equal to %v but was found to be %v",
				i+1, test.expectedTime, modTime)
		}

		// Check if a disk is considered both online and outdated,
		// which is a contradiction.
		overlappingDisks := make(map[string]*posix)
		for _, onlineDisk := range onlineDisks {
			if onlineDisk == nil {
				continue
			}
			pDisk := toPosix(onlineDisk)
			overlappingDisks[pDisk.diskPath] = pDisk
		}

		for _, outdatedDisk := range outdatedDisks {
			if outdatedDisk == nil {
				continue
			}
			pDisk := toPosix(outdatedDisk)
			if _, ok := overlappingDisks[pDisk.diskPath]; ok {
				t.Errorf("Test %d: Outdated disk %v was also detected as an online disk",
					i+1, pDisk)
			}
		}

	}
}
