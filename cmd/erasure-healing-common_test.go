// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/madmin-go"
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
		ctime, _ := commonTime(testCase.times, nil)
		if !testCase.time.Equal(ctime) {
			t.Fatalf("Test case %d, expect to pass but failed. Wanted modTime: %s, got modTime: %s\n", i+1, testCase.time, ctime)
		}
	}
}

// TestListOnlineDisks - checks if listOnlineDisks and outDatedDisks
// are consistent with each other.
func TestListOnlineDisks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	defer obj.Shutdown(context.Background())
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
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.meta.
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
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.meta.
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
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	object := "object"
	data := bytes.Repeat([]byte("a"), smallFileThreshold*16)
	z := obj.(*erasureServerPools)
	erasureDisks := z.serverPools[0].sets[0].getDisks()
	for i, test := range testCases {
		test := test
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("Failed to putObject %v", err)
			}

			partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
			fi, err := getLatestFileInfo(ctx, partsMetadata, errs)
			if err != nil {
				t.Fatalf("Failed to getLatestFileInfo %v", err)
			}

			for j := range partsMetadata {
				if errs[j] != nil {
					t.Fatalf("expected error to be nil: %s", errs[j])
				}
				partsMetadata[j].ModTime = test.modTimes[j]
			}

			tamperedIndex := -1
			switch test._tamperBackend {
			case deletePart:
				for index, err := range test.errs {
					if err != nil {
						continue
					}
					// Remove a part from a disk
					// which has a valid xl.meta,
					// and check if that disk
					// appears in outDatedDisks.
					tamperedIndex = index
					dErr := erasureDisks[index].Delete(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), false)
					if dErr != nil {
						t.Fatalf("Failed to delete %s - %v", filepath.Join(object, "part.1"), dErr)
					}
					break
				}
			case corruptPart:
				for index, err := range test.errs {
					if err != nil {
						continue
					}
					// Corrupt a part from a disk
					// which has a valid xl.meta,
					// and check if that disk
					// appears in outDatedDisks.
					tamperedIndex = index
					filePath := pathJoin(erasureDisks[index].String(), bucket, object, fi.DataDir, "part.1")
					f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0)
					if err != nil {
						t.Fatalf("Failed to open %s: %s\n", filePath, err)
					}
					f.Write([]byte("oops")) // Will cause bitrot error
					f.Close()
					break
				}

			}

			onlineDisks, modTime, dataDir := listOnlineDisks(erasureDisks, partsMetadata, test.errs)
			if !modTime.Equal(test.expectedTime) {
				t.Fatalf("Expected modTime to be equal to %v but was found to be %v",
					test.expectedTime, modTime)
			}
			if fi.DataDir != dataDir {
				t.Fatalf("Expected dataDir to be equal to %v but was found to be %v",
					fi.DataDir, dataDir)
			}
			availableDisks, newErrs := disksWithAllParts(ctx, onlineDisks, partsMetadata, test.errs, bucket, object, madmin.HealDeepScan)
			test.errs = newErrs

			if test._tamperBackend != noTamper {
				if tamperedIndex != -1 && availableDisks[tamperedIndex] != nil {
					t.Fatalf("disk (%v) with part.1 missing is not a disk with available data",
						erasureDisks[tamperedIndex])
				}
			}
		})
	}
}

// TestListOnlineDisksSmallObjects - checks if listOnlineDisks and outDatedDisks
// are consistent with each other.
func TestListOnlineDisksSmallObjects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(disks)

	type tamperKind int
	const (
		noTamper    tamperKind = iota
		deletePart  tamperKind = iota
		corruptPart tamperKind = iota
	)
	timeSentinel := time.Unix(1, 0).UTC()
	threeNanoSecs := time.Unix(3, 0).UTC()
	fourNanoSecs := time.Unix(4, 0).UTC()
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
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.meta.
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
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil,
				// Majority of disks don't have xl.meta.
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
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	object := "object"
	data := bytes.Repeat([]byte("a"), smallFileThreshold/2)
	z := obj.(*erasureServerPools)
	erasureDisks := z.serverPools[0].sets[0].getDisks()
	for i, test := range testCases {
		test := test
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("Failed to putObject %v", err)
			}

			partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", true)
			fi, err := getLatestFileInfo(ctx, partsMetadata, errs)
			if err != nil {
				t.Fatalf("Failed to getLatestFileInfo %v", err)
			}

			for j := range partsMetadata {
				if errs[j] != nil {
					t.Fatalf("expected error to be nil: %s", errs[j])
				}
				partsMetadata[j].ModTime = test.modTimes[j]
			}

			if erasureDisks, err = writeUniqueFileInfo(ctx, erasureDisks, bucket, object, partsMetadata, diskCount(erasureDisks)); err != nil {
				t.Fatal(ctx, err)
			}

			tamperedIndex := -1
			switch test._tamperBackend {
			case deletePart:
				for index, err := range test.errs {
					if err != nil {
						continue
					}
					// Remove a part from a disk
					// which has a valid xl.meta,
					// and check if that disk
					// appears in outDatedDisks.
					tamperedIndex = index
					dErr := erasureDisks[index].Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), false)
					if dErr != nil {
						t.Fatalf("Failed to delete %s - %v", pathJoin(object, xlStorageFormatFile), dErr)
					}
					break
				}
			case corruptPart:
				for index, err := range test.errs {
					if err != nil {
						continue
					}
					// Corrupt a part from a disk
					// which has a valid xl.meta,
					// and check if that disk
					// appears in outDatedDisks.
					tamperedIndex = index
					filePath := pathJoin(erasureDisks[index].String(), bucket, object, xlStorageFormatFile)
					f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0)
					if err != nil {
						t.Fatalf("Failed to open %s: %s\n", filePath, err)
					}
					f.Write([]byte("oops")) // Will cause bitrot error
					f.Close()
					break
				}

			}
			partsMetadata, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", true)
			_, err = getLatestFileInfo(ctx, partsMetadata, errs)
			if err != nil {
				t.Fatalf("Failed to getLatestFileInfo %v", err)
			}

			onlineDisks, modTime, dataDir := listOnlineDisks(erasureDisks, partsMetadata, test.errs)
			if !modTime.Equal(test.expectedTime) {
				t.Fatalf("Expected modTime to be equal to %v but was found to be %v",
					test.expectedTime, modTime)
			}

			if fi.DataDir != dataDir {
				t.Fatalf("Expected dataDir to be equal to %v but was found to be %v",
					fi.DataDir, dataDir)
			}

			availableDisks, newErrs := disksWithAllParts(ctx, onlineDisks, partsMetadata, test.errs, bucket, object, madmin.HealDeepScan)
			test.errs = newErrs

			if test._tamperBackend != noTamper {
				if tamperedIndex != -1 && availableDisks[tamperedIndex] != nil {
					t.Fatalf("disk (%v) with part.1 missing is not a disk with available data",
						erasureDisks[tamperedIndex])
				}
			}
		})
	}
}

func TestDisksWithAllParts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(disks)

	bucket := "bucket"
	object := "object"
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), 6*1024*1024*partCount)
	z := obj.(*erasureServerPools)
	s := z.serverPools[0].sets[0]
	erasureDisks := s.getDisks()
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	_, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	readQuorum := len(erasureDisks) / 2
	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		t.Fatalf("Failed to read xl meta data %v", reducedErr)
	}

	// Test 1: Test that all disks are returned without any failures with
	// unmodified meta data
	partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	if err != nil {
		t.Fatalf("Failed to read xl meta data %v", err)
	}

	erasureDisks, _, _ = listOnlineDisks(erasureDisks, partsMetadata, errs)

	filteredDisks, errs := disksWithAllParts(ctx, erasureDisks, partsMetadata, errs, bucket, object, madmin.HealDeepScan)

	if len(filteredDisks) != len(erasureDisks) {
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

	// Test 2: Not synchronized modtime
	partsMetadataBackup := partsMetadata[0]
	partsMetadata[0].ModTime = partsMetadata[0].ModTime.Add(-1 * time.Hour)

	errs = make([]error, len(erasureDisks))
	filteredDisks, _ = disksWithAllParts(ctx, erasureDisks, partsMetadata, errs, bucket, object, madmin.HealDeepScan)

	if len(filteredDisks) != len(erasureDisks) {
		t.Errorf("Unexpected number of disks: %d", len(filteredDisks))
	}
	for diskIndex, disk := range filteredDisks {
		if diskIndex == 0 && disk != nil {
			t.Errorf("Disk not filtered as expected, disk: %d", diskIndex)
		}
		if diskIndex != 0 && disk == nil {
			t.Errorf("Disk erroneously filtered, diskIndex: %d", diskIndex)
		}
	}
	partsMetadata[0] = partsMetadataBackup // Revert before going to the next test

	// Test 3: Not synchronized DataDir
	partsMetadataBackup = partsMetadata[1]
	partsMetadata[1].DataDir = "foo-random"

	errs = make([]error, len(erasureDisks))
	filteredDisks, _ = disksWithAllParts(ctx, erasureDisks, partsMetadata, errs, bucket, object, madmin.HealDeepScan)

	if len(filteredDisks) != len(erasureDisks) {
		t.Errorf("Unexpected number of disks: %d", len(filteredDisks))
	}
	for diskIndex, disk := range filteredDisks {
		if diskIndex == 1 && disk != nil {
			t.Errorf("Disk not filtered as expected, disk: %d", diskIndex)
		}
		if diskIndex != 1 && disk == nil {
			t.Errorf("Disk erroneously filtered, diskIndex: %d", diskIndex)
		}
	}
	partsMetadata[1] = partsMetadataBackup // Revert before going to the next test

	// Test 4: key = disk index, value = part name with hash mismatch
	diskFailures := make(map[int]string)
	diskFailures[0] = "part.1"
	diskFailures[3] = "part.1"
	diskFailures[15] = "part.1"

	for diskIndex, partName := range diskFailures {
		for i := range partsMetadata[diskIndex].Erasure.Checksums {
			if fmt.Sprintf("part.%d", i+1) == partName {
				filePath := pathJoin(erasureDisks[diskIndex].String(), bucket, object, partsMetadata[diskIndex].DataDir, partName)
				f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0)
				if err != nil {
					t.Fatalf("Failed to open %s: %s\n", filePath, err)
				}
				f.Write([]byte("oops")) // Will cause bitrot error
				f.Close()
			}
		}
	}

	errs = make([]error, len(erasureDisks))
	filteredDisks, errs = disksWithAllParts(ctx, erasureDisks, partsMetadata, errs, bucket, object, madmin.HealDeepScan)

	if len(filteredDisks) != len(erasureDisks) {
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
}
