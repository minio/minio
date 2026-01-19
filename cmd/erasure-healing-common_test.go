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
	"runtime"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

// Returns the latest updated FileInfo files and error in case of failure.
func getLatestFileInfo(ctx context.Context, partsMetadata []FileInfo, defaultParityCount int, errs []error) (FileInfo, error) {
	// There should be at least half correct entries, if not return failure
	expectedRQuorum := len(partsMetadata) / 2
	if defaultParityCount == 0 {
		// if parity count is '0', we expected all entries to be present.
		expectedRQuorum = len(partsMetadata)
	}

	reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, expectedRQuorum)
	if reducedErr != nil {
		return FileInfo{}, reducedErr
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Count all latest updated FileInfo values
	var count int
	var latestFileInfo FileInfo

	// Reduce list of UUIDs to a single common value - i.e. the last updated Time
	modTime := commonTime(modTimes, expectedRQuorum)

	if modTime.IsZero() || modTime.Equal(timeSentinel) {
		return FileInfo{}, errErasureReadQuorum
	}

	// Iterate through all the modTimes and count the FileInfo(s) with latest time.
	for index, t := range modTimes {
		if partsMetadata[index].IsValid() && t.Equal(modTime) {
			latestFileInfo = partsMetadata[index]
			count++
		}
	}

	if !latestFileInfo.IsValid() {
		return FileInfo{}, errErasureReadQuorum
	}

	if count < latestFileInfo.Erasure.DataBlocks {
		return FileInfo{}, errErasureReadQuorum
	}

	return latestFileInfo, nil
}

// validates functionality provided to find most common
// time occurrence from a list of time.
func TestCommonTime(t *testing.T) {
	// List of test cases for common modTime.
	testCases := []struct {
		times  []time.Time
		time   time.Time
		quorum int
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
			},
			time.Unix(0, 3).UTC(),
			3,
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
			},
			time.Unix(0, 3).UTC(),
			4,
		},
		{
			// 3. Tests common time obtained when elements have a mixture of
			// sentinel values and don't have read quorum on any of the values.
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
			},
			timeSentinel,
			5,
		},
	}

	// Tests all the testcases, and validates them against expected
	// common modtime. Tests fail if modtime does not match.
	for i, testCase := range testCases {
		// Obtain a common mod time from modTimes slice.
		ctime := commonTime(testCase.times, testCase.quorum)
		if !testCase.time.Equal(ctime) {
			t.Errorf("Test case %d, expect to pass but failed. Wanted modTime: %s, got modTime: %s\n", i+1, testCase.time, ctime)
		}
	}
}

// TestListOnlineDisks - checks if listOnlineDisks and outDatedDisks
// are consistent with each other.
func TestListOnlineDisks(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		t.Skip()
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	setObjectLayer(obj)
	defer obj.Shutdown(t.Context())
	defer removeRoots(disks)

	type tamperKind int
	const (
		noTamper tamperKind = iota
		deletePart
		corruptPart
	)

	timeSentinel := time.Unix(1, 0).UTC()
	threeNanoSecs := time.Unix(3, 0).UTC()
	fourNanoSecs := time.Unix(4, 0).UTC()
	modTimesThreeNone := make([]time.Time, 16)
	modTimesThreeFour := make([]time.Time, 16)
	for i := range 16 {
		// Have 13 good xl.meta, 12 for default parity count = 4 (EC:4) and one
		// to be tampered with.
		if i > 12 {
			modTimesThreeFour[i] = fourNanoSecs
			modTimesThreeNone[i] = timeSentinel
			continue
		}
		modTimesThreeFour[i] = threeNanoSecs
		modTimesThreeNone[i] = threeNanoSecs
	}

	testCases := []struct {
		modTimes       []time.Time
		expectedTime   time.Time
		errs           []error
		_tamperBackend tamperKind
	}{
		{
			modTimes:     modTimesThreeFour,
			expectedTime: threeNanoSecs,
			errs: []error{
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil,
			},
			_tamperBackend: noTamper,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
				// Some disks can't access xl.meta.
				errFileNotFound, errDiskAccessDenied, errDiskNotFound,
			},
			_tamperBackend: deletePart,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
				// Some disks don't have xl.meta.
				errDiskNotFound, errFileNotFound, errFileNotFound,
			},
			_tamperBackend: corruptPart,
		},
	}

	bucket := "bucket"
	err = obj.MakeBucket(ctx, "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	object := "object"
	data := bytes.Repeat([]byte("a"), smallFileThreshold*32)
	z := obj.(*erasureServerPools)

	erasureDisks, err := z.GetDisks(0, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("Failed to putObject %v", err)
			}

			partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, "", bucket, object, "", false, true)
			fi, err := getLatestFileInfo(ctx, partsMetadata, z.serverPools[0].sets[0].defaultParityCount, errs)
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
					dErr := erasureDisks[index].Delete(t.Context(), bucket, pathJoin(object, fi.DataDir, "part.1"), DeleteOptions{
						Recursive: false,
						Immediate: false,
					})
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
					f.WriteString("oops") // Will cause bitrot error
					f.Close()
					break
				}
			}

			rQuorum := len(errs) - z.serverPools[0].sets[0].defaultParityCount
			onlineDisks, modTime, _ := listOnlineDisks(erasureDisks, partsMetadata, test.errs, rQuorum)
			if !modTime.Equal(test.expectedTime) {
				t.Fatalf("Expected modTime to be equal to %v but was found to be %v",
					test.expectedTime, modTime)
			}
			_, _ = checkObjectWithAllParts(ctx, onlineDisks, partsMetadata,
				test.errs, fi, false, bucket, object, madmin.HealDeepScan)

			if test._tamperBackend != noTamper {
				if tamperedIndex != -1 && onlineDisks[tamperedIndex] != nil {
					t.Fatalf("Drive (%v) with part.1 missing is not a drive with available data",
						erasureDisks[tamperedIndex])
				}
			}
		})
	}
}

// TestListOnlineDisksSmallObjects - checks if listOnlineDisks and outDatedDisks
// are consistent with each other.
func TestListOnlineDisksSmallObjects(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	setObjectLayer(obj)
	defer obj.Shutdown(t.Context())
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
	modTimesThreeNone := make([]time.Time, 16)
	modTimesThreeFour := make([]time.Time, 16)
	for i := range 16 {
		// Have 13 good xl.meta, 12 for default parity count = 4 (EC:4) and one
		// to be tampered with.
		if i > 12 {
			modTimesThreeFour[i] = fourNanoSecs
			modTimesThreeNone[i] = timeSentinel
			continue
		}
		modTimesThreeFour[i] = threeNanoSecs
		modTimesThreeNone[i] = threeNanoSecs
	}

	testCases := []struct {
		modTimes       []time.Time
		expectedTime   time.Time
		errs           []error
		_tamperBackend tamperKind
	}{
		{
			modTimes:     modTimesThreeFour,
			expectedTime: threeNanoSecs,
			errs: []error{
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil,
			},
			_tamperBackend: noTamper,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
				// Some disks can't access xl.meta.
				errFileNotFound, errDiskAccessDenied, errDiskNotFound,
			},
			_tamperBackend: deletePart,
		},
		{
			modTimes:     modTimesThreeNone,
			expectedTime: threeNanoSecs,
			errs: []error{
				// Disks that have a valid xl.meta.
				nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil,
				// Some disks don't have xl.meta.
				errDiskNotFound, errFileNotFound, errFileNotFound,
			},
			_tamperBackend: corruptPart,
		},
	}

	bucket := "bucket"
	err = obj.MakeBucket(ctx, "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	object := "object"
	data := bytes.Repeat([]byte("a"), smallFileThreshold/2)
	z := obj.(*erasureServerPools)

	erasureDisks, err := z.GetDisks(0, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			_, err := obj.PutObject(ctx, bucket, object,
				mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("Failed to putObject %v", err)
			}

			partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, "", bucket, object, "", true, true)
			fi, err := getLatestFileInfo(ctx, partsMetadata, z.serverPools[0].sets[0].defaultParityCount, errs)
			if err != nil {
				t.Fatalf("Failed to getLatestFileInfo %v", err)
			}

			for j := range partsMetadata {
				if errs[j] != nil {
					t.Fatalf("expected error to be nil: %s", errs[j])
				}
				partsMetadata[j].ModTime = test.modTimes[j]
			}

			if erasureDisks, err = writeUniqueFileInfo(ctx, erasureDisks, "", bucket, object, partsMetadata, diskCount(erasureDisks)); err != nil {
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
					dErr := erasureDisks[index].Delete(t.Context(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
						Recursive: false,
						Immediate: false,
					})
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
					f.WriteString("oops") // Will cause bitrot error
					f.Close()
					break
				}
			}

			rQuorum := len(errs) - z.serverPools[0].sets[0].defaultParityCount
			onlineDisks, modTime, _ := listOnlineDisks(erasureDisks, partsMetadata, test.errs, rQuorum)
			if !modTime.Equal(test.expectedTime) {
				t.Fatalf("Expected modTime to be equal to %v but was found to be %v",
					test.expectedTime, modTime)
			}

			_, _ = checkObjectWithAllParts(ctx, onlineDisks, partsMetadata,
				test.errs, fi, false, bucket, object, madmin.HealDeepScan)

			if test._tamperBackend != noTamper {
				if tamperedIndex != -1 && onlineDisks[tamperedIndex] != nil {
					t.Fatalf("Drive (%v) with part.1 missing is not a drive with available data",
						erasureDisks[tamperedIndex])
				}
			}
		})
	}
}

func TestDisksWithAllParts(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	obj, disks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Prepare Erasure backend failed - %v", err)
	}
	setObjectLayer(obj)
	defer obj.Shutdown(t.Context())
	defer removeRoots(disks)

	bucket := "bucket"
	object := "object"
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), 6*1024*1024*partCount)
	z := obj.(*erasureServerPools)
	s := z.serverPools[0].sets[0]
	erasureDisks := s.getDisks()
	err = obj.MakeBucket(ctx, "bucket", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	_, errs := readAllFileInfo(ctx, erasureDisks, "", bucket, object, "", false, true)
	readQuorum := len(erasureDisks) / 2
	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		t.Fatalf("Failed to read xl meta data %v", reducedErr)
	}

	// Test 1: Test that all disks are returned without any failures with
	// unmodified meta data
	erasureDisks = s.getDisks()
	partsMetadata, errs := readAllFileInfo(ctx, erasureDisks, "", bucket, object, "", false, true)
	if err != nil {
		t.Fatalf("Failed to read xl meta data %v", err)
	}

	fi, err := getLatestFileInfo(ctx, partsMetadata, s.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to get quorum consistent fileInfo %v", err)
	}

	erasureDisks, _, _ = listOnlineDisks(erasureDisks, partsMetadata, errs, readQuorum)

	dataErrsPerDisk, _ := checkObjectWithAllParts(ctx, erasureDisks, partsMetadata,
		errs, fi, false, bucket, object, madmin.HealDeepScan)

	for diskIndex, disk := range erasureDisks {
		if partNeedsHealing(dataErrsPerDisk[diskIndex]) {
			t.Errorf("Unexpected error: %v", dataErrsPerDisk[diskIndex])
		}

		if disk == nil {
			t.Errorf("Drive erroneously filtered, driveIndex: %d", diskIndex)
		}
	}

	// Test 2: Not synchronized modtime
	erasureDisks = s.getDisks()
	partsMetadataBackup := partsMetadata[0]
	partsMetadata[0].ModTime = partsMetadata[0].ModTime.Add(-1 * time.Hour)

	errs = make([]error, len(erasureDisks))
	_, _ = checkObjectWithAllParts(ctx, erasureDisks, partsMetadata,
		errs, fi, false, bucket, object, madmin.HealDeepScan)

	for diskIndex, disk := range erasureDisks {
		if diskIndex == 0 && disk != nil {
			t.Errorf("Drive not filtered as expected, drive: %d", diskIndex)
		}
		if diskIndex != 0 && disk == nil {
			t.Errorf("Drive erroneously filtered, driveIndex: %d", diskIndex)
		}
	}
	partsMetadata[0] = partsMetadataBackup // Revert before going to the next test

	// Test 3: Not synchronized DataDir
	erasureDisks = s.getDisks()
	partsMetadataBackup = partsMetadata[1]
	partsMetadata[1].DataDir = "foo-random"

	errs = make([]error, len(erasureDisks))
	_, _ = checkObjectWithAllParts(ctx, erasureDisks, partsMetadata,
		errs, fi, false, bucket, object, madmin.HealDeepScan)

	for diskIndex, disk := range erasureDisks {
		if diskIndex == 1 && disk != nil {
			t.Errorf("Drive not filtered as expected, drive: %d", diskIndex)
		}
		if diskIndex != 1 && disk == nil {
			t.Errorf("Drive erroneously filtered, driveIndex: %d", diskIndex)
		}
	}
	partsMetadata[1] = partsMetadataBackup // Revert before going to the next test

	// Test 4: key = disk index, value = part name with hash mismatch
	erasureDisks = s.getDisks()
	diskFailures := make(map[int]string)
	diskFailures[0] = "part.1"
	diskFailures[3] = "part.1"
	diskFailures[15] = "part.1"

	for diskIndex, partName := range diskFailures {
		for i := range partsMetadata[diskIndex].Parts {
			if fmt.Sprintf("part.%d", i+1) == partName {
				filePath := pathJoin(erasureDisks[diskIndex].String(), bucket, object, partsMetadata[diskIndex].DataDir, partName)
				f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_SYNC, 0)
				if err != nil {
					t.Fatalf("Failed to open %s: %s\n", filePath, err)
				}
				f.WriteString("oops") // Will cause bitrot error
				f.Close()
			}
		}
	}

	errs = make([]error, len(erasureDisks))
	dataErrsPerDisk, _ = checkObjectWithAllParts(ctx, erasureDisks, partsMetadata,
		errs, fi, false, bucket, object, madmin.HealDeepScan)

	for diskIndex := range erasureDisks {
		if _, ok := diskFailures[diskIndex]; ok {
			if !partNeedsHealing(dataErrsPerDisk[diskIndex]) {
				t.Errorf("Disk expected to be healed, driveIndex: %d", diskIndex)
			}
		} else {
			if partNeedsHealing(dataErrsPerDisk[diskIndex]) {
				t.Errorf("Disk not expected to be healed, driveIndex: %d", diskIndex)
			}
		}
	}
}

func TestCommonParities(t *testing.T) {
	// This test uses two FileInfo values that represent the same object but
	// have different parities. They occur in equal number of drives, but only
	// one has read quorum. commonParity should pick the parity corresponding to
	// the FileInfo which has read quorum.
	fi1 := FileInfo{
		Volume:         "mybucket",
		Name:           "myobject",
		VersionID:      "",
		IsLatest:       true,
		Deleted:        false,
		ExpireRestored: false,
		DataDir:        "4a01d9dd-0c5e-4103-88f8-b307c57d212e",
		XLV1:           false,
		ModTime:        time.Date(2023, time.March, 15, 11, 18, 4, 989906961, time.UTC),
		Size:           329289, Mode: 0x0, WrittenByVersion: 0x63c77756,
		Metadata: map[string]string{
			"content-type": "application/octet-stream", "etag": "f205307ef9f50594c4b86d9c246bee86", "x-minio-internal-erasure-upgraded": "5->6", "x-minio-internal-inline-data": "true",
		},
		Parts: []ObjectPartInfo{
			{
				ETag:       "",
				Number:     1,
				Size:       329289,
				ActualSize: 329289,
				ModTime:    time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
				Index:      []uint8(nil),
				Checksums:  map[string]string(nil),
			},
		},
		Erasure: ErasureInfo{
			Algorithm:    "ReedSolomon",
			DataBlocks:   6,
			ParityBlocks: 6,
			BlockSize:    1048576,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			Checksums:    []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}},
		},
		NumVersions: 1,
		Idx:         0,
	}

	fi2 := FileInfo{
		Volume:           "mybucket",
		Name:             "myobject",
		VersionID:        "",
		IsLatest:         true,
		Deleted:          false,
		DataDir:          "6f5c106d-9d28-4c85-a7f4-eac56225876b",
		ModTime:          time.Date(2023, time.March, 15, 19, 57, 30, 492530160, time.UTC),
		Size:             329289,
		Mode:             0x0,
		WrittenByVersion: 0x63c77756,
		Metadata:         map[string]string{"content-type": "application/octet-stream", "etag": "f205307ef9f50594c4b86d9c246bee86", "x-minio-internal-inline-data": "true"},
		Parts: []ObjectPartInfo{
			{
				ETag:       "",
				Number:     1,
				Size:       329289,
				ActualSize: 329289,
				ModTime:    time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
				Index:      []uint8(nil),
				Checksums:  map[string]string(nil),
			},
		},
		Erasure: ErasureInfo{
			Algorithm:    "ReedSolomon",
			DataBlocks:   7,
			ParityBlocks: 5,
			BlockSize:    1048576,
			Index:        2,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			Checksums: []ChecksumInfo{
				{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}},
			},
		},
		NumVersions: 1,
		Idx:         0,
	}

	fiDel := FileInfo{
		Volume:           "mybucket",
		Name:             "myobject",
		VersionID:        "",
		IsLatest:         true,
		Deleted:          true,
		ModTime:          time.Date(2023, time.March, 15, 19, 57, 30, 492530160, time.UTC),
		Mode:             0x0,
		WrittenByVersion: 0x63c77756,
		NumVersions:      1,
		Idx:              0,
	}

	tests := []struct {
		fi1, fi2 FileInfo
	}{
		{
			fi1: fi1,
			fi2: fi2,
		},
		{
			fi1: fi1,
			fi2: fiDel,
		},
	}
	for idx, test := range tests {
		var metaArr []FileInfo
		for i := range 12 {
			fi := test.fi1
			if i%2 == 0 {
				fi = test.fi2
			}
			metaArr = append(metaArr, fi)
		}

		parities := listObjectParities(metaArr, make([]error, len(metaArr)))
		parity := commonParity(parities, 5)
		var match int
		for _, fi := range metaArr {
			if fi.Erasure.ParityBlocks == parity {
				match++
			}
		}
		if match < len(metaArr)-parity {
			t.Fatalf("Test %d: Expected %d drives with parity=%d, but got %d", idx, len(metaArr)-parity, parity, match)
		}
	}
}
