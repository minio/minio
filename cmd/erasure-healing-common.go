/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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
	"context"
	"time"

	"github.com/minio/minio/pkg/madmin"
)

// commonTime returns a maximally occurring time from a list of time.
func commonTime(modTimes []time.Time) (modTime time.Time, count int) {
	var maxima int // Counter for remembering max occurrence of elements.
	timeOccurenceMap := make(map[time.Time]int)
	// Ignore the uuid sentinel and count the rest.
	for _, time := range modTimes {
		if time.Equal(timeSentinel) {
			continue
		}
		timeOccurenceMap[time]++
	}
	// Find the common cardinality from previously collected
	// occurrences of elements.
	for time, count := range timeOccurenceMap {
		if count > maxima || (count == maxima && time.After(modTime)) {
			maxima = count
			modTime = time
		}
	}
	// Return the collected common uuid.
	return modTime, maxima
}

// Beginning of unix time is treated as sentinel value here.
var timeSentinel = time.Unix(0, 0).UTC()

// Boot modTimes up to disk count, setting the value to time sentinel.
func bootModtimes(diskCount int) []time.Time {
	modTimes := make([]time.Time, diskCount)
	// Boots up all the modtimes.
	for i := range modTimes {
		modTimes[i] = timeSentinel
	}
	return modTimes
}

// Extracts list of times from FileInfo slice and returns, skips
// slice elements which have errors.
func listObjectModtimes(partsMetadata []FileInfo, errs []error) (modTimes []time.Time) {
	modTimes = bootModtimes(len(partsMetadata))
	for index, metadata := range partsMetadata {
		if errs[index] != nil {
			continue
		}
		// Once the file is found, save the uuid saved on disk.
		modTimes[index] = metadata.ModTime
	}
	return modTimes
}

// Notes:
// There are 5 possible states a disk could be in,
// 1. __online__             - has the latest copy of xl.meta - returned by listOnlineDisks
//
// 2. __offline__            - err == errDiskNotFound
//
// 3. __availableWithParts__ - has the latest copy of xl.meta and has all
//                             parts with checksums matching; returned by disksWithAllParts
//
// 4. __outdated__           - returned by outDatedDisk, provided []StorageAPI
//                             returned by diskWithAllParts is passed for latestDisks.
//    - has an old copy of xl.meta
//    - doesn't have xl.meta (errFileNotFound)
//    - has the latest xl.meta but one or more parts are corrupt
//
// 5. __missingParts__       - has the latest copy of xl.meta but has some parts
// missing.  This is identified separately since this may need manual
// inspection to understand the root cause. E.g, this could be due to
// backend filesystem corruption.

// listOnlineDisks - returns
// - a slice of disks where disk having 'older' xl.meta (or nothing)
// are set to nil.
// - latest (in time) of the maximally occurring modTime(s).
func listOnlineDisks(disks []StorageAPI, partsMetadata []FileInfo, errs []error) (onlineDisks []StorageAPI, modTime time.Time) {
	onlineDisks = make([]StorageAPI, len(disks))

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Reduce list of UUIDs to a single common value.
	modTime, _ = commonTime(modTimes)

	// Create a new online disks slice, which have common uuid.
	for index, t := range modTimes {
		if t.Equal(modTime) {
			onlineDisks[index] = disks[index]
		} else {
			onlineDisks[index] = nil
		}
	}
	return onlineDisks, modTime
}

// Returns the latest updated FileInfo files and error in case of failure.
func getLatestFileInfo(ctx context.Context, partsMetadata []FileInfo, errs []error) (FileInfo, error) {
	// There should be atleast half correct entries, if not return failure
	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, len(partsMetadata)/2); reducedErr != nil {
		return FileInfo{}, reducedErr
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Count all latest updated FileInfo values
	var count int
	var latestFileInfo FileInfo

	// Reduce list of UUIDs to a single common value - i.e. the last updated Time
	modTime, _ := commonTime(modTimes)

	// Interate through all the modTimes and count the FileInfo(s) with latest time.
	for index, t := range modTimes {
		if t.Equal(modTime) && partsMetadata[index].IsValid() {
			latestFileInfo = partsMetadata[index]
			count++
		}
	}
	if count < len(partsMetadata)/2 {
		return FileInfo{}, errErasureReadQuorum
	}

	return latestFileInfo, nil
}

// disksWithAllParts - This function needs to be called with
// []StorageAPI returned by listOnlineDisks. Returns,
//
// - disks which have all parts specified in the latest xl.meta.
//
// - slice of errors about the state of data files on disk - can have
//   a not-found error or a hash-mismatch error.
func disksWithAllParts(ctx context.Context, onlineDisks []StorageAPI, partsMetadata []FileInfo, errs []error, bucket,
	object string, scanMode madmin.HealScanMode) ([]StorageAPI, []error) {
	availableDisks := make([]StorageAPI, len(onlineDisks))
	dataErrs := make([]error, len(onlineDisks))

	for i, onlineDisk := range onlineDisks {
		if errs[i] != nil {
			dataErrs[i] = errs[i]
			continue
		}
		if onlineDisk == nil {
			dataErrs[i] = errDiskNotFound
			continue
		}

		switch scanMode {
		case madmin.HealDeepScan:
			// disk has a valid xl.meta but may not have all the
			// parts. This is considered an outdated disk, since
			// it needs healing too.
			dataErrs[i] = onlineDisk.VerifyFile(bucket, object, partsMetadata[i])
		case madmin.HealNormalScan:
			dataErrs[i] = onlineDisk.CheckParts(bucket, object, partsMetadata[i])
		}

		if dataErrs[i] == nil {
			// All parts verified, mark it as all data available.
			availableDisks[i] = onlineDisk
		}
	}

	return availableDisks, dataErrs
}
