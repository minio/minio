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
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// commonTime returns a maximally occurring time from a list of time.
func commonTime(modTimes []time.Time) (modTime time.Time, count int) {
	var maxima int // Counter for remembering max occurrence of elements.
	timeOccurenceMap := make(map[time.Time]int)
	// Ignore the uuid sentinel and count the rest.
	for _, time := range modTimes {
		if time == timeSentinel {
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

// Extracts list of times from xlMetaV1 slice and returns, skips
// slice elements which have errors.
func listObjectModtimes(partsMetadata []xlMetaV1, errs []error) (modTimes []time.Time) {
	modTimes = bootModtimes(len(partsMetadata))
	for index, metadata := range partsMetadata {
		if errs[index] != nil {
			continue
		}
		// Once the file is found, save the uuid saved on disk.
		modTimes[index] = metadata.Stat.ModTime
	}
	return modTimes
}

// Notes:
// There are 5 possible states a disk could be in,
// 1. __online__             - has the latest copy of xl.json - returned by listOnlineDisks
//
// 2. __offline__            - err == errDiskNotFound
//
// 3. __availableWithParts__ - has the latest copy of xl.json and has all
//                             parts with checksums matching; returned by disksWithAllParts
//
// 4. __outdated__           - returned by outDatedDisk, provided []StorageAPI
//                             returned by diskWithAllParts is passed for latestDisks.
//    - has an old copy of xl.json
//    - doesn't have xl.json (errFileNotFound)
//    - has the latest xl.json but one or more parts are corrupt
//
// 5. __missingParts__       - has the latest copy of xl.json but has some parts
// missing.  This is identified separately since this may need manual
// inspection to understand the root cause. E.g, this could be due to
// backend filesystem corruption.

// listOnlineDisks - returns
// - a slice of disks where disk having 'older' xl.json (or nothing)
// are set to nil.
// - latest (in time) of the maximally occurring modTime(s).
func listOnlineDisks(disks []StorageAPI, partsMetadata []xlMetaV1, errs []error) (onlineDisks []StorageAPI, modTime time.Time) {
	onlineDisks = make([]StorageAPI, len(disks))

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Reduce list of UUIDs to a single common value.
	modTime, _ = commonTime(modTimes)

	// Create a new online disks slice, which have common uuid.
	for index, t := range modTimes {
		if t == modTime {
			onlineDisks[index] = disks[index]
		} else {
			onlineDisks[index] = nil
		}
	}
	return onlineDisks, modTime
}

// Returns the latest updated xlMeta files and error in case of failure.
func getLatestXLMeta(ctx context.Context, partsMetadata []xlMetaV1, errs []error) (xlMetaV1, error) {

	// There should be atleast half correct entries, if not return failure
	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, globalXLSetDriveCount/2); reducedErr != nil {
		return xlMetaV1{}, reducedErr
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Count all latest updated xlMeta values
	var count int
	var latestXLMeta xlMetaV1

	// Reduce list of UUIDs to a single common value - i.e. the last updated Time
	modTime, _ := commonTime(modTimes)

	// Interate through all the modTimes and count the xlMeta(s) with latest time.
	for index, t := range modTimes {
		if t == modTime && partsMetadata[index].IsValid() {
			latestXLMeta = partsMetadata[index]
			count++
		}
	}
	if count < len(partsMetadata)/2 {
		return xlMetaV1{}, errXLReadQuorum
	}

	return latestXLMeta, nil
}

// disksWithAllParts - This function needs to be called with
// []StorageAPI returned by listOnlineDisks. Returns,
//
// - disks which have all parts specified in the latest xl.json.
//
// - slice of errors about the state of data files on disk - can have
//   a not-found error or a hash-mismatch error.
func disksWithAllParts(ctx context.Context, onlineDisks []StorageAPI, partsMetadata []xlMetaV1, errs []error, bucket,
	object string) ([]StorageAPI, []error) {
	availableDisks := make([]StorageAPI, len(onlineDisks))
	buffer := []byte{}
	dataErrs := make([]error, len(onlineDisks))

	for i, onlineDisk := range onlineDisks {
		if onlineDisk == nil {
			dataErrs[i] = errDiskNotFound
			continue
		}

		// disk has a valid xl.json but may not have all the
		// parts. This is considered an outdated disk, since
		// it needs healing too.
		for _, part := range partsMetadata[i].Parts {
			partPath := filepath.Join(object, part.Name)
			checksumInfo := partsMetadata[i].Erasure.GetChecksumInfo(part.Name)
			verifier := NewBitrotVerifier(checksumInfo.Algorithm, checksumInfo.Hash)

			// verification happens even if a 0-length
			// buffer is passed
			_, hErr := onlineDisk.ReadFile(bucket, partPath, 0, buffer, verifier)

			isCorrupt := false
			if hErr != nil {
				isCorrupt = strings.HasPrefix(hErr.Error(), "Bitrot verification mismatch - expected ")
			}
			switch {
			case isCorrupt:
				fallthrough
			case hErr == errFileNotFound, hErr == errVolumeNotFound:
				dataErrs[i] = hErr
				break
			case hErr != nil:
				logger.LogIf(ctx, hErr)
				dataErrs[i] = hErr
				break
			}
		}

		if dataErrs[i] == nil {
			// All parts verified, mark it as all data available.
			availableDisks[i] = onlineDisk
		}
	}

	return availableDisks, dataErrs
}
