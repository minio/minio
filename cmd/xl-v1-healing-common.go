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
	"path/filepath"
	"time"
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
		if count == maxima && time.After(modTime) {
			maxima = count
			modTime = time

		} else if count > maxima {
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

// outDatedDisks - return disks which don't have the latest object (i.e xl.json).
// disks that are offline are not 'marked' outdated.
func outDatedDisks(disks, latestDisks []StorageAPI, errs []error, partsMetadata []xlMetaV1,
	bucket, object string) (outDatedDisks []StorageAPI) {

	outDatedDisks = make([]StorageAPI, len(disks))
	for index, latestDisk := range latestDisks {
		if latestDisk != nil {
			continue
		}
		// disk either has an older xl.json or doesn't have one.
		switch errorCause(errs[index]) {
		case nil, errFileNotFound:
			outDatedDisks[index] = disks[index]
		}
	}

	return outDatedDisks
}

// Returns if the object should be healed.
func xlShouldHeal(disks []StorageAPI, partsMetadata []xlMetaV1, errs []error, bucket, object string) bool {
	onlineDisks, _ := listOnlineDisks(disks, partsMetadata,
		errs)
	// Return true even if one of the disks have stale data.
	for _, disk := range onlineDisks {
		if disk == nil {
			return true
		}
	}

	// Check if all parts of an object are available and their
	// checksums are valid.
	availableDisks, _, err := disksWithAllParts(onlineDisks, partsMetadata,
		errs, bucket, object)
	if err != nil {
		// Note: This error is due to failure of blake2b
		// checksum computation of a part. It doesn't clearly
		// indicate if the object needs healing. At this
		// juncture healing could fail with the same
		// error. So, we choose to return that there is no
		// need to heal.
		return false
	}

	// Return true even if one disk has xl.json or one or more
	// parts missing.
	for _, disk := range availableDisks {
		if disk == nil {
			return true
		}
	}

	return false
}

// xlHealStat - returns a structure which describes how many data,
// parity erasure blocks are missing and if it is possible to heal
// with the blocks present.
func xlHealStat(xl xlObjects, partsMetadata []xlMetaV1, errs []error) HealObjectInfo {
	// Less than quorum erasure coded blocks of the object have the same create time.
	// This object can't be healed with the information we have.
	modTime, count := commonTime(listObjectModtimes(partsMetadata, errs))
	if count < xl.readQuorum {
		return HealObjectInfo{
			Status:             quorumUnavailable,
			MissingDataCount:   0,
			MissingParityCount: 0,
		}
	}

	// If there isn't a valid xlMeta then we can't heal the object.
	xlMeta, err := pickValidXLMeta(partsMetadata, modTime)
	if err != nil {
		return HealObjectInfo{
			Status:             corrupted,
			MissingDataCount:   0,
			MissingParityCount: 0,
		}
	}

	// Compute heal statistics like bytes to be healed, missing
	// data and missing parity count.
	missingDataCount := 0
	missingParityCount := 0

	disksMissing := false
	for i, err := range errs {
		// xl.json is not found, which implies the erasure
		// coded blocks are unavailable in the corresponding disk.
		// First half of the disks are data and the rest are parity.
		switch realErr := errorCause(err); realErr {
		case errDiskNotFound:
			disksMissing = true
			fallthrough
		case errFileNotFound:
			if xlMeta.Erasure.Distribution[i]-1 < xl.dataBlocks {
				missingDataCount++
			} else {
				missingParityCount++
			}
		}
	}

	// The object may not be healed completely, since some of the
	// disks needing healing are unavailable.
	if disksMissing {
		return HealObjectInfo{
			Status:             canPartiallyHeal,
			MissingDataCount:   missingDataCount,
			MissingParityCount: missingParityCount,
		}
	}

	// This object can be healed. We have enough object metadata
	// to reconstruct missing erasure coded blocks.
	return HealObjectInfo{
		Status:             canHeal,
		MissingDataCount:   missingDataCount,
		MissingParityCount: missingParityCount,
	}
}

// disksWithAllParts - This function needs to be called with
// []StorageAPI returned by listOnlineDisks. Returns,
//
// - disks which have all parts specified in the latest xl.json.
//
// - errs updated to have errFileNotFound in place of disks that had
//   missing or corrupted parts.
//
// - non-nil error if any of the disks failed unexpectedly (i.e. error
//   other than file not found and not a checksum error).
func disksWithAllParts(onlineDisks []StorageAPI, partsMetadata []xlMetaV1, errs []error, bucket,
	object string) ([]StorageAPI, []error, error) {

	availableDisks := make([]StorageAPI, len(onlineDisks))
	buffer := []byte{}

	for i, onlineDisk := range onlineDisks {
		if onlineDisk == OfflineDisk {
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
			if hErr != nil {
				_, isCorrupted := hErr.(hashMismatchError)
				if isCorrupted || hErr == errFileNotFound {
					errs[i] = errFileNotFound
					availableDisks[i] = OfflineDisk
					break
				}
				return nil, nil, traceError(hErr)
			}
		}

		if errs[i] == nil {
			// All parts verified, mark it as all data available.
			availableDisks[i] = onlineDisk
		}
	}

	return availableDisks, errs, nil
}
