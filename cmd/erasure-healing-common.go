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
	"time"

	"github.com/minio/madmin-go"
)

// commonTime returns a maximally occurring time from a list of time.
func commonTime(modTimes []time.Time, dataDirs []string) (modTime time.Time, dataDir string) {
	var maxima int // Counter for remembering max occurrence of elements.

	timeOccurenceMap := make(map[int64]int, len(modTimes))
	dataDirOccurenceMap := make(map[string]int, len(dataDirs))
	// Ignore the uuid sentinel and count the rest.
	for _, time := range modTimes {
		if time.Equal(timeSentinel) {
			continue
		}
		timeOccurenceMap[time.UnixNano()]++
	}

	for _, dataDir := range dataDirs {
		if dataDir == "" {
			continue
		}
		dataDirOccurenceMap[dataDir]++
	}

	// Find the common cardinality from previously collected
	// occurrences of elements.
	for nano, count := range timeOccurenceMap {
		t := time.Unix(0, nano)
		if count > maxima || (count == maxima && t.After(modTime)) {
			maxima = count
			modTime = t
		}
	}

	// Find the common cardinality from the previously collected
	// occurrences of elements.
	var dmaxima int
	for ddataDir, count := range dataDirOccurenceMap {
		if count > dmaxima {
			dmaxima = count
			dataDir = ddataDir
		}
	}

	// Return the collected common uuid.
	return modTime, dataDir
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
func listOnlineDisks(disks []StorageAPI, partsMetadata []FileInfo, errs []error) (onlineDisks []StorageAPI, modTime time.Time, dataDir string) {
	onlineDisks = make([]StorageAPI, len(disks))

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	dataDirs := make([]string, len(partsMetadata))
	for idx, fi := range partsMetadata {
		if errs[idx] != nil {
			continue
		}
		dataDirs[idx] = fi.DataDir
	}

	// Reduce list of UUIDs to a single common value.
	modTime, dataDir = commonTime(modTimes, dataDirs)

	// Create a new online disks slice, which have common uuid.
	for index, t := range modTimes {
		if partsMetadata[index].IsValid() && t.Equal(modTime) && partsMetadata[index].DataDir == dataDir {
			onlineDisks[index] = disks[index]
		} else {
			onlineDisks[index] = nil
		}
	}

	return onlineDisks, modTime, dataDir
}

// Returns the latest updated FileInfo files and error in case of failure.
func getLatestFileInfo(ctx context.Context, partsMetadata []FileInfo, errs []error) (FileInfo, error) {
	// There should be atleast half correct entries, if not return failure
	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, len(partsMetadata)/2); reducedErr != nil {
		return FileInfo{}, reducedErr
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	dataDirs := make([]string, len(partsMetadata))
	for idx, fi := range partsMetadata {
		if errs[idx] != nil {
			continue
		}
		dataDirs[idx] = fi.DataDir
	}

	// Count all latest updated FileInfo values
	var count int
	var latestFileInfo FileInfo

	// Reduce list of UUIDs to a single common value - i.e. the last updated Time
	modTime, dataDir := commonTime(modTimes, dataDirs)

	// Interate through all the modTimes and count the FileInfo(s) with latest time.
	for index, t := range modTimes {
		if partsMetadata[index].IsValid() && t.Equal(modTime) && dataDir == partsMetadata[index].DataDir {
			latestFileInfo = partsMetadata[index]
			count++
		}
	}
	if count < len(partsMetadata)/2 {
		return FileInfo{}, errErasureReadQuorum
	}

	return latestFileInfo, nil
}

// fileInfoConsistent whether all fileinfos are consistent with each other.
// Will return false if any fileinfo mismatches.
func fileInfoConsistent(ctx context.Context, partsMetadata []FileInfo, errs []error) bool {
	// There should be atleast half correct entries, if not return failure
	if reducedErr := reduceReadQuorumErrs(ctx, errs, nil, len(partsMetadata)/2); reducedErr != nil {
		return false
	}
	if len(partsMetadata) == 1 {
		return true
	}
	// Reference
	ref := partsMetadata[0]
	if !ref.IsValid() {
		return false
	}
	for _, meta := range partsMetadata[1:] {
		if !meta.IsValid() {
			return false
		}
		if !meta.ModTime.Equal(ref.ModTime) {
			return false
		}
		if meta.DataDir != ref.DataDir {
			return false
		}
	}
	return true
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
	// List of disks having latest version of the object er.meta  (by modtime)
	_, modTime, dataDir := listOnlineDisks(onlineDisks, partsMetadata, errs)

	availableDisks := make([]StorageAPI, len(onlineDisks))
	dataErrs := make([]error, len(onlineDisks))
	inconsistent := 0
	for i, meta := range partsMetadata {
		if !meta.IsValid() {
			// Since for majority of the cases erasure.Index matches with erasure.Distribution we can
			// consider the offline disks as consistent.
			continue
		}
		if len(meta.Erasure.Distribution) != len(onlineDisks) {
			// Erasure distribution seems to have lesser
			// number of items than number of online disks.
			inconsistent++
			continue
		}
		if meta.Erasure.Distribution[i] != meta.Erasure.Index {
			// Mismatch indexes with distribution order
			inconsistent++
		}
	}

	erasureDistributionReliable := true
	if inconsistent > len(partsMetadata)/2 {
		// If there are too many inconsistent files, then we can't trust erasure.Distribution (most likely
		// because of bugs found in CopyObject/PutObjectTags) https://github.com/minio/minio/pull/10772
		erasureDistributionReliable = false
	}

	for i, onlineDisk := range onlineDisks {
		if errs[i] != nil {
			dataErrs[i] = errs[i]
			continue
		}
		if onlineDisk == nil {
			dataErrs[i] = errDiskNotFound
			continue
		}
		meta := partsMetadata[i]

		if !meta.ModTime.Equal(modTime) || meta.DataDir != dataDir {
			dataErrs[i] = errFileCorrupt
			partsMetadata[i] = FileInfo{}
			continue
		}

		if erasureDistributionReliable {
			if !meta.IsValid() {
				continue
			}

			if len(meta.Erasure.Distribution) != len(onlineDisks) {
				// Erasure distribution is not the same as onlineDisks
				// attempt a fix if possible, assuming other entries
				// might have the right erasure distribution.
				partsMetadata[i] = FileInfo{}
				dataErrs[i] = errFileCorrupt
				continue
			}

			// Since erasure.Distribution is trustable we can fix the mismatching erasure.Index
			if meta.Erasure.Distribution[i] != meta.Erasure.Index {
				partsMetadata[i] = FileInfo{}
				dataErrs[i] = errFileCorrupt
				continue
			}
		}

		// Always check data, if we got it.
		if (len(meta.Data) > 0 || meta.Size == 0) && len(meta.Parts) > 0 {
			checksumInfo := meta.Erasure.GetChecksumInfo(meta.Parts[0].Number)
			dataErrs[i] = bitrotVerify(bytes.NewReader(meta.Data),
				int64(len(meta.Data)),
				meta.Erasure.ShardFileSize(meta.Size),
				checksumInfo.Algorithm,
				checksumInfo.Hash, meta.Erasure.ShardSize())
			if dataErrs[i] == nil {
				// All parts verified, mark it as all data available.
				availableDisks[i] = onlineDisk
			} else {
				// upon errors just make that disk's fileinfo invalid
				partsMetadata[i] = FileInfo{}
			}
			continue
		}

		switch scanMode {
		case madmin.HealDeepScan:
			// disk has a valid xl.meta but may not have all the
			// parts. This is considered an outdated disk, since
			// it needs healing too.
			if !partsMetadata[i].IsRemote() {
				dataErrs[i] = onlineDisk.VerifyFile(ctx, bucket, object, partsMetadata[i])
			}
		case madmin.HealNormalScan:
			if !partsMetadata[i].IsRemote() {
				dataErrs[i] = onlineDisk.CheckParts(ctx, bucket, object, partsMetadata[i])
			}
		}

		if dataErrs[i] == nil {
			// All parts verified, mark it as all data available.
			availableDisks[i] = onlineDisk
		} else {
			// upon errors just make that disk's fileinfo invalid
			partsMetadata[i] = FileInfo{}
		}
	}

	return availableDisks, dataErrs
}
