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
	"slices"
	"time"

	"github.com/minio/madmin-go/v3"
)

func commonETags(etags []string) (etag string, maxima int) {
	etagOccurrenceMap := make(map[string]int, len(etags))

	// Ignore the uuid sentinel and count the rest.
	for _, etag := range etags {
		if etag == "" {
			continue
		}
		etagOccurrenceMap[etag]++
	}

	maxima = 0 // Counter for remembering max occurrence of elements.
	latest := ""

	// Find the common cardinality from previously collected
	// occurrences of elements.
	for etag, count := range etagOccurrenceMap {
		if count < maxima {
			continue
		}

		// We are at or above maxima
		if count > maxima {
			maxima = count
			latest = etag
		}
	}

	// Return the collected common max time, with maxima
	return latest, maxima
}

// commonTime returns a maximally occurring time from a list of time.
func commonTimeAndOccurrence(times []time.Time, group time.Duration) (maxTime time.Time, maxima int) {
	timeOccurrenceMap := make(map[int64]int, len(times))
	groupNano := group.Nanoseconds()
	// Ignore the uuid sentinel and count the rest.
	for _, t := range times {
		if t.Equal(timeSentinel) || t.IsZero() {
			continue
		}
		nano := t.UnixNano()
		if group > 0 {
			for k := range timeOccurrenceMap {
				if k == nano {
					// We add to ourself later
					continue
				}
				diff := k - nano
				if diff < 0 {
					diff = -diff
				}
				// We are within the limit
				if diff < groupNano {
					timeOccurrenceMap[k]++
				}
			}
		}
		// Add ourself...
		timeOccurrenceMap[nano]++
	}

	maxima = 0 // Counter for remembering max occurrence of elements.
	latest := int64(0)

	// Find the common cardinality from previously collected
	// occurrences of elements.
	for nano, count := range timeOccurrenceMap {
		if count < maxima {
			continue
		}

		// We are at or above maxima
		if count > maxima || nano > latest {
			maxima = count
			latest = nano
		}
	}

	// Return the collected common max time, with maxima
	return time.Unix(0, latest).UTC(), maxima
}

// commonTime returns a maximally occurring time from a list of time if it
// occurs >= quorum, else return timeSentinel
func commonTime(modTimes []time.Time, quorum int) time.Time {
	if modTime, count := commonTimeAndOccurrence(modTimes, 0); count >= quorum {
		return modTime
	}

	return timeSentinel
}

func commonETag(etags []string, quorum int) string {
	if etag, count := commonETags(etags); count >= quorum {
		return etag
	}
	return ""
}

// Beginning of unix time is treated as sentinel value here.
var (
	timeSentinel     = time.Unix(0, 0).UTC()
	timeSentinel1970 = time.Unix(0, 1).UTC() // 1970 used for special cases when xlmeta.version == 0
)

// Boot modTimes up to disk count, setting the value to time sentinel.
func bootModtimes(diskCount int) []time.Time {
	modTimes := make([]time.Time, diskCount)
	// Boots up all the modtimes.
	for i := range modTimes {
		modTimes[i] = timeSentinel
	}
	return modTimes
}

func listObjectETags(partsMetadata []FileInfo, errs []error, quorum int) (etags []string) {
	etags = make([]string, len(partsMetadata))
	vidMap := map[string]int{}
	for index, metadata := range partsMetadata {
		if errs[index] != nil {
			continue
		}
		vid := metadata.VersionID
		if metadata.VersionID == "" {
			vid = nullVersionID
		}
		vidMap[vid]++
		etags[index] = metadata.Metadata["etag"]
	}

	for _, count := range vidMap {
		// do we have enough common versions
		// that have enough quorum to satisfy
		// the etag.
		if count >= quorum {
			return etags
		}
	}

	return make([]string, len(partsMetadata))
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

func filterOnlineDisksInplace(fi FileInfo, partsMetadata []FileInfo, onlineDisks []StorageAPI) {
	for i, meta := range partsMetadata {
		if fi.XLV1 == meta.XLV1 {
			continue
		}
		onlineDisks[i] = nil
	}
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
// - latest (in time) of the maximally occurring modTime(s), which has at least quorum occurrences.
func listOnlineDisks(disks []StorageAPI, partsMetadata []FileInfo, errs []error, quorum int) (onlineDisks []StorageAPI, modTime time.Time, etag string) {
	onlineDisks = make([]StorageAPI, len(disks))

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Reduce list of UUIDs to a single common value.
	modTime = commonTime(modTimes, quorum)

	if modTime.IsZero() || modTime.Equal(timeSentinel) {
		etags := listObjectETags(partsMetadata, errs, quorum)

		etag = commonETag(etags, quorum)

		if etag != "" { // allow this fallback only if a non-empty etag is found.
			for index, e := range etags {
				if partsMetadata[index].IsValid() && e == etag {
					onlineDisks[index] = disks[index]
				} else {
					onlineDisks[index] = nil
				}
			}
			return onlineDisks, modTime, etag
		}
	}

	// Create a new online disks slice, which have common uuid.
	for index, t := range modTimes {
		if partsMetadata[index].IsValid() && t.Equal(modTime) {
			onlineDisks[index] = disks[index]
		} else {
			onlineDisks[index] = nil
		}
	}

	return onlineDisks, modTime, ""
}

// Convert verify or check parts returned error to integer representation
func convPartErrToInt(err error) int {
	err = unwrapAll(err)
	switch err {
	case nil:
		return checkPartSuccess
	case errFileNotFound, errFileVersionNotFound:
		return checkPartFileNotFound
	case errFileCorrupt:
		return checkPartFileCorrupt
	case errVolumeNotFound:
		return checkPartVolumeNotFound
	case errDiskNotFound:
		return checkPartDiskNotFound
	default:
		return checkPartUnknown
	}
}

func partNeedsHealing(partErrs []int) bool {
	return slices.IndexFunc(partErrs, func(i int) bool { return i != checkPartSuccess && i != checkPartUnknown }) > -1
}

func countPartNotSuccess(partErrs []int) (c int) {
	for _, pe := range partErrs {
		if pe != checkPartSuccess {
			c++
		}
	}
	return c
}

// checkObjectWithAllParts sets partsMetadata and onlineDisks when xl.meta is inexistant/corrupted or outdated
// it also checks if the status of each part (corrupted, missing, ok) in each drive
func checkObjectWithAllParts(ctx context.Context, onlineDisks []StorageAPI, partsMetadata []FileInfo,
	errs []error, latestMeta FileInfo, filterByETag bool, bucket, object string,
	scanMode madmin.HealScanMode,
) (dataErrsByDisk map[int][]int, dataErrsByPart map[int][]int) {
	dataErrsByDisk = make(map[int][]int, len(onlineDisks))
	for i := range onlineDisks {
		dataErrsByDisk[i] = make([]int, len(latestMeta.Parts))
	}

	dataErrsByPart = make(map[int][]int, len(latestMeta.Parts))
	for i := range latestMeta.Parts {
		dataErrsByPart[i] = make([]int, len(onlineDisks))
	}

	inconsistent := 0
	for i, meta := range partsMetadata {
		if !meta.IsValid() {
			// Since for majority of the cases erasure.Index matches with erasure.Distribution we can
			// consider the offline disks as consistent.
			continue
		}
		if !meta.Deleted {
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
	}

	erasureDistributionReliable := inconsistent <= len(partsMetadata)/2

	metaErrs := make([]error, len(errs))

	for i := range onlineDisks {
		if errs[i] != nil {
			metaErrs[i] = errs[i]
			continue
		}
		if onlineDisks[i] == OfflineDisk {
			metaErrs[i] = errDiskNotFound
			continue
		}

		meta := partsMetadata[i]
		corrupted := false
		if filterByETag {
			corrupted = meta.Metadata["etag"] != latestMeta.Metadata["etag"]
		} else {
			corrupted = !meta.ModTime.Equal(latestMeta.ModTime) || meta.DataDir != latestMeta.DataDir
		}

		if corrupted {
			metaErrs[i] = errFileCorrupt
			partsMetadata[i] = FileInfo{}
			onlineDisks[i] = nil
			continue
		}

		if erasureDistributionReliable {
			if !meta.IsValid() {
				partsMetadata[i] = FileInfo{}
				metaErrs[i] = errFileCorrupt
				onlineDisks[i] = nil
				continue
			}

			if !meta.Deleted {
				if len(meta.Erasure.Distribution) != len(onlineDisks) {
					// Erasure distribution is not the same as onlineDisks
					// attempt a fix if possible, assuming other entries
					// might have the right erasure distribution.
					partsMetadata[i] = FileInfo{}
					metaErrs[i] = errFileCorrupt
					onlineDisks[i] = nil
					continue
				}
			}
		}
	}

	// Copy meta errors to part errors
	for i, err := range metaErrs {
		if err != nil {
			partErr := convPartErrToInt(err)
			for p := range latestMeta.Parts {
				dataErrsByPart[p][i] = partErr
			}
		}
	}

	for i, onlineDisk := range onlineDisks {
		if metaErrs[i] != nil {
			continue
		}

		meta := partsMetadata[i]
		if meta.Deleted || meta.IsRemote() {
			continue
		}

		// Always check data, if we got it.
		if (len(meta.Data) > 0 || meta.Size == 0) && len(meta.Parts) > 0 {
			checksumInfo := meta.Erasure.GetChecksumInfo(meta.Parts[0].Number)
			verifyErr := bitrotVerify(bytes.NewReader(meta.Data),
				int64(len(meta.Data)),
				meta.Erasure.ShardFileSize(meta.Size),
				checksumInfo.Algorithm,
				checksumInfo.Hash, meta.Erasure.ShardSize())
			dataErrsByPart[0][i] = convPartErrToInt(verifyErr)
			continue
		}

		var (
			verifyErr  error
			verifyResp *CheckPartsResp
		)

		switch scanMode {
		case madmin.HealDeepScan:
			// disk has a valid xl.meta but may not have all the
			// parts. This is considered an outdated disk, since
			// it needs healing too.
			verifyResp, verifyErr = onlineDisk.VerifyFile(ctx, bucket, object, meta)
		default:
			verifyResp, verifyErr = onlineDisk.CheckParts(ctx, bucket, object, meta)
		}

		for p := range latestMeta.Parts {
			if verifyErr != nil {
				dataErrsByPart[p][i] = convPartErrToInt(verifyErr)
			} else {
				dataErrsByPart[p][i] = verifyResp.Results[p]
			}
		}
	}

	// Build dataErrs by disk from dataErrs by part
	for part, disks := range dataErrsByPart {
		for disk := range disks {
			dataErrsByDisk[disk][part] = dataErrsByPart[part][disk]
		}
	}
	return dataErrsByDisk, dataErrsByPart
}
