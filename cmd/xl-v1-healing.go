/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"path"
	"sync"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/madmin"
)

func (xl xlObjects) HealFormat(dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, errors.Trace(NotImplemented{})
}

// Heals a bucket if it doesn't exist on one of the disks, additionally
// also heals the missing entries for bucket metadata files
// `policy.json, notification.xml, listeners.json`.
func (xl xlObjects) HealBucket(bucket string, dryRun bool) (
	results []madmin.HealResultItem, err error) {

	if err = checkBucketExist(bucket, xl); err != nil {
		return nil, err
	}

	// get write quorum for an object
	writeQuorum := len(xl.getDisks())/2 + 1

	// Heal bucket.
	var result madmin.HealResultItem
	result, err = healBucket(xl.getDisks(), bucket, writeQuorum, dryRun)
	if err != nil {
		return nil, err
	}
	results = append(results, result)

	// Proceed to heal bucket metadata.
	metaResults, err := healBucketMetadata(xl, bucket, dryRun)
	results = append(results, metaResults...)
	return results, err
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(storageDisks []StorageAPI, bucket string, writeQuorum int,
	dryRun bool) (res madmin.HealResultItem, err error) {

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(storageDisks))

	// Disk states slices
	beforeState := make([]string, len(storageDisks))
	afterState := make([]string, len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range storageDisks {
		if disk == nil {
			dErrs[index] = errors.Trace(errDiskNotFound)
			beforeState[index] = madmin.DriveStateOffline
			afterState[index] = madmin.DriveStateOffline
			continue
		}
		wg.Add(1)

		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, err := disk.StatVol(bucket); err != nil {
				if errors.Cause(err) == errDiskNotFound {
					beforeState[index] = madmin.DriveStateOffline
					afterState[index] = madmin.DriveStateOffline
					dErrs[index] = err
					return
				}
				if errors.Cause(err) != errVolumeNotFound {
					beforeState[index] = madmin.DriveStateCorrupt
					afterState[index] = madmin.DriveStateCorrupt
					dErrs[index] = err
					return
				}

				beforeState[index] = madmin.DriveStateMissing
				afterState[index] = madmin.DriveStateMissing

				// mutate only if not a dry-run
				if dryRun {
					return
				}

				makeErr := disk.MakeVol(bucket)
				dErrs[index] = makeErr
				if makeErr == nil {
					afterState[index] = madmin.DriveStateOk
				}
				return
			}
			beforeState[index] = madmin.DriveStateOk
			afterState[index] = madmin.DriveStateOk
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	// Initialize heal result info
	res = madmin.HealResultItem{
		Type:      madmin.HealItemBucket,
		Bucket:    bucket,
		DiskCount: len(storageDisks),
	}
	for i, before := range beforeState {
		if storageDisks[i] == nil {
			res.Before.Drives = append(res.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: "",
				State:    before,
			})
			res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: "",
				State:    afterState[i],
			})
			continue
		}
		drive := storageDisks[i].String()
		res.Before.Drives = append(res.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: drive,
			State:    before,
		})
		res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: drive,
			State:    afterState[i],
		})
	}

	reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, writeQuorum)
	if errors.Cause(reducedErr) == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
	}
	return res, reducedErr
}

// Heals all the metadata associated for a given bucket, this function
// heals `policy.json`, `notification.xml` and `listeners.json`.
func healBucketMetadata(xl xlObjects, bucket string, dryRun bool) (
	results []madmin.HealResultItem, err error) {

	healBucketMetaFn := func(metaPath string) error {
		result, healErr := xl.HealObject(minioMetaBucket, metaPath, dryRun)
		// If object is not found, no result to add.
		if isErrObjectNotFound(healErr) {
			return nil
		}
		if healErr != nil {
			return healErr
		}
		result.Type = madmin.HealItemBucketMetadata
		results = append(results, result)
		return nil
	}

	// Heal `policy.json` for missing entries, ignores if
	// `policy.json` is not found.
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)
	err = healBucketMetaFn(policyPath)
	if err != nil {
		return results, err
	}

	// Heal `notification.xml` for missing entries, ignores if
	// `notification.xml` is not found.
	nConfigPath := path.Join(bucketConfigPrefix, bucket,
		bucketNotificationConfig)
	err = healBucketMetaFn(nConfigPath)
	if err != nil {
		return results, err
	}

	// Heal `listeners.json` for missing entries, ignores if
	// `listeners.json` is not found.
	lConfigPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	err = healBucketMetaFn(lConfigPath)
	return results, err
}

// listAllBuckets lists all buckets from all disks. It also
// returns the occurrence of each buckets in all disks
func listAllBuckets(storageDisks []StorageAPI) (buckets map[string]VolInfo,
	bucketsOcc map[string]int, err error) {

	buckets = make(map[string]VolInfo)
	bucketsOcc = make(map[string]int)
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		var volsInfo []VolInfo
		volsInfo, err = disk.ListVols()
		if err != nil {
			if errors.IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
				continue
			}
			return nil, nil, err
		}
		for _, volInfo := range volsInfo {
			// StorageAPI can send volume names which are
			// incompatible with buckets - these are
			// skipped, like the meta-bucket.
			if !IsValidBucketName(volInfo.Name) ||
				isMinioMetaBucketName(volInfo.Name) {
				continue
			}
			// Increase counter per bucket name
			bucketsOcc[volInfo.Name]++
			// Save volume info under bucket name
			buckets[volInfo.Name] = volInfo
		}
	}
	return buckets, bucketsOcc, nil
}

// Heals an object by re-writing corrupt/missing erasure blocks.
func healObject(storageDisks []StorageAPI, bucket string, object string,
	quorum int, dryRun bool) (result madmin.HealResultItem, err error) {

	partsMetadata, errs := readAllXLMetadata(storageDisks, bucket, object)

	// readQuorum suffices for xl.json since we use monotonic
	// system time to break the tie when a split-brain situation
	// arises.
	if reducedErr := reduceReadQuorumErrs(errs, nil, quorum); reducedErr != nil {
		return result, toObjectErr(reducedErr, bucket, object)
	}

	// List of disks having latest version of the object xl.json
	// (by modtime).
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// List of disks having all parts as per latest xl.json.
	availableDisks, dataErrs, aErr := disksWithAllParts(latestDisks, partsMetadata, errs, bucket, object)
	if aErr != nil {
		return result, toObjectErr(aErr, bucket, object)
	}

	// Initialize heal result object
	result = madmin.HealResultItem{
		Type:      madmin.HealItemObject,
		Bucket:    bucket,
		Object:    object,
		DiskCount: len(storageDisks),

		// Initialize object size to -1, so we can detect if we are
		// unable to reliably find the object size.
		ObjectSize: -1,
	}

	// Loop to find number of disks with valid data, per-drive
	// data state and a list of outdated disks on which data needs
	// to be healed.
	outDatedDisks := make([]StorageAPI, len(storageDisks))
	numAvailableDisks := 0
	disksToHealCount := 0
	for i, v := range availableDisks {
		driveState := ""
		switch {
		case v != nil:
			driveState = madmin.DriveStateOk
			numAvailableDisks++
			// If data is sane on any one disk, we can
			// extract the correct object size.
			result.ObjectSize = partsMetadata[i].Stat.Size
			result.ParityBlocks = partsMetadata[i].Erasure.ParityBlocks
			result.DataBlocks = partsMetadata[i].Erasure.DataBlocks
		case errors.Cause(errs[i]) == errDiskNotFound:
			driveState = madmin.DriveStateOffline
		case errors.Cause(errs[i]) == errFileNotFound, errors.Cause(errs[i]) == errVolumeNotFound:
			fallthrough
		case errors.Cause(dataErrs[i]) == errFileNotFound, errors.Cause(dataErrs[i]) == errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		default:
			// all remaining cases imply corrupt data/metadata
			driveState = madmin.DriveStateCorrupt
		}

		// an online disk without valid data/metadata is
		// outdated and can be healed.
		if errs[i] != errDiskNotFound && v == nil {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
		}
		if v == nil {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: "",
				State:    driveState,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: "",
				State:    driveState,
			})
			continue
		}
		drive := v.String()
		result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: drive,
			State:    driveState,
		})
		result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: drive,
			State:    driveState,
		})
	}

	// If less than read quorum number of disks have all the parts
	// of the data, we can't reconstruct the erasure-coded data.
	if numAvailableDisks < quorum {
		return result, toObjectErr(errXLReadQuorum, bucket, object)
	}

	if disksToHealCount == 0 {
		// Nothing to heal!
		return result, nil
	}

	// After this point, only have to repair data on disk - so
	// return if it is a dry-run
	if dryRun {
		return result, nil
	}

	// Latest xlMetaV1 for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, pErr := pickValidXLMeta(partsMetadata, modTime)
	if pErr != nil {
		return result, toObjectErr(pErr, bucket, object)
	}

	// Clear data files of the object on outdated disks
	for _, disk := range outDatedDisks {
		// Before healing outdated disks, we need to remove
		// xl.json and part files from "bucket/object/" so
		// that rename(minioMetaBucket, "tmp/tmpuuid/",
		// "bucket", "object/") succeeds.
		if disk == nil {
			// Not an outdated disk.
			continue
		}

		// List and delete the object directory, ignoring
		// errors.
		files, derr := disk.ListDir(bucket, object)
		if derr == nil {
			for _, entry := range files {
				_ = disk.DeleteFile(bucket,
					pathJoin(object, entry))
			}
		}
	}

	// Reorder so that we have data disks first and parity disks next.
	latestDisks = shuffleDisks(latestDisks, latestMeta.Erasure.Distribution)
	outDatedDisks = shuffleDisks(outDatedDisks, latestMeta.Erasure.Distribution)
	partsMetadata = shufflePartsMetadata(partsMetadata, latestMeta.Erasure.Distribution)

	// We write at temporary location and then rename to final location.
	tmpID := mustGetUUID()

	// Checksum of the part files. checkSumInfos[index] will
	// contain checksums of all the part files in the
	// outDatedDisks[index]
	checksumInfos := make([][]ChecksumInfo, len(outDatedDisks))

	// Heal each part. erasureHealFile() will write the healed
	// part to .minio/tmp/uuid/ which needs to be renamed later to
	// the final location.
	storage, err := NewErasureStorage(latestDisks, latestMeta.Erasure.DataBlocks,
		latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
	if err != nil {
		return result, toObjectErr(err, bucket, object)
	}
	checksums := make([][]byte, len(latestDisks))
	for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
		partName := latestMeta.Parts[partIndex].Name
		partSize := latestMeta.Parts[partIndex].Size
		erasure := latestMeta.Erasure
		var algorithm BitrotAlgorithm
		for i, disk := range storage.disks {
			if disk != OfflineDisk {
				info := partsMetadata[i].Erasure.GetChecksumInfo(partName)
				algorithm = info.Algorithm
				checksums[i] = info.Hash
			}
		}
		// Heal the part file.
		file, hErr := storage.HealFile(outDatedDisks, bucket, pathJoin(object, partName),
			erasure.BlockSize, minioMetaTmpBucket, pathJoin(tmpID, partName), partSize,
			algorithm, checksums)
		if hErr != nil {
			return result, toObjectErr(hErr, bucket, object)
		}
		// outDatedDisks that had write errors should not be
		// written to for remaining parts, so we nil it out.
		for i, disk := range outDatedDisks {
			if disk == nil {
				continue
			}
			// A non-nil stale disk which did not receive
			// a healed part checksum had a write error.
			if file.Checksums[i] == nil {
				outDatedDisks[i] = nil
				disksToHealCount--
				continue
			}
			// append part checksums
			checksumInfos[i] = append(checksumInfos[i],
				ChecksumInfo{partName, file.Algorithm, file.Checksums[i]})
		}

		// If all disks are having errors, we give up.
		if disksToHealCount == 0 {
			return result, fmt.Errorf("all disks without up-to-date data had write errors")
		}
	}

	// xl.json should be written to all the healed disks.
	for index, disk := range outDatedDisks {
		if disk == nil {
			continue
		}
		partsMetadata[index] = latestMeta
		partsMetadata[index].Erasure.Checksums = checksumInfos[index]
	}

	// Generate and write `xl.json` generated from other disks.
	outDatedDisks, aErr = writeUniqueXLMetadata(outDatedDisks, minioMetaTmpBucket, tmpID,
		partsMetadata, diskCount(outDatedDisks))
	if aErr != nil {
		return result, toObjectErr(aErr, bucket, object)
	}

	// Rename from tmp location to the actual location.
	for diskIndex, disk := range outDatedDisks {
		if disk == nil {
			continue
		}

		// Attempt a rename now from healed data to final location.
		aErr = disk.RenameFile(minioMetaTmpBucket, retainSlash(tmpID), bucket,
			retainSlash(object))
		if aErr != nil {
			return result, toObjectErr(errors.Trace(aErr), bucket, object)
		}

		realDiskIdx := unshuffleIndex(diskIndex, latestMeta.Erasure.Distribution)
		if outDatedDisks[realDiskIdx] != nil {
			for i, v := range result.After.Drives {
				if v.Endpoint == outDatedDisks[realDiskIdx].String() {
					result.After.Drives[i].State = madmin.DriveStateOk
				}
			}
		}
	}

	// Set the size of the object in the heal result
	result.ObjectSize = latestMeta.Stat.Size

	return result, nil
}

// HealObject - heal the given object.
//
// FIXME: If an object object was deleted and one disk was down,
// and later the disk comes back up again, heal on the object
// should delete it.
func (xl xlObjects) HealObject(bucket, object string, dryRun bool) (hr madmin.HealResultItem, err error) {

	// FIXME: Metadata is read again in the healObject() call below.
	// Read metadata files from all the disks
	partsMetadata, errs := readAllXLMetadata(xl.getDisks(), bucket, object)

	// get read quorum for this object
	var readQuorum int
	readQuorum, _, err = objectQuorumFromMeta(xl, partsMetadata, errs)
	if err != nil {
		return hr, err
	}

	// Lock the object before healing.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if lerr := objectLock.GetRLock(globalHealingTimeout); lerr != nil {
		return hr, lerr
	}
	defer objectLock.RUnlock()

	// Heal the object.
	return healObject(xl.getDisks(), bucket, object, readQuorum, dryRun)
}
