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
	"sort"
	"sync"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/madmin"
)

// healFormatXL - heals missing `format.json` on freshly or corrupted
// disks (missing format.json but does have erasure coded data in it).
func healFormatXL(storageDisks []StorageAPI, dryRun bool) (res madmin.HealResultItem,
	err error) {

	// Attempt to load all `format.json`.
	formatConfigs, sErrs := loadAllFormats(storageDisks)

	// Generic format check.
	// - if (no quorum) return error
	// - if (disks not recognized) // Always error.
	if err = genericFormatCheckXL(formatConfigs, sErrs); err != nil {
		return res, err
	}

	// Prepare heal-result
	res = madmin.HealResultItem{
		Type:      madmin.HealItemMetadata,
		Detail:    "disk-format",
		DiskCount: len(storageDisks),
	}
	res.InitDrives()
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formatConfigs {
		drive := globalEndpoints.GetString(i)
		switch {
		case format != nil:
			res.DriveInfo.Before[drive] = madmin.DriveStateOk
		case sErrs[i] == errCorruptedFormat:
			res.DriveInfo.Before[drive] = madmin.DriveStateCorrupt
		case sErrs[i] == errUnformattedDisk:
			res.DriveInfo.Before[drive] = madmin.DriveStateMissing
		default:
			res.DriveInfo.Before[drive] = madmin.DriveStateOffline
		}
	}
	// Copy "after" drive state too
	for k, v := range res.DriveInfo.Before {
		res.DriveInfo.After[k] = v
	}

	numDisks := len(storageDisks)
	_, unformattedDiskCount, diskNotFoundCount,
		corruptedFormatCount, otherErrCount := formatErrsSummary(sErrs)

	switch {
	case unformattedDiskCount == numDisks:
		// all unformatted.
		if !dryRun {
			err = initFormatXL(storageDisks)
			if err != nil {
				return res, err
			}
			for i := 0; i < len(storageDisks); i++ {
				drive := globalEndpoints.GetString(i)
				res.DriveInfo.After[drive] = madmin.DriveStateOk
			}
		}
		return res, nil

	case diskNotFoundCount > 0:
		return res, fmt.Errorf("cannot proceed with heal as %s",
			errSomeDiskOffline)

	case otherErrCount > 0:
		return res, fmt.Errorf("cannot proceed with heal as some disks had unhandled errors")

	case corruptedFormatCount > 0:
		// heal corrupted disks
		err = healFormatXLCorruptedDisks(storageDisks, formatConfigs,
			dryRun)
		if err != nil {
			return res, err
		}
		// success
		if !dryRun {
			for i := 0; i < len(storageDisks); i++ {
				drive := globalEndpoints.GetString(i)
				res.DriveInfo.After[drive] = madmin.DriveStateOk
			}
		}
		return res, nil

	case unformattedDiskCount > 0:
		// heal unformatted disks
		err = healFormatXLFreshDisks(storageDisks, formatConfigs,
			dryRun)
		if err != nil {
			return res, err
		}
		// success
		if !dryRun {
			for i := 0; i < len(storageDisks); i++ {
				drive := globalEndpoints.GetString(i)
				res.DriveInfo.After[drive] = madmin.DriveStateOk
			}
		}
		return res, nil
	}

	return res, nil
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
	writeQuorum := len(xl.storageDisks)/2 + 1
	bucketLock := xl.nsMutex.NewNSLock(bucket, "")
	if err = bucketLock.GetLock(globalHealingTimeout); err != nil {
		return nil, err
	}
	defer bucketLock.Unlock()

	// Heal bucket.
	result, err := healBucket(xl.storageDisks, bucket, writeQuorum, dryRun)
	if err != nil {
		return results, err
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
				if errors.Cause(err) != errVolumeNotFound {
					beforeState[index] = madmin.DriveStateCorrupt
					afterState[index] = madmin.DriveStateCorrupt
					dErrs[index] = errors.Trace(err)
					return
				}

				beforeState[index] = madmin.DriveStateMissing
				afterState[index] = madmin.DriveStateMissing

				// mutate only if not a dry-run
				if dryRun {
					return
				}

				makeErr := disk.MakeVol(bucket)
				dErrs[index] = errors.Trace(makeErr)
				if makeErr == nil {
					afterState[index] = madmin.DriveStateOk
				}
			} else {
				beforeState[index] = madmin.DriveStateOk
				afterState[index] = madmin.DriveStateOk
			}
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
	res.InitDrives()
	for i, before := range beforeState {
		drive := globalEndpoints.GetString(i)
		res.DriveInfo.Before[drive] = before
		res.DriveInfo.After[drive] = afterState[i]
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
			break
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
	return buckets, bucketsOcc, err
}

// ListBucketsHeal - Find all buckets that need to be healed
func (xl xlObjects) ListBucketsHeal() ([]BucketInfo, error) {
	listBuckets := []BucketInfo{}
	// List all buckets that can be found in all disks
	buckets, _, err := listAllBuckets(xl.storageDisks)
	if err != nil {
		return listBuckets, err
	}

	// Iterate over all buckets
	for _, currBucket := range buckets {
		listBuckets = append(listBuckets,
			BucketInfo{currBucket.Name, currBucket.Created})
	}

	// Sort found buckets
	sort.Sort(byBucketName(listBuckets))
	return listBuckets, nil
}

// This function is meant for all the healing that needs to be done
// during startup i.e healing of buckets, bucket metadata (policy.json,
// notification.xml, listeners.json) etc. Currently this function
// supports quick healing of buckets, bucket metadata.
func quickHeal(xlObj xlObjects, writeQuorum int, readQuorum int) error {
	// List all bucket name occurrence from all disks.
	_, bucketOcc, err := listAllBuckets(xlObj.storageDisks)
	if err != nil {
		return err
	}

	// All bucket names and bucket metadata that should be healed.
	for bucketName, occCount := range bucketOcc {
		// Heal bucket only if healing is needed.
		if occCount != len(xlObj.storageDisks) {
			bucketLock := xlObj.nsMutex.NewNSLock(bucketName, "")
			if perr := bucketLock.GetLock(globalHealingTimeout); perr != nil {
				return perr
			}
			defer bucketLock.Unlock()

			// Heal bucket and then proceed to heal bucket metadata if any.
			if _, err = healBucket(xlObj.storageDisks, bucketName, writeQuorum, false); err == nil {
				if _, err = healBucketMetadata(xlObj, bucketName, false); err == nil {
					continue
				}
				return err
			}
			return err
		}
	}

	// Success.
	return nil
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
	result.InitDrives()

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
		drive := globalEndpoints.GetString(i)
		result.DriveInfo.Before[drive] = driveState
		// copy for 'after' state
		result.DriveInfo.After[drive] = driveState

		// an online disk without valid data/metadata is
		// outdated and can be healed.
		if errs[i] != errDiskNotFound && v == nil {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
		}
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

		realDiskIdx := unshuffleIndex(diskIndex,
			latestMeta.Erasure.Distribution)
		drive := globalEndpoints.GetString(realDiskIdx)
		result.DriveInfo.After[drive] = madmin.DriveStateOk
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
func (xl xlObjects) HealObject(bucket, object string, dryRun bool) (
	hr madmin.HealResultItem, err error) {

	// FIXME: Metadata is read again in the healObject() call below.
	// Read metadata files from all the disks
	partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, object)

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
	return healObject(xl.storageDisks, bucket, object, readQuorum, dryRun)
}
