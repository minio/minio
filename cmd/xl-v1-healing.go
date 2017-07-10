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
	res = madmin.HealResultItem{Type: madmin.HealItemMetadata, Detail: "disk-format"}
	res.InitDrives()
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formatConfigs {
		drive := globalEndpoints.GetString(i)
		switch {
		case format != nil:
			res.DriveInfo.Before[drive] = madmin.OnlineDriveState
		case sErrs[i] == errCorruptedFormat:
			res.DriveInfo.Before[drive] = madmin.CorruptDriveState
		default:
			res.DriveInfo.Before[drive] = madmin.OfflineDriveState
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
				res.DriveInfo.After[drive] = madmin.OnlineDriveState
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
				res.DriveInfo.After[drive] = madmin.OnlineDriveState
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
				res.DriveInfo.After[drive] = madmin.OnlineDriveState
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

	// Heal bucket.
	result, err := healBucket(xl.storageDisks, bucket, xl.writeQuorum,
		dryRun)
	if err != nil {
		return results, err
	}
	results = append(results, result)

	// Proceed to heal bucket metadata.
	metaResults, err := healBucketMetadata(xl.storageDisks, bucket,
		xl.readQuorum, dryRun)
	results = append(results, metaResults...)
	return results, err
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(storageDisks []StorageAPI, bucket string, writeQuorum int,
	dryRun bool) (res madmin.HealResultItem, err error) {

	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalHealingTimeout); err != nil {
		return res, err
	}
	defer bucketLock.Unlock()

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(storageDisks))

	// Initialize heal result info
	res.Type = madmin.HealItemBucket
	res.Bucket = bucket
	res.InitDrives()
	beforeOnline := make([]bool, len(storageDisks))
	beforeOffline := make([]bool, len(storageDisks))
	afterOnline := make([]bool, len(storageDisks))
	afterOffline := make([]bool, len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range storageDisks {
		if disk == nil {
			dErrs[index] = errors.Trace(errDiskNotFound)
			beforeOffline[index] = true
			afterOffline[index] = true
			continue
		}
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, err := disk.StatVol(bucket); err != nil {
				beforeOffline[index] = true
				afterOffline[index] = true
				if err != errVolumeNotFound {
					dErrs[index] = errors.Trace(err)
					return
				}
				// mutate only if not a dry-run
				if dryRun {
					return
				}
				makeErr := disk.MakeVol(bucket)
				dErrs[index] = errors.Trace(makeErr)
				afterOffline[index] = makeErr != nil
				afterOnline[index] = makeErr == nil
			} else {
				beforeOnline[index] = true
				afterOnline[index] = true
			}
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	for i := 0; i < len(storageDisks); i++ {
		drive := globalEndpoints.GetString(i)
		if beforeOffline[i] {
			res.DriveInfo.Before[drive] = madmin.OfflineDriveState
		}
		if beforeOnline[i] {
			res.DriveInfo.Before[drive] = madmin.OnlineDriveState
		}
		if afterOffline[i] {
			res.DriveInfo.After[drive] = madmin.OfflineDriveState
		}
		if afterOnline[i] {
			res.DriveInfo.After[drive] = madmin.OnlineDriveState
		}
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
func healBucketMetadata(storageDisks []StorageAPI, bucket string,
	readQuorum int, dryRun bool) (results []madmin.HealResultItem, err error) {

	healBucketMetaFn := func(metaPath string) error {
		metaLock := globalNSMutex.NewNSLock(minioMetaBucket, metaPath)
		if lerr := metaLock.GetRLock(globalHealingTimeout); lerr != nil {
			return lerr
		}
		defer metaLock.RUnlock()
		// Heals the given file at metaPath.
		result, lerr := healObject(storageDisks, minioMetaBucket,
			metaPath, readQuorum, dryRun)
		// If object is not found, no result to add.
		if isErrObjectNotFound(lerr) {
			return nil
		}
		if lerr != nil {
			return lerr
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
	buckets, occ, err := listAllBuckets(xl.storageDisks)
	if err != nil {
		return listBuckets, err
	}

	// Iterate over all buckets
	for k, currBucket := range buckets {
		// skip buckets occurring in less than read-quorum
		// disks
		if occ[k] < xl.readQuorum {
			continue
		}

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
func quickHeal(storageDisks []StorageAPI, writeQuorum int, readQuorum int) error {
	// List all bucket name occurrence from all disks.
	_, bucketOcc, err := listAllBuckets(storageDisks)
	if err != nil {
		return err
	}

	// All bucket names and bucket metadata that should be healed.
	for bucketName, occCount := range bucketOcc {
		// Heal bucket only if healing is needed.
		if occCount != len(storageDisks) {
			// Heal bucket and then proceed to heal bucket metadata if any.
			if _, err = healBucket(storageDisks, bucketName, writeQuorum, false); err == nil {
				if _, err = healBucketMetadata(storageDisks, bucketName, readQuorum, false); err == nil {
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

// Heals an object only the corrupted/missing erasure blocks.
func healObject(storageDisks []StorageAPI, bucket string, object string,
	quorum int, dryRun bool) (result madmin.HealResultItem, err error) {

	partsMetadata, errs := readAllXLMetadata(storageDisks, bucket, object)
	// readQuorum suffices for xl.json since we use monotonic
	// system time to break the tie when a split-brain situation
	// arises.
	if reducedErr := reduceReadQuorumErrs(errs, nil, quorum); reducedErr != nil {
		return result, toObjectErr(reducedErr, bucket, object)
	}

	// List of disks having latest version of the object.
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// List of disks having all parts as per latest xl.json.
	availableDisks, errs, aErr := disksWithAllParts(latestDisks, partsMetadata, errs, bucket, object)
	if aErr != nil {
		return result, toObjectErr(aErr, bucket, object)
	}

	// Initialize heal result object
	result.Type = madmin.HealItemObject
	result.Bucket = bucket
	result.Object = object
	result.ObjectSize = partsMetadata[0].Stat.Size
	result.InitDrives()
	numAvailableDisks := 0
	for i, v := range availableDisks {
		drive := globalEndpoints.GetString(i)
		if v == nil {
			if latestDisks[i] == nil {
				// drives without latest version of
				// object is considered offline.
				result.DriveInfo.Before[drive] = madmin.OfflineDriveState
			} else {
				// drives with latest version of
				// xl.json but not having valid object
				// data (i.e due to bit)
				result.DriveInfo.Before[drive] = madmin.CorruptDriveState
			}
			continue
		}

		result.DriveInfo.Before[drive] = madmin.OnlineDriveState
		numAvailableDisks++
	}
	// Before heal drive-state is now initialized. Make a copy for
	// "after" drive-state.
	for k, v := range result.DriveInfo.Before {
		result.DriveInfo.After[k] = v
	}

	if numAvailableDisks == len(storageDisks) {
		// Nothing to heal!
		return result, nil
	}

	// If less than read quorum number of disks have all the parts
	// of the data, we can't reconstruct the erasure-coded data.
	if numAvailableDisks < quorum {
		return result, toObjectErr(errXLReadQuorum, bucket, object)
	}

	// After this point, only have to repair data on disk - so
	// return if it is a dry-run
	if dryRun {
		return result, nil
	}

	// List of disks having outdated version of the object or missing object.
	outDatedDisks := outDatedDisks(storageDisks, availableDisks, errs,
		partsMetadata, bucket, object)

	// Latest xlMetaV1 for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, pErr := pickValidXLMeta(partsMetadata, modTime)
	if pErr != nil {
		return result, toObjectErr(pErr, bucket, object)
	}

	disksToHealCount := 0
	for index, disk := range outDatedDisks {
		// Before healing outdated disks, we need to remove
		// xl.json and part files from "bucket/object/" so
		// that rename(minioMetaBucket, "tmp/tmpuuid/",
		// "bucket", "object/") succeeds.
		if disk == nil {
			// Not an outdated disk.
			continue
		}

		disksToHealCount++

		// errFileNotFound implies that xl.json is missing. We
		// may have object parts still present in the object
		// directory. This needs to be deleted for object to
		// healed successfully.
		if errs[index] != nil && !errors.IsErr(errs[index], errFileNotFound) {
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
		latestMeta.Erasure.ParityBlocks)
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
		result.DriveInfo.After[drive] = madmin.OnlineDriveState
	}

	// Set the size of the object in the heal result
	result.ObjectSize = latestMeta.Stat.Size

	return result, nil
}

// HealObject - heal the given object. Returns list of disk indices on
// which latest data is present.
//
// FIXME: If an object object was deleted and one disk was down,
// and later the disk comes back up again, heal on the object
// should delete it.
func (xl xlObjects) HealObject(bucket, object string, dryRun bool) (
	hr madmin.HealResultItem, err error) {

	// Lock the object before healing.
	objectLock := globalNSMutex.NewNSLock(bucket, object)
	if lerr := objectLock.GetRLock(globalHealingTimeout); lerr != nil {
		return hr, lerr
	}
	defer objectLock.RUnlock()

	// Heal the object.
	return healObject(xl.storageDisks, bucket, object, xl.readQuorum, dryRun)
}
