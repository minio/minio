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
)

// healFormatXL - heals missing `format.json` on freshly or corrupted
// disks (missing format.json but does have erasure coded data in it).
func healFormatXL(storageDisks []StorageAPI) (err error) {
	// Attempt to load all `format.json`.
	formatConfigs, sErrs := loadAllFormats(storageDisks)

	// Generic format check.
	// - if (no quorum) return error
	// - if (disks not recognized) // Always error.
	if err = genericFormatCheck(formatConfigs, sErrs); err != nil {
		return err
	}

	// Handles different cases properly.
	switch reduceFormatErrs(sErrs, len(storageDisks)) {
	case errCorruptedFormat:
		if err = healFormatXLCorruptedDisks(storageDisks); err != nil {
			return fmt.Errorf("Unable to repair corrupted format, %s", err)
		}
	case errSomeDiskUnformatted:
		// All drives online but some report missing format.json.
		if err = healFormatXLFreshDisks(storageDisks); err != nil {
			// There was an unexpected unrecoverable error during healing.
			return fmt.Errorf("Unable to heal backend %s", err)
		}
	case errSomeDiskOffline:
		// FIXME: in future.
		return fmt.Errorf("Unable to initialize format %s and %s", errSomeDiskOffline, errSomeDiskUnformatted)
	}
	return nil
}

// Heals a bucket if it doesn't exist on one of the disks, additionally
// also heals the missing entries for bucket metadata files
// `policy.json, notification.xml, listeners.json`.
func (xl xlObjects) HealBucket(bucket string) error {
	if err := checkBucketExist(bucket, xl); err != nil {
		return err
	}

	// Heal bucket.
	if err := healBucket(xl.storageDisks, bucket, xl.writeQuorum); err != nil {
		return err
	}

	// Proceed to heal bucket metadata.
	return healBucketMetadata(xl.storageDisks, bucket, xl.readQuorum)
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(storageDisks []StorageAPI, bucket string, writeQuorum int) error {
	bucketLock := nsMutex.NewNSLock(bucket, "")
	bucketLock.Lock()
	defer bucketLock.Unlock()

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range storageDisks {
		if disk == nil {
			dErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, err := disk.StatVol(bucket); err != nil {
				if err != errVolumeNotFound {
					dErrs[index] = traceError(err)
					return
				}
				if err = disk.MakeVol(bucket); err != nil {
					dErrs[index] = traceError(err)
				}
			}
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	// Do we have write quorum?.
	if !isDiskQuorum(dErrs, writeQuorum) {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
		return toObjectErr(traceError(errXLWriteQuorum), bucket)
	}

	// Verify we have any other errors which should be returned as failure.
	if reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, writeQuorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket)
	}
	return nil
}

// Heals all the metadata associated for a given bucket, this function
// heals `policy.json`, `notification.xml` and `listeners.json`.
func healBucketMetadata(storageDisks []StorageAPI, bucket string, readQuorum int) error {
	healBucketMetaFn := func(metaPath string) error {
		metaLock := nsMutex.NewNSLock(minioMetaBucket, metaPath)
		metaLock.RLock()
		defer metaLock.RUnlock()
		// Heals the given file at metaPath.
		if err := healObject(storageDisks, minioMetaBucket, metaPath, readQuorum); err != nil && !isErrObjectNotFound(err) {
			return err
		} // Success.
		return nil
	}

	// Heal `policy.json` for missing entries, ignores if `policy.json` is not found.
	policyPath := pathJoin(bucketConfigPrefix, bucket, policyJSON)
	if err := healBucketMetaFn(policyPath); err != nil {
		return err
	}

	// Heal `notification.xml` for missing entries, ignores if `notification.xml` is not found.
	nConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	if err := healBucketMetaFn(nConfigPath); err != nil {
		return err
	}

	// Heal `listeners.json` for missing entries, ignores if `listeners.json` is not found.
	lConfigPath := path.Join(bucketConfigPrefix, bucket, bucketListenerConfig)
	return healBucketMetaFn(lConfigPath)
}

// listBucketNames list all bucket names from all disks to heal.
func listBucketNames(storageDisks []StorageAPI) (bucketNames map[string]struct{}, err error) {
	bucketNames = make(map[string]struct{})
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}
		var volsInfo []VolInfo
		volsInfo, err = disk.ListVols()
		if err == nil {
			for _, volInfo := range volsInfo {
				// StorageAPI can send volume names which are
				// incompatible with buckets, handle it and skip them.
				if !IsValidBucketName(volInfo.Name) {
					continue
				}
				// Ignore the volume special bucket.
				if volInfo.Name == minioMetaBucket {
					continue
				}
				bucketNames[volInfo.Name] = struct{}{}
			}
			continue
		}
		// Ignore any disks not found.
		if isErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return bucketNames, err
}

// This function is meant for all the healing that needs to be done
// during startup i.e healing of buckets, bucket metadata (policy.json,
// notification.xml, listeners.json) etc. Currently this function
// supports quick healing of buckets, bucket metadata.
//
// TODO :-
// - add support for healing dangling `uploads.json`.
// - add support for healing dangling `xl.json`.
func quickHeal(storageDisks []StorageAPI, writeQuorum int, readQuorum int) error {
	// List all bucket names from all disks.
	bucketNames, err := listBucketNames(storageDisks)
	if err != nil {
		return err
	}
	// All bucket names and bucket metadata should be healed.
	for bucketName := range bucketNames {
		// Heal bucket and then proceed to heal bucket metadata.
		if err = healBucket(storageDisks, bucketName, writeQuorum); err == nil {
			if err = healBucketMetadata(storageDisks, bucketName, readQuorum); err == nil {
				continue
			}
			return err
		}
		return err
	}
	return nil
}

// Heals an object only the corrupted/missing erasure blocks.
func healObject(storageDisks []StorageAPI, bucket string, object string, quorum int) error {
	partsMetadata, errs := readAllXLMetadata(storageDisks, bucket, object)
	if reducedErr := reduceReadQuorumErrs(errs, nil, quorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket, object)
	}

	if !xlShouldHeal(partsMetadata, errs) {
		// There is nothing to heal.
		return nil
	}

	// List of disks having latest version of the object.
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)
	// List of disks having outdated version of the object or missing object.
	outDatedDisks := outDatedDisks(storageDisks, partsMetadata, errs)
	// Latest xlMetaV1 for reference. If a valid metadata is not present, it is as good as object not found.
	latestMeta, pErr := pickValidXLMeta(partsMetadata, modTime)
	if pErr != nil {
		return pErr
	}

	for index, disk := range outDatedDisks {
		// Before healing outdated disks, we need to remove xl.json
		// and part files from "bucket/object/" so that
		// rename(".minio.sys", "tmp/tmpuuid/", "bucket", "object/") succeeds.
		if disk == nil {
			// Not an outdated disk.
			continue
		}
		if errs[index] != nil {
			// If there was an error (most likely errFileNotFound)
			continue
		}
		// Outdated object with the same name exists that needs to be deleted.
		outDatedMeta := partsMetadata[index]
		// Delete all the parts.
		for partIndex := 0; partIndex < len(outDatedMeta.Parts); partIndex++ {
			err := disk.DeleteFile(bucket, pathJoin(object, outDatedMeta.Parts[partIndex].Name))
			if err != nil {
				return traceError(err)
			}
		}
		// Delete xl.json file.
		err := disk.DeleteFile(bucket, pathJoin(object, xlMetaJSONFile))
		if err != nil {
			return traceError(err)
		}
	}

	// Reorder so that we have data disks first and parity disks next.
	latestDisks = getOrderedDisks(latestMeta.Erasure.Distribution, latestDisks)
	outDatedDisks = getOrderedDisks(latestMeta.Erasure.Distribution, outDatedDisks)
	partsMetadata = getOrderedPartsMetadata(latestMeta.Erasure.Distribution, partsMetadata)

	// We write at temporary location and then rename to fianal location.
	tmpID := mustGetUUID()

	// Checksum of the part files. checkSumInfos[index] will contain checksums
	// of all the part files in the outDatedDisks[index]
	checkSumInfos := make([][]checkSumInfo, len(outDatedDisks))

	// Heal each part. erasureHealFile() will write the healed part to
	// .minio/tmp/uuid/ which needs to be renamed later to the final location.
	for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
		partName := latestMeta.Parts[partIndex].Name
		partSize := latestMeta.Parts[partIndex].Size
		erasure := latestMeta.Erasure
		sumInfo := latestMeta.Erasure.GetCheckSumInfo(partName)
		// Heal the part file.
		checkSums, err := erasureHealFile(latestDisks, outDatedDisks,
			bucket, pathJoin(object, partName),
			minioMetaTmpBucket, pathJoin(tmpID, partName),
			partSize, erasure.BlockSize, erasure.DataBlocks, erasure.ParityBlocks, sumInfo.Algorithm)
		if err != nil {
			return err
		}
		for index, sum := range checkSums {
			if outDatedDisks[index] != nil {
				checkSumInfos[index] = append(checkSumInfos[index], checkSumInfo{
					Name:      partName,
					Algorithm: sumInfo.Algorithm,
					Hash:      sum,
				})
			}
		}
	}

	// xl.json should be written to all the healed disks.
	for index, disk := range outDatedDisks {
		if disk == nil {
			continue
		}
		partsMetadata[index] = latestMeta
		partsMetadata[index].Erasure.Checksum = checkSumInfos[index]
	}

	// Generate and write `xl.json` generated from other disks.
	err := writeUniqueXLMetadata(outDatedDisks, minioMetaTmpBucket, tmpID, partsMetadata, diskCount(outDatedDisks))
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Rename from tmp location to the actual location.
	for _, disk := range outDatedDisks {
		if disk == nil {
			continue
		}
		// Remove any lingering partial data from current namespace.
		err = disk.DeleteFile(bucket, retainSlash(object))
		if err != nil && err != errFileNotFound {
			return traceError(err)
		}
		// Attempt a rename now from healed data to final location.
		err = disk.RenameFile(minioMetaTmpBucket, retainSlash(tmpID), bucket, retainSlash(object))
		if err != nil {
			return traceError(err)
		}
	}
	return nil
}

// HealObject heals a given object for all its missing entries.
// FIXME: If an object object was deleted and one disk was down,
// and later the disk comes back up again, heal on the object
// should delete it.
func (xl xlObjects) HealObject(bucket, object string) error {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return err
	}

	// Lock the object before healing.
	objectLock := nsMutex.NewNSLock(bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	// Heal the object.
	return healObject(xl.storageDisks, bucket, object, xl.readQuorum)
}
