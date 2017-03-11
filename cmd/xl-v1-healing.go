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
)

// healFormatXL - heals missing `format.json` on freshly or corrupted
// disks (missing format.json but does have erasure coded data in it).
func healFormatXL(storageDisks []StorageAPI) (err error) {
	// Attempt to load all `format.json`.
	formatConfigs, sErrs := loadAllFormats(storageDisks)

	// Generic format check.
	// - if (no quorum) return error
	// - if (disks not recognized) // Always error.
	if err = genericFormatCheckXL(formatConfigs, sErrs); err != nil {
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
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
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

	reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, writeQuorum)
	if errorCause(reducedErr) == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
	}
	return reducedErr
}

// Heals all the metadata associated for a given bucket, this function
// heals `policy.json`, `notification.xml` and `listeners.json`.
func healBucketMetadata(storageDisks []StorageAPI, bucket string, readQuorum int) error {
	healBucketMetaFn := func(metaPath string) error {
		metaLock := globalNSMutex.NewNSLock(minioMetaBucket, metaPath)
		metaLock.RLock()
		defer metaLock.RUnlock()
		// Heals the given file at metaPath.
		if err := healObject(storageDisks, minioMetaBucket, metaPath, readQuorum); err != nil && !isErrObjectNotFound(err) {
			return err
		} // Success.
		return nil
	}

	// Heal `policy.json` for missing entries, ignores if `policy.json` is not found.
	policyPath := pathJoin(bucketConfigPrefix, bucket, bucketPolicyConfig)
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

// listAllBuckets lists all buckets from all disks. It also
// returns the occurrence of each buckets in all disks
func listAllBuckets(storageDisks []StorageAPI) (buckets map[string]VolInfo, bucketsOcc map[string]int, err error) {
	buckets = make(map[string]VolInfo)
	bucketsOcc = make(map[string]int)
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
				// Skip special volume buckets.
				if isMinioMetaBucketName(volInfo.Name) {
					continue
				}
				// Increase counter per bucket name
				bucketsOcc[volInfo.Name]++
				// Save volume info under bucket name
				buckets[volInfo.Name] = volInfo
			}
			continue
		}
		// Ignore any disks not found.
		if isErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return buckets, bucketsOcc, err
}

// reduceHealStatus - fetches the worst heal status in a provided slice
func reduceHealStatus(status []healStatus) healStatus {
	worstStatus := healthy
	for _, st := range status {
		if st > worstStatus {
			worstStatus = st
		}
	}
	return worstStatus
}

// bucketHealStatus - returns the heal status of the provided bucket. Internally,
// this function lists all object heal status of objects inside meta bucket config
// directory and returns the worst heal status that can be found
func (xl xlObjects) bucketHealStatus(bucketName string) (healStatus, error) {
	// A list of all the bucket config files
	configFiles := []string{bucketPolicyConfig, bucketNotificationConfig, bucketListenerConfig}
	// The status of buckets config files
	configsHealStatus := make([]healStatus, len(configFiles))
	// The list of errors found during checking heal status of each config file
	configsErrs := make([]error, len(configFiles))
	// The path of meta bucket that contains all config files
	configBucket := path.Join(minioMetaBucket, bucketConfigPrefix, bucketName)

	// Check of config files heal status in go-routines
	var wg sync.WaitGroup
	// Loop over config files
	for idx, configFile := range configFiles {
		wg.Add(1)
		// Compute heal status of current config file
		go func(bucket, object string, index int) {
			defer wg.Done()
			// Check
			listObjectsHeal, err := xl.listObjectsHeal(bucket, object, "", "", 1)
			// If any error, save and immediately quit
			if err != nil {
				configsErrs[index] = err
				return
			}
			// Check if current bucket contains any not healthy config file and save heal status
			if len(listObjectsHeal.Objects) > 0 {
				configsHealStatus[index] = listObjectsHeal.Objects[0].HealObjectInfo.Status
			}
		}(configBucket, configFile, idx)
	}
	wg.Wait()

	// Return any found error
	for _, err := range configsErrs {
		if err != nil {
			return healthy, err
		}
	}

	// Reduce and return heal status
	return reduceHealStatus(configsHealStatus), nil
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
	for _, currBucket := range buckets {
		// Check the status of bucket metadata
		bucketHealStatus, err := xl.bucketHealStatus(currBucket.Name)
		if err != nil {
			return []BucketInfo{}, err
		}
		// If all metadata are sane, check if the bucket directory is present in all disks
		if bucketHealStatus == healthy && occ[currBucket.Name] != len(xl.storageDisks) {
			// Current bucket is missing in some of the storage disks
			bucketHealStatus = canHeal
		}
		// Add current bucket to the returned result if not healthy
		if bucketHealStatus != healthy {
			listBuckets = append(listBuckets,
				BucketInfo{
					Name:           currBucket.Name,
					Created:        currBucket.Created,
					HealBucketInfo: &HealBucketInfo{Status: bucketHealStatus},
				})
		}

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
			if err = healBucket(storageDisks, bucketName, writeQuorum); err == nil {
				if err = healBucketMetadata(storageDisks, bucketName, readQuorum); err == nil {
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
func healObject(storageDisks []StorageAPI, bucket string, object string, quorum int) error {
	partsMetadata, errs := readAllXLMetadata(storageDisks, bucket, object)
	// readQuorum suffices for xl.json since we use monotonic
	// system time to break the tie when a split-brain situation
	// arises.
	if reducedErr := reduceReadQuorumErrs(errs, nil, quorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket, object)
	}

	if !xlShouldHeal(partsMetadata, errs) {
		// There is nothing to heal.
		return nil
	}

	// List of disks having latest version of the object.
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// List of disks having all parts as per latest xl.json.
	availableDisks, errs, aErr := disksWithAllParts(latestDisks, partsMetadata, errs, bucket, object)
	if aErr != nil {
		return toObjectErr(aErr, bucket, object)
	}

	numAvailableDisks := 0
	for _, disk := range availableDisks {
		if disk != nil {
			numAvailableDisks++
		}
	}

	// If less than read quorum number of disks have all the parts
	// of the data, we can't reconstruct the erasure-coded data.
	if numAvailableDisks < quorum {
		return toObjectErr(errXLReadQuorum, bucket, object)
	}

	// List of disks having outdated version of the object or missing object.
	outDatedDisks := outDatedDisks(storageDisks, availableDisks, errs, partsMetadata,
		bucket, object)

	// Latest xlMetaV1 for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, pErr := pickValidXLMeta(partsMetadata, modTime)
	if pErr != nil {
		return toObjectErr(pErr, bucket, object)
	}

	for index, disk := range outDatedDisks {
		// Before healing outdated disks, we need to remove xl.json
		// and part files from "bucket/object/" so that
		// rename(".minio.sys", "tmp/tmpuuid/", "bucket", "object/") succeeds.
		if disk == nil {
			// Not an outdated disk.
			continue
		}

		// errFileNotFound implies that xl.json is missing. We
		// may have object parts still present in the object
		// directory. This needs to be deleted for object to
		// healed successfully.
		if errs[index] != nil && !isErr(errs[index], errFileNotFound) {
			continue
		}

		// Outdated object with the same name exists that needs to be deleted.
		outDatedMeta := partsMetadata[index]
		// Consult valid metadata picked when there is no
		// metadata available on this disk.
		if isErr(errs[index], errFileNotFound) {
			outDatedMeta = latestMeta
		}

		// Delete all the parts. Ignore if parts are not found.
		for _, part := range outDatedMeta.Parts {
			dErr := disk.DeleteFile(bucket, pathJoin(object, part.Name))
			if dErr != nil && !isErr(dErr, errFileNotFound) {
				return toObjectErr(traceError(dErr), bucket, object)
			}
		}

		// Delete xl.json file. Ignore if xl.json not found.
		dErr := disk.DeleteFile(bucket, pathJoin(object, xlMetaJSONFile))
		if dErr != nil && !isErr(dErr, errFileNotFound) {
			return toObjectErr(traceError(dErr), bucket, object)
		}
	}

	// Reorder so that we have data disks first and parity disks next.
	latestDisks = shuffleDisks(latestDisks, latestMeta.Erasure.Distribution)
	outDatedDisks = shuffleDisks(outDatedDisks, latestMeta.Erasure.Distribution)
	partsMetadata = shufflePartsMetadata(partsMetadata, latestMeta.Erasure.Distribution)

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
		checkSums, hErr := erasureHealFile(latestDisks, outDatedDisks,
			bucket, pathJoin(object, partName),
			minioMetaTmpBucket, pathJoin(tmpID, partName),
			partSize, erasure.BlockSize, erasure.DataBlocks, erasure.ParityBlocks, sumInfo.Algorithm)
		if hErr != nil {
			return toObjectErr(hErr, bucket, object)
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
	aErr = writeUniqueXLMetadata(outDatedDisks, minioMetaTmpBucket, tmpID, partsMetadata, diskCount(outDatedDisks))
	if aErr != nil {
		return toObjectErr(aErr, bucket, object)
	}

	// Rename from tmp location to the actual location.
	for _, disk := range outDatedDisks {
		if disk == nil {
			continue
		}
		// Remove any lingering partial data from current namespace.
		aErr = disk.DeleteFile(bucket, retainSlash(object))
		if aErr != nil && aErr != errFileNotFound {
			return toObjectErr(traceError(aErr), bucket, object)
		}
		// Attempt a rename now from healed data to final location.
		aErr = disk.RenameFile(minioMetaTmpBucket, retainSlash(tmpID), bucket, retainSlash(object))
		if aErr != nil {
			return toObjectErr(traceError(aErr), bucket, object)
		}
	}
	return nil
}

// HealObject heals a given object for all its missing entries.
// FIXME: If an object object was deleted and one disk was down,
// and later the disk comes back up again, heal on the object
// should delete it.
func (xl xlObjects) HealObject(bucket, object string) error {
	// Lock the object before healing.
	objectLock := globalNSMutex.NewNSLock(bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	// Heal the object.
	return healObject(xl.storageDisks, bucket, object, xl.readQuorum)
}
