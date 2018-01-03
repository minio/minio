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

	numDisks := len(storageDisks)
	_, unformattedDiskCount, diskNotFoundCount,
		corruptedFormatCount, otherErrCount := formatErrsSummary(sErrs)

	switch {
	case unformattedDiskCount == numDisks:
		// all unformatted.
		if err = initFormatXL(storageDisks); err != nil {
			return err
		}

	case diskNotFoundCount > 0:
		return fmt.Errorf("cannot proceed with heal as %s",
			errSomeDiskOffline)

	case otherErrCount > 0:
		return fmt.Errorf("cannot proceed with heal as some disks had unhandled errors")

	case corruptedFormatCount > 0:
		if err = healFormatXLCorruptedDisks(storageDisks, formatConfigs); err != nil {
			return fmt.Errorf("Unable to repair corrupted format, %s", err)
		}

	case unformattedDiskCount > 0:
		// All drives online but some report missing format.json.
		if err = healFormatXLFreshDisks(storageDisks, formatConfigs); err != nil {
			// There was an unexpected unrecoverable error
			// during healing.
			return fmt.Errorf("Unable to heal backend %s", err)
		}

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

	// get write quorum for an object
	writeQuorum := len(xl.storageDisks)/2 + 1

	// Heal bucket.
	if err := healBucket(xl.storageDisks, bucket, writeQuorum); err != nil {
		return err
	}

	// Proceed to heal bucket metadata.
	return healBucketMetadata(xl, bucket)
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(storageDisks []StorageAPI, bucket string, writeQuorum int) error {
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalHealingTimeout); err != nil {
		return err
	}
	defer bucketLock.Unlock()

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range storageDisks {
		if disk == nil {
			dErrs[index] = errors.Trace(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, err := disk.StatVol(bucket); err != nil {
				if err != errVolumeNotFound {
					dErrs[index] = errors.Trace(err)
					return
				}
				if err = disk.MakeVol(bucket); err != nil {
					dErrs[index] = errors.Trace(err)
				}
			}
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, writeQuorum)
	if errors.Cause(reducedErr) == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
	}
	return reducedErr
}

// Heals all the metadata associated for a given bucket, this function
// heals `policy.json`, `notification.xml` and `listeners.json`.
func healBucketMetadata(xlObj xlObjects, bucket string) error {
	healBucketMetaFn := func(metaPath string) error {
		if _, _, err := xlObj.HealObject(minioMetaBucket, metaPath); err != nil && !isErrObjectNotFound(err) {
			return err
		}
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
		if errors.IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
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
			// Heal bucket and then proceed to heal bucket metadata if any.
			if err = healBucket(xlObj.storageDisks, bucketName, writeQuorum); err == nil {
				if err = healBucketMetadata(xlObj, bucketName); err == nil {
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
func healObject(storageDisks []StorageAPI, bucket, object string, quorum int) (int, int, error) {

	partsMetadata, errs := readAllXLMetadata(storageDisks, bucket, object)
	// readQuorum suffices for xl.json since we use monotonic
	// system time to break the tie when a split-brain situation
	// arises.
	if rErr := reduceReadQuorumErrs(errs, nil, quorum); rErr != nil {
		return 0, 0, toObjectErr(rErr, bucket, object)
	}

	// List of disks having latest version of the object.
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// List of disks having all parts as per latest xl.json - this
	// does a full pass over the data and verifies all part files
	// on disk
	availableDisks, errs, aErr := disksWithAllParts(latestDisks, partsMetadata, errs, bucket,
		object)
	if aErr != nil {
		return 0, 0, toObjectErr(aErr, bucket, object)
	}

	// Number of disks which don't serve data.
	numOfflineDisks := 0
	for index, disk := range storageDisks {
		if disk == nil || errs[index] == errDiskNotFound {
			numOfflineDisks++
		}
	}

	// Number of disks which have all parts of the given object.
	numAvailableDisks := 0
	for _, disk := range availableDisks {
		if disk != nil {
			numAvailableDisks++
		}
	}

	if numAvailableDisks == len(storageDisks) {
		// nothing to heal in this case
		return 0, 0, nil
	}

	// If less than read quorum number of disks have all the parts
	// of the data, we can't reconstruct the erasure-coded data.
	if numAvailableDisks < quorum {
		return 0, 0, toObjectErr(errXLReadQuorum, bucket, object)
	}

	// List of disks having outdated version of the object or missing object.
	outDatedDisks := outDatedDisks(storageDisks, availableDisks, errs, partsMetadata, bucket,
		object)

	// Number of disks that had outdated content of the given
	// object and are online to be healed.
	numHealedDisks := 0
	for _, disk := range outDatedDisks {
		if disk != nil {
			numHealedDisks++
		}
	}

	// Latest xlMetaV1 for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, pErr := pickValidXLMeta(partsMetadata, modTime)
	if pErr != nil {
		return 0, 0, toObjectErr(pErr, bucket, object)
	}

	for index, disk := range outDatedDisks {
		// Before healing outdated disks, we need to remove
		// xl.json and part files from "bucket/object/" so
		// that rename(minioMetaBucket, "tmp/tmpuuid/",
		// "bucket", "object/") succeeds.
		if disk == nil {
			// Not an outdated disk.
			continue
		}

		// errFileNotFound implies that xl.json is missing. We
		// may have object parts still present in the object
		// directory. This needs to be deleted for object to
		// healed successfully.
		if errs[index] != nil && !errors.IsErr(errs[index], errFileNotFound) {
			continue
		}

		// List and delete the object directory, ignoring
		// errors.
		files, err := disk.ListDir(bucket, object)
		if err == nil {
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
	storage, err := NewErasureStorage(latestDisks,
		latestMeta.Erasure.DataBlocks, latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
	if err != nil {
		return 0, 0, toObjectErr(err, bucket, object)
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
			return 0, 0, toObjectErr(hErr, bucket, object)
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
				numHealedDisks--
				continue
			}
			// append part checksums
			checksumInfos[i] = append(checksumInfos[i],
				ChecksumInfo{partName, file.Algorithm, file.Checksums[i]})
		}

		// If all disks are having errors, we give up.
		if numHealedDisks == 0 {
			return 0, 0, fmt.Errorf("all disks without up-to-date data had write errors")
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
		return 0, 0, toObjectErr(aErr, bucket, object)
	}

	// Rename from tmp location to the actual location.
	for _, disk := range outDatedDisks {
		if disk == nil {
			continue
		}

		// Attempt a rename now from healed data to final location.
		aErr = disk.RenameFile(minioMetaTmpBucket, retainSlash(tmpID), bucket,
			retainSlash(object))
		if aErr != nil {
			return 0, 0, toObjectErr(errors.Trace(aErr), bucket, object)
		}
	}
	return numOfflineDisks, numHealedDisks, nil
}

// HealObject heals a given object for all its missing entries.
// FIXME: If an object object was deleted and one disk was down,
// and later the disk comes back up again, heal on the object
// should delete it.
func (xl xlObjects) HealObject(bucket, object string) (int, int, error) {
	// Read metadata files from all the disks
	partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, object)

	// get read quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(xl, partsMetadata, errs)
	if err != nil {
		return 0, 0, err
	}

	// Lock the object before healing.
	objectLock := globalNSMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalHealingTimeout); err != nil {
		return 0, 0, err
	}
	defer objectLock.RUnlock()

	// Heal the object.
	return healObject(xl.storageDisks, bucket, object, readQuorum)
}
