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

import "sync"

// Heals a bucket if it doesn't exist on one of the disks.
func (xl xlObjects) HealBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify if bucket exists.
	if !xl.isBucketExist(bucket) {
		return traceError(BucketNotFound{Bucket: bucket})
	}

	// Heal bucket - create buckets on disks where it does not exist.

	// get a random ID for lock instrumentation.
	opsID := getOpsID()

	nsMutex.Lock(bucket, "", opsID)
	defer nsMutex.Unlock(bucket, "", opsID)

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
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
	if !isDiskQuorum(dErrs, xl.writeQuorum) {
		// Purge successfully created buckets if we don't have writeQuorum.
		xl.undoMakeBucket(bucket)
		return toObjectErr(traceError(errXLWriteQuorum), bucket)
	}

	// Verify we have any other errors which should be returned as failure.
	if reducedErr := reduceErrs(dErrs, []error{
		errDiskNotFound,
		errFaultyDisk,
		errDiskAccessDenied,
	}); reducedErr != nil {
		return toObjectErr(reducedErr, bucket)
	}
	return nil
}

// HealObject heals a given object for all its missing entries.
// FIXME: If an object object was deleted and one disk was down, and later the disk comes back
// up again, heal on the object should delete it.
func (xl xlObjects) HealObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return traceError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// get a random ID for lock instrumentation.
	opsID := getOpsID()

	// Lock the object before healing.
	nsMutex.RLock(bucket, object, opsID)
	defer nsMutex.RUnlock(bucket, object, opsID)

	partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, object)
	if err := reduceErrs(errs, nil); err != nil {
		return toObjectErr(err, bucket, object)
	}

	if !xlShouldHeal(partsMetadata, errs) {
		// There is nothing to heal.
		return nil
	}

	// List of disks having latest version of the object.
	latestDisks, modTime := listOnlineDisks(xl.storageDisks, partsMetadata, errs)
	// List of disks having outdated version of the object or missing object.
	outDatedDisks := outDatedDisks(xl.storageDisks, partsMetadata, errs)
	// Latest xlMetaV1 for reference.
	latestMeta := pickValidXLMeta(partsMetadata, modTime)

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
	tmpID := getUUID()

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
			minioMetaBucket, pathJoin(tmpMetaPrefix, tmpID, partName),
			partSize, erasure.BlockSize, erasure.DataBlocks, erasure.ParityBlocks, sumInfo.Algorithm)
		if err != nil {
			return err
		}
		for index, sum := range checkSums {
			if outDatedDisks[index] == nil {
				continue
			}
			checkSumInfos[index] = append(checkSumInfos[index], checkSumInfo{partName, sumInfo.Algorithm, sum})
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
	err := writeUniqueXLMetadata(outDatedDisks, minioMetaBucket, pathJoin(tmpMetaPrefix, tmpID), partsMetadata, diskCount(outDatedDisks))
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
		err = disk.RenameFile(minioMetaBucket, retainSlash(pathJoin(tmpMetaPrefix, tmpID)), bucket, retainSlash(object))
		if err != nil {
			return traceError(err)
		}
	}
	return nil
}
