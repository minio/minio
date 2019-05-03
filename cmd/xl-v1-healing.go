/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
)

func (xl xlObjects) ReloadFormat(ctx context.Context, dryRun bool) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

func (xl xlObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	logger.LogIf(ctx, NotImplemented{})
	return madmin.HealResultItem{}, NotImplemented{}
}

// Heals a bucket if it doesn't exist on one of the disks, additionally
// also heals the missing entries for bucket metadata files
// `policy.json, notification.xml, listeners.json`.
func (xl xlObjects) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (
	result madmin.HealResultItem, err error) {

	storageDisks := xl.getDisks()

	// get write quorum for an object
	writeQuorum := len(storageDisks)/2 + 1

	// Heal bucket.
	return healBucket(ctx, storageDisks, bucket, writeQuorum, dryRun)
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(ctx context.Context, storageDisks []StorageAPI, bucket string, writeQuorum int,
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
			dErrs[index] = errDiskNotFound
			beforeState[index] = madmin.DriveStateOffline
			afterState[index] = madmin.DriveStateOffline
			continue
		}
		wg.Add(1)

		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, serr := disk.StatVol(bucket); serr != nil {
				if serr == errDiskNotFound {
					beforeState[index] = madmin.DriveStateOffline
					afterState[index] = madmin.DriveStateOffline
					dErrs[index] = serr
					return
				}
				if serr != errVolumeNotFound {
					beforeState[index] = madmin.DriveStateCorrupt
					afterState[index] = madmin.DriveStateCorrupt
					dErrs[index] = serr
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
		if storageDisks[i] != nil {
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
	}

	reducedErr := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, writeQuorum)
	if reducedErr == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
	}
	return res, reducedErr
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
			if IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
				continue
			}
			return nil, nil, err
		}
		for _, volInfo := range volsInfo {
			// StorageAPI can send volume names which are
			// incompatible with buckets - these are
			// skipped, like the meta-bucket.
			if isReservedOrInvalidBucket(volInfo.Name, false) {
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

// Only heal on disks where we are sure that healing is needed. We can expand
// this list as and when we figure out more errors can be added to this list safely.
func shouldHealObjectOnDisk(xlErr, dataErr error, meta xlMetaV1, quorumModTime time.Time) bool {
	switch xlErr {
	case errFileNotFound:
		return true
	case errCorruptedFormat:
		return true
	}
	if xlErr == nil {
		// If xl.json was read fine but there is some problem with the part.N files.
		if dataErr == errFileNotFound {
			return true
		}
		if _, ok := dataErr.(hashMismatchError); ok {
			return true
		}
		if quorumModTime != meta.Stat.ModTime {
			return true
		}
	}
	return false
}

// Heals an object by re-writing corrupt/missing erasure blocks.
func (xl xlObjects) healObject(ctx context.Context, bucket string, object string,
	partsMetadata []xlMetaV1, errs []error, latestXLMeta xlMetaV1,
	dryRun bool, remove bool, scanMode madmin.HealScanMode) (result madmin.HealResultItem, err error) {

	dataBlocks := latestXLMeta.Erasure.DataBlocks

	storageDisks := xl.getDisks()

	// List of disks having latest version of the object xl.json
	// (by modtime).
	latestDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// List of disks having all parts as per latest xl.json.
	availableDisks, dataErrs := disksWithAllParts(ctx, latestDisks, partsMetadata, errs, bucket, object, scanMode)

	// Initialize heal result object
	result = madmin.HealResultItem{
		Type:         madmin.HealItemObject,
		Bucket:       bucket,
		Object:       object,
		DiskCount:    len(storageDisks),
		ParityBlocks: latestXLMeta.Erasure.ParityBlocks,
		DataBlocks:   latestXLMeta.Erasure.DataBlocks,

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
		case errs[i] == errDiskNotFound, dataErrs[i] == errDiskNotFound:
			driveState = madmin.DriveStateOffline
		case errs[i] == errFileNotFound, errs[i] == errVolumeNotFound:
			fallthrough
		case dataErrs[i] == errFileNotFound, dataErrs[i] == errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		default:
			// all remaining cases imply corrupt data/metadata
			driveState = madmin.DriveStateCorrupt
		}

		var drive string
		if storageDisks[i] != nil {
			drive = storageDisks[i].String()
		}
		if shouldHealObjectOnDisk(errs[i], dataErrs[i], partsMetadata[i], modTime) {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
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
			continue
		}
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
	if numAvailableDisks < dataBlocks {
		// Check if xl.json, and corresponding parts are also missing.
		if m, ok := isObjectDangling(partsMetadata, errs, dataErrs); ok {
			writeQuorum := m.Erasure.DataBlocks + 1
			if m.Erasure.DataBlocks == 0 {
				writeQuorum = len(storageDisks)/2 + 1
			}
			if !dryRun && remove {
				err = xl.deleteObject(ctx, bucket, object, writeQuorum, false)
			}
			return defaultHealResult(latestXLMeta, storageDisks, errs, bucket, object), err
		}
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
	latestMeta, pErr := pickValidXLMeta(ctx, partsMetadata, modTime, dataBlocks)
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

		// List and delete the object directory,
		files, derr := disk.ListDir(bucket, object, -1, "")
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
	for i := range outDatedDisks {
		if outDatedDisks[i] == nil {
			continue
		}
		partsMetadata[i] = newXLMetaFromXLMeta(latestMeta)
	}

	// We write at temporary location and then rename to final location.
	tmpID := mustGetUUID()

	// Heal each part. erasureHealFile() will write the healed
	// part to .minio/tmp/uuid/ which needs to be renamed later to
	// the final location.
	erasure, err := NewErasure(ctx, latestMeta.Erasure.DataBlocks,
		latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
	if err != nil {
		return result, toObjectErr(err, bucket, object)
	}

	erasureInfo := latestMeta.Erasure
	for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
		partName := latestMeta.Parts[partIndex].Name
		partSize := latestMeta.Parts[partIndex].Size
		partActualSize := latestMeta.Parts[partIndex].ActualSize
		partNumber := latestMeta.Parts[partIndex].Number
		tillOffset := erasure.ShardFileTillOffset(0, partSize, partSize)
		readers := make([]io.ReaderAt, len(latestDisks))
		checksumAlgo := erasureInfo.GetChecksumInfo(partName).Algorithm
		for i, disk := range latestDisks {
			if disk == OfflineDisk {
				continue
			}
			checksumInfo := partsMetadata[i].Erasure.GetChecksumInfo(partName)
			readers[i] = newBitrotReader(disk, bucket, pathJoin(object, partName), tillOffset, checksumAlgo, checksumInfo.Hash, erasure.ShardSize())
		}
		writers := make([]io.Writer, len(outDatedDisks))
		for i, disk := range outDatedDisks {
			if disk == OfflineDisk {
				continue
			}
			writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, pathJoin(tmpID, partName), tillOffset, checksumAlgo, erasure.ShardSize())
		}
		hErr := erasure.Heal(ctx, readers, writers, partSize)
		closeBitrotReaders(readers)
		closeBitrotWriters(writers)
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
			if writers[i] == nil {
				outDatedDisks[i] = nil
				disksToHealCount--
				continue
			}
			partsMetadata[i].AddObjectPart(partNumber, partName, "", partSize, partActualSize)
			partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, checksumAlgo, bitrotWriterSum(writers[i])})
		}

		// If all disks are having errors, we give up.
		if disksToHealCount == 0 {
			return result, fmt.Errorf("all disks without up-to-date data had write errors")
		}
	}

	// Cleanup in case of xl.json writing failure
	writeQuorum := latestMeta.Erasure.DataBlocks + 1
	defer xl.deleteObject(ctx, minioMetaTmpBucket, tmpID, writeQuorum, false)

	// Generate and write `xl.json` generated from other disks.
	outDatedDisks, aErr := writeUniqueXLMetadata(ctx, outDatedDisks, minioMetaTmpBucket, tmpID,
		partsMetadata, diskCount(outDatedDisks))
	if aErr != nil {
		return result, toObjectErr(aErr, bucket, object)
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
			logger.LogIf(ctx, aErr)
			return result, toObjectErr(aErr, bucket, object)
		}

		for i, v := range result.Before.Drives {
			if v.Endpoint == disk.String() {
				result.After.Drives[i].State = madmin.DriveStateOk
			}
		}
	}

	// Set the size of the object in the heal result
	result.ObjectSize = latestMeta.Stat.Size

	return result, nil
}

// healObjectDir - heals object directory specifically, this special call
// is needed since we do not have a special backend format for directories.
func (xl xlObjects) healObjectDir(ctx context.Context, bucket, object string, dryRun bool) (hr madmin.HealResultItem, err error) {
	storageDisks := xl.getDisks()

	// Initialize heal result object
	hr = madmin.HealResultItem{
		Type:         madmin.HealItemObject,
		Bucket:       bucket,
		Object:       object,
		DiskCount:    len(storageDisks),
		ParityBlocks: len(storageDisks) / 2,
		DataBlocks:   len(storageDisks) / 2,
		ObjectSize:   0,
	}

	hr.Before.Drives = make([]madmin.HealDriveInfo, len(storageDisks))
	hr.After.Drives = make([]madmin.HealDriveInfo, len(storageDisks))

	errs := statAllDirs(ctx, storageDisks, bucket, object)
	if isObjectDirDangling(errs) {
		for i, err := range errs {
			if err == nil {
				storageDisks[i].DeleteFile(bucket, object)
			}
		}
	}

	// Prepare object creation in all disks
	for i, err := range errs {
		var drive string
		if storageDisks[i] != nil {
			drive = storageDisks[i].String()
		}
		switch err {
		case errDiskNotFound:
			hr.Before.Drives[i] = madmin.HealDriveInfo{State: madmin.DriveStateOffline}
			hr.After.Drives[i] = madmin.HealDriveInfo{State: madmin.DriveStateOffline}
		case errVolumeNotFound:
			hr.Before.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateMissing}
			hr.After.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateMissing}
		default:
			hr.Before.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateCorrupt}
			hr.After.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateCorrupt}
		}
	}
	if dryRun {
		return hr, nil
	}
	for i, err := range errs {
		switch err {
		case errVolumeNotFound:
			merr := storageDisks[i].MakeVol(pathJoin(bucket, object))
			switch merr {
			case nil, errVolumeExists:
				hr.After.Drives[i].State = madmin.DriveStateOk
			case errDiskNotFound:
				hr.After.Drives[i].State = madmin.DriveStateOffline
			default:
				logger.LogIf(ctx, merr)
				hr.After.Drives[i].State = madmin.DriveStateCorrupt
			}
		}
	}
	return hr, nil
}

// Populates default heal result item entries with possible values when we are returning prematurely.
// This is to ensure that in any circumstance we are not returning empty arrays with wrong values.
func defaultHealResult(latestXLMeta xlMetaV1, storageDisks []StorageAPI, errs []error, bucket, object string) madmin.HealResultItem {
	// Initialize heal result object
	result := madmin.HealResultItem{
		Type:      madmin.HealItemObject,
		Bucket:    bucket,
		Object:    object,
		DiskCount: len(storageDisks),

		// Initialize object size to -1, so we can detect if we are
		// unable to reliably find the object size.
		ObjectSize: -1,
	}
	if latestXLMeta.IsValid() {
		result.ObjectSize = latestXLMeta.Stat.Size
	}

	for index, disk := range storageDisks {
		if disk == nil {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:  "",
				State: madmin.DriveStateOffline,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:  "",
				State: madmin.DriveStateOffline,
			})
			continue
		}
		drive := disk.String()
		driveState := madmin.DriveStateCorrupt
		switch errs[index] {
		case errFileNotFound, errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		}
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

	if !latestXLMeta.IsValid() {
		// Default to most common configuration for erasure blocks.
		result.ParityBlocks = len(storageDisks) / 2
		result.DataBlocks = len(storageDisks) / 2
	} else {
		result.ParityBlocks = latestXLMeta.Erasure.ParityBlocks
		result.DataBlocks = latestXLMeta.Erasure.DataBlocks
	}

	return result
}

// Stat all directories.
func statAllDirs(ctx context.Context, storageDisks []StorageAPI, bucket, prefix string) []error {
	var errs = make([]error, len(storageDisks))
	var wg sync.WaitGroup
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			entries, err := disk.ListDir(bucket, prefix, 1, "")
			if err != nil {
				errs[index] = err
				return
			}
			if len(entries) > 0 {
				errs[index] = errVolumeNotEmpty
				return
			}
		}(index, disk)
	}

	wg.Wait()
	return errs
}

// ObjectDir is considered dangling/corrupted if any only
// if total disks - a combination of corrupted and missing
// files is lesser than N/2+1 number of disks.
func isObjectDirDangling(errs []error) (ok bool) {
	var notFoundDir int
	for _, readErr := range errs {
		if readErr == errFileNotFound {
			notFoundDir++
		}
	}
	return notFoundDir > len(errs)/2
}

// Object is considered dangling/corrupted if any only
// if total disks - a combination of corrupted and missing
// files is lesser than number of data blocks.
func isObjectDangling(metaArr []xlMetaV1, errs []error, dataErrs []error) (validMeta xlMetaV1, ok bool) {
	// We can consider an object data not reliable
	// when xl.json is not found in read quorum disks.
	// or when xl.json is not readable in read quorum disks.
	var notFoundXLJSON, corruptedXLJSON int
	for _, readErr := range errs {
		if readErr == errFileNotFound {
			notFoundXLJSON++
		} else if readErr == errCorruptedFormat {
			corruptedXLJSON++
		}
	}
	var notFoundParts int
	for i := range dataErrs {
		// Only count part errors, if the error is not
		// same as xl.json error. This is to avoid
		// double counting when both parts and xl.json
		// are not available.
		if errs[i] != dataErrs[i] {
			if dataErrs[i] == errFileNotFound {
				notFoundParts++
			}
		}
	}

	for _, m := range metaArr {
		if !m.IsValid() {
			continue
		}
		validMeta = m
		break
	}

	// We couldn't find any valid meta we are indeed corrupted, return true right away.
	if validMeta.Erasure.DataBlocks == 0 {
		return validMeta, true
	}

	// We have valid meta, now verify if we have enough files with parity blocks.
	return validMeta, corruptedXLJSON+notFoundXLJSON+notFoundParts > validMeta.Erasure.ParityBlocks
}

// HealObject - heal the given object, automatically deletes the object if stale/corrupted if `remove` is true.
func (xl xlObjects) HealObject(ctx context.Context, bucket, object string, dryRun bool, remove bool, scanMode madmin.HealScanMode) (hr madmin.HealResultItem, err error) {
	// Create context that also contains information about the object and bucket.
	// The top level handler might not have this information.
	reqInfo := logger.GetReqInfo(ctx)
	var newReqInfo *logger.ReqInfo
	if reqInfo != nil {
		newReqInfo = logger.NewReqInfo(reqInfo.RemoteHost, reqInfo.UserAgent, reqInfo.DeploymentID, reqInfo.RequestID, reqInfo.API, bucket, object)
	} else {
		newReqInfo = logger.NewReqInfo("", "", globalDeploymentID, "", "Heal", bucket, object)
	}
	healCtx := logger.SetReqInfo(context.Background(), newReqInfo)

	// Healing directories handle it separately.
	if hasSuffix(object, slashSeparator) {
		return xl.healObjectDir(healCtx, bucket, object, dryRun)
	}

	storageDisks := xl.getDisks()

	// Read metadata files from all the disks
	partsMetadata, errs := readAllXLMetadata(healCtx, storageDisks, bucket, object)

	// Check if the object is dangling, if yes and user requested
	// remove we simply delete it from namespace.
	if m, ok := isObjectDangling(partsMetadata, errs, []error{}); ok {
		writeQuorum := m.Erasure.DataBlocks + 1
		if m.Erasure.DataBlocks == 0 {
			writeQuorum = len(xl.getDisks())/2 + 1
		}
		if !dryRun && remove {
			err = xl.deleteObject(healCtx, bucket, object, writeQuorum, false)
		}
		return defaultHealResult(xlMetaV1{}, storageDisks, errs, bucket, object), err
	}

	latestXLMeta, err := getLatestXLMeta(healCtx, partsMetadata, errs)
	if err != nil {
		return defaultHealResult(xlMetaV1{}, storageDisks, errs, bucket, object), toObjectErr(err, bucket, object)
	}

	// Lock the object before healing.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if lerr := objectLock.GetRLock(globalHealingTimeout); lerr != nil {
		return defaultHealResult(latestXLMeta, storageDisks, errs, bucket, object), lerr
	}
	defer objectLock.RUnlock()

	errCount := 0
	for _, err := range errs {
		if err != nil {
			errCount++
		}
	}

	if errCount == len(errs) {
		// Only if we get errors from all the disks we return error. Else we need to
		// continue to return filled madmin.HealResultItem struct which includes info
		// on what disks the file is available etc.
		if reducedErr := reduceReadQuorumErrs(ctx, errs, nil, latestXLMeta.Erasure.DataBlocks); reducedErr != nil {
			if m, ok := isObjectDangling(partsMetadata, errs, []error{}); ok {
				writeQuorum := m.Erasure.DataBlocks + 1
				if m.Erasure.DataBlocks == 0 {
					writeQuorum = len(storageDisks)/2 + 1
				}
				if !dryRun && remove {
					err = xl.deleteObject(ctx, bucket, object, writeQuorum, false)
				}
			}
			return defaultHealResult(latestXLMeta, storageDisks, errs, bucket, object), toObjectErr(reducedErr, bucket, object)
		}
	}

	// Heal the object.
	return xl.healObject(healCtx, bucket, object, partsMetadata, errs, latestXLMeta, dryRun, remove, scanMode)
}
