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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

// Heals a bucket if it doesn't exist on one of the disks, additionally
// also heals the missing entries for bucket metadata files
// `policy.json, notification.xml, listeners.json`.
func (er erasureObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (
	result madmin.HealResultItem, err error) {
	if !opts.DryRun {
		defer NSUpdated(bucket, slashSeparator)
	}

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// get write quorum for an object
	writeQuorum := len(storageDisks) - er.defaultParityCount
	if writeQuorum == er.defaultParityCount {
		writeQuorum++
	}

	// Heal bucket.
	return healBucket(ctx, storageDisks, storageEndpoints, bucket, writeQuorum, opts)
}

// Heal bucket - create buckets on disks where it does not exist.
func healBucket(ctx context.Context, storageDisks []StorageAPI, storageEndpoints []string, bucket string, writeQuorum int,
	opts madmin.HealOpts) (res madmin.HealResultItem, err error) {

	// Initialize sync waitgroup.
	g := errgroup.WithNErrs(len(storageDisks))

	// Disk states slices
	beforeState := make([]string, len(storageDisks))
	afterState := make([]string, len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				beforeState[index] = madmin.DriveStateOffline
				afterState[index] = madmin.DriveStateOffline
				return errDiskNotFound
			}
			if _, serr := storageDisks[index].StatVol(ctx, bucket); serr != nil {
				if serr == errDiskNotFound {
					beforeState[index] = madmin.DriveStateOffline
					afterState[index] = madmin.DriveStateOffline
					return serr
				}
				if serr != errVolumeNotFound {
					beforeState[index] = madmin.DriveStateCorrupt
					afterState[index] = madmin.DriveStateCorrupt
					return serr
				}

				beforeState[index] = madmin.DriveStateMissing
				afterState[index] = madmin.DriveStateMissing

				// mutate only if not a dry-run
				if opts.DryRun {
					return nil
				}

				return serr
			}
			beforeState[index] = madmin.DriveStateOk
			afterState[index] = madmin.DriveStateOk
			return nil
		}, index)
	}

	errs := g.Wait()

	// Initialize heal result info
	res = madmin.HealResultItem{
		Type:         madmin.HealItemBucket,
		Bucket:       bucket,
		DiskCount:    len(storageDisks),
		ParityBlocks: len(storageDisks) / 2,
		DataBlocks:   len(storageDisks) / 2,
	}

	for i := range beforeState {
		res.Before.Drives = append(res.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i],
			State:    beforeState[i],
		})
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, writeQuorum-1)
	if errors.Is(reducedErr, errVolumeNotFound) && !opts.Recreate {
		for i := range beforeState {
			res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i],
				State:    madmin.DriveStateOk,
			})
		}
		return res, nil
	}

	// Initialize sync waitgroup.
	g = errgroup.WithNErrs(len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if beforeState[index] == madmin.DriveStateMissing {
				makeErr := storageDisks[index].MakeVol(ctx, bucket)
				if makeErr == nil {
					afterState[index] = madmin.DriveStateOk
				}
				return makeErr
			}
			return errs[index]
		}, index)
	}

	errs = g.Wait()

	reducedErr = reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, writeQuorum)
	if reducedErr != nil {
		return res, reducedErr
	}

	for i := range afterState {
		res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i],
			State:    afterState[i],
		})
	}
	return res, nil
}

// listAllBuckets lists all buckets from all disks. It also
// returns the occurrence of each buckets in all disks
func listAllBuckets(ctx context.Context, storageDisks []StorageAPI, healBuckets map[string]VolInfo) error {
	g := errgroup.WithNErrs(len(storageDisks))
	var mu sync.Mutex
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				// we ignore disk not found errors
				return nil
			}
			volsInfo, err := storageDisks[index].ListVols(ctx)
			if err != nil {
				return err
			}
			for _, volInfo := range volsInfo {
				// StorageAPI can send volume names which are
				// incompatible with buckets - these are
				// skipped, like the meta-bucket.
				if isReservedOrInvalidBucket(volInfo.Name, false) {
					continue
				}
				mu.Lock()
				if _, ok := healBuckets[volInfo.Name]; !ok {
					healBuckets[volInfo.Name] = volInfo
				}
				mu.Unlock()
			}
			return nil
		}, index)
	}
	return reduceReadQuorumErrs(ctx, g.Wait(), bucketMetadataOpIgnoredErrs, len(storageDisks)/2)
}

// Only heal on disks where we are sure that healing is needed. We can expand
// this list as and when we figure out more errors can be added to this list safely.
func shouldHealObjectOnDisk(erErr, dataErr error, meta FileInfo, quorumModTime time.Time) bool {
	switch {
	case errors.Is(erErr, errFileNotFound) || errors.Is(erErr, errFileVersionNotFound):
		return true
	case errors.Is(erErr, errCorruptedFormat):
		return true
	}
	if erErr == nil {
		if !meta.IsRemote() {
			// If xl.meta was read fine but there may be problem with the part.N files.
			if IsErr(dataErr, []error{
				errFileNotFound,
				errFileVersionNotFound,
				errFileCorrupt,
			}...) {
				return true
			}
		}
		if !quorumModTime.Equal(meta.ModTime) {
			return true
		}
		if meta.XLV1 {
			return true
		}
	}
	return false
}

// Heals an object by re-writing corrupt/missing erasure blocks.
func (er erasureObjects) healObject(ctx context.Context, bucket string, object string, versionID string, opts madmin.HealOpts) (result madmin.HealResultItem, err error) {
	if !opts.DryRun {
		defer NSUpdated(bucket, object)
	}

	dryRun := opts.DryRun
	scanMode := opts.ScanMode

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// Initialize heal result object
	result = madmin.HealResultItem{
		Type:         madmin.HealItemObject,
		Bucket:       bucket,
		Object:       object,
		DiskCount:    len(storageDisks),
		ParityBlocks: er.defaultParityCount,
		DataBlocks:   len(storageDisks) - er.defaultParityCount,
	}

	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return result, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx.Cancel)
	}

	// Re-read when we have lock...
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, bucket, object, versionID, true)

	_, err = getLatestFileInfo(ctx, partsMetadata, errs)
	if err != nil {
		return er.purgeObjectDangling(ctx, bucket, object, versionID, partsMetadata, errs, []error{}, opts)
	}
	// List of disks having latest version of the object er.meta
	// (by modtime).
	_, modTime, dataDir := listOnlineDisks(storageDisks, partsMetadata, errs)

	// make sure all parts metadata dataDir is same as returned by listOnlineDisks()
	// the reason is its possible that some of the disks might have stale data, for those
	// we simply override them with maximally occurring 'dataDir' - this ensures that
	// disksWithAllParts() verifies same dataDir across all drives.
	for i := range partsMetadata {
		partsMetadata[i].DataDir = dataDir
	}

	// List of disks having all parts as per latest metadata.
	// NOTE: do not pass in latestDisks to diskWithAllParts since
	// the diskWithAllParts needs to reach the drive to ensure
	// validity of the metadata content, we should make sure that
	// we pass in disks as is for it to be verified. Once verified
	// the disksWithAllParts() returns the actual disks that can be
	// used here for reconstruction. This is done to ensure that
	// we do not skip drives that have inconsistent metadata to be
	// skipped from purging when they are stale.
	availableDisks, dataErrs := disksWithAllParts(ctx, storageDisks, partsMetadata, errs, bucket, object, scanMode)

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
			result.ObjectSize = partsMetadata[i].Size
			if partsMetadata[i].Erasure.ParityBlocks > 0 && partsMetadata[i].Erasure.DataBlocks > 0 {
				result.ParityBlocks = partsMetadata[i].Erasure.ParityBlocks
				result.DataBlocks = partsMetadata[i].Erasure.DataBlocks
			}
		case errs[i] == errDiskNotFound, dataErrs[i] == errDiskNotFound:
			driveState = madmin.DriveStateOffline
		case errs[i] == errFileNotFound, errs[i] == errFileVersionNotFound, errs[i] == errVolumeNotFound:
			fallthrough
		case dataErrs[i] == errFileNotFound, dataErrs[i] == errFileVersionNotFound, dataErrs[i] == errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		default:
			// all remaining cases imply corrupt data/metadata
			driveState = madmin.DriveStateCorrupt
		}

		if shouldHealObjectOnDisk(errs[i], dataErrs[i], partsMetadata[i], modTime) {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i],
				State:    driveState,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i],
				State:    driveState,
			})
			continue
		}
		result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i],
			State:    driveState,
		})
		result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i],
			State:    driveState,
		})
	}

	if isAllNotFound(errs) {
		err = toObjectErr(errFileNotFound, bucket, object)
		if versionID != "" {
			err = toObjectErr(errFileVersionNotFound, bucket, object, versionID)
		}
		// File is fully gone, fileInfo is empty.
		return defaultHealResult(FileInfo{}, storageDisks, storageEndpoints, errs, bucket, object, versionID, er.defaultParityCount), err
	}

	// If less than read quorum number of disks have all the parts
	// of the data, we can't reconstruct the erasure-coded data.
	if numAvailableDisks < result.DataBlocks {
		return er.purgeObjectDangling(ctx, bucket, object, versionID, partsMetadata, errs, dataErrs, opts)
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

	// Latest FileInfo for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, err := pickValidFileInfo(ctx, partsMetadata, modTime, dataDir, result.DataBlocks)
	if err != nil {
		return result, toObjectErr(err, bucket, object, versionID)
	}

	cleanFileInfo := func(fi FileInfo) FileInfo {
		// Returns a copy of the 'fi' with checksums and parts nil'ed.
		nfi := fi
		nfi.Erasure.Index = 0
		nfi.Erasure.Checksums = nil
		if fi.IsRemote() {
			nfi.Parts = nil
		}
		return nfi
	}

	// We write at temporary location and then rename to final location.
	tmpID := mustGetUUID()
	migrateDataDir := mustGetUUID()

	copyPartsMetadata := make([]FileInfo, len(partsMetadata))
	for i := range outDatedDisks {
		if outDatedDisks[i] == nil {
			continue
		}
		copyPartsMetadata[i] = partsMetadata[i]
		partsMetadata[i] = cleanFileInfo(latestMeta)
	}

	// source data dir shall be empty in case of XLV1
	// differentiate it with dstDataDir for readability
	// srcDataDir is the one used with newBitrotReader()
	// to read existing content.
	srcDataDir := latestMeta.DataDir
	dstDataDir := latestMeta.DataDir
	if latestMeta.XLV1 {
		dstDataDir = migrateDataDir
	}

	var inlineBuffers []*bytes.Buffer
	if len(latestMeta.Parts) <= 1 && latestMeta.Size < smallFileThreshold {
		inlineBuffers = make([]*bytes.Buffer, len(outDatedDisks))
	}

	if !latestMeta.Deleted && !latestMeta.IsRemote() {
		result.DataBlocks = latestMeta.Erasure.DataBlocks
		result.ParityBlocks = latestMeta.Erasure.ParityBlocks

		// Reorder so that we have data disks first and parity disks next.
		latestDisks := shuffleDisks(availableDisks, latestMeta.Erasure.Distribution)
		outDatedDisks = shuffleDisks(outDatedDisks, latestMeta.Erasure.Distribution)
		partsMetadata = shufflePartsMetadata(partsMetadata, latestMeta.Erasure.Distribution)
		copyPartsMetadata = shufflePartsMetadata(copyPartsMetadata, latestMeta.Erasure.Distribution)

		// Heal each part. erasureHealFile() will write the healed
		// part to .minio/tmp/uuid/ which needs to be renamed later to
		// the final location.
		erasure, err := NewErasure(ctx, latestMeta.Erasure.DataBlocks,
			latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
		if err != nil {
			return result, toObjectErr(err, bucket, object)
		}

		erasureInfo := latestMeta.Erasure
		bp := er.bp
		if erasureInfo.BlockSize == blockSizeV1 {
			bp = er.bpOld
		}
		for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
			partSize := latestMeta.Parts[partIndex].Size
			partActualSize := latestMeta.Parts[partIndex].ActualSize
			partNumber := latestMeta.Parts[partIndex].Number
			tillOffset := erasure.ShardFileOffset(0, partSize, partSize)
			readers := make([]io.ReaderAt, len(latestDisks))
			checksumAlgo := erasureInfo.GetChecksumInfo(partNumber).Algorithm
			for i, disk := range latestDisks {
				if disk == OfflineDisk {
					continue
				}
				checksumInfo := copyPartsMetadata[i].Erasure.GetChecksumInfo(partNumber)
				partPath := pathJoin(object, srcDataDir, fmt.Sprintf("part.%d", partNumber))
				readers[i] = newBitrotReader(disk, partsMetadata[i].Data, bucket, partPath, tillOffset, checksumAlgo,
					checksumInfo.Hash, erasure.ShardSize())
			}
			writers := make([]io.Writer, len(outDatedDisks))
			for i, disk := range outDatedDisks {
				if disk == OfflineDisk {
					continue
				}
				partPath := pathJoin(tmpID, dstDataDir, fmt.Sprintf("part.%d", partNumber))
				if len(inlineBuffers) > 0 {
					inlineBuffers[i] = bytes.NewBuffer(make([]byte, 0, erasure.ShardFileSize(latestMeta.Size)))
					writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
				} else {
					writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, partPath,
						tillOffset, DefaultBitrotAlgorithm, erasure.ShardSize())
				}
			}
			err = erasure.Heal(ctx, readers, writers, partSize, bp)
			closeBitrotReaders(readers)
			closeBitrotWriters(writers)
			if err != nil {
				return result, toObjectErr(err, bucket, object)
			}

			// outDatedDisks that had write errors should not be
			// written to for remaining parts, so we nil it out.
			for i, disk := range outDatedDisks {
				if disk == OfflineDisk {
					continue
				}

				// A non-nil stale disk which did not receive
				// a healed part checksum had a write error.
				if writers[i] == nil {
					outDatedDisks[i] = nil
					disksToHealCount--
					continue
				}

				partsMetadata[i].DataDir = dstDataDir
				partsMetadata[i].AddObjectPart(partNumber, "", partSize, partActualSize)
				partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
					PartNumber: partNumber,
					Algorithm:  checksumAlgo,
					Hash:       bitrotWriterSum(writers[i]),
				})
				if len(inlineBuffers) > 0 && inlineBuffers[i] != nil {
					partsMetadata[i].Data = inlineBuffers[i].Bytes()
				} else {
					partsMetadata[i].Data = nil
				}
			}

			// If all disks are having errors, we give up.
			if disksToHealCount == 0 {
				return result, fmt.Errorf("all disks had write errors, unable to heal")
			}

		}

	}

	defer er.deleteObject(context.Background(), minioMetaTmpBucket, tmpID, len(storageDisks)/2+1)

	// Rename from tmp location to the actual location.
	for i, disk := range outDatedDisks {
		if disk == OfflineDisk {
			continue
		}

		// record the index of the updated disks
		partsMetadata[i].Erasure.Index = i + 1

		// dataDir should be empty when
		// - transitionStatus is complete and not in restored state
		if partsMetadata[i].IsRemote() {
			partsMetadata[i].DataDir = ""
		}

		// Attempt a rename now from healed data to final location.
		if err = disk.RenameData(ctx, minioMetaTmpBucket, tmpID, partsMetadata[i], bucket, object); err != nil {
			logger.LogIf(ctx, err)
			return result, toObjectErr(err, bucket, object)
		}

		for i, v := range result.Before.Drives {
			if v.Endpoint == disk.String() {
				result.After.Drives[i].State = madmin.DriveStateOk
			}
		}
	}

	// Set the size of the object in the heal result
	result.ObjectSize = latestMeta.Size

	return result, nil
}

// healObjectDir - heals object directory specifically, this special call
// is needed since we do not have a special backend format for directories.
func (er erasureObjects) healObjectDir(ctx context.Context, bucket, object string, dryRun bool, remove bool) (hr madmin.HealResultItem, err error) {
	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// Initialize heal result object
	hr = madmin.HealResultItem{
		Type:         madmin.HealItemObject,
		Bucket:       bucket,
		Object:       object,
		DiskCount:    len(storageDisks),
		ParityBlocks: er.defaultParityCount,
		DataBlocks:   len(storageDisks) - er.defaultParityCount,
		ObjectSize:   0,
	}

	hr.Before.Drives = make([]madmin.HealDriveInfo, len(storageDisks))
	hr.After.Drives = make([]madmin.HealDriveInfo, len(storageDisks))

	errs := statAllDirs(ctx, storageDisks, bucket, object)
	danglingObject := isObjectDirDangling(errs)
	if danglingObject {
		if !dryRun && remove {
			var wg sync.WaitGroup
			// Remove versions in bulk for each disk
			for index, disk := range storageDisks {
				if disk == nil {
					continue
				}
				wg.Add(1)
				go func(index int, disk StorageAPI) {
					defer wg.Done()
					_ = disk.Delete(ctx, bucket, object, false)
				}(index, disk)
			}
			wg.Wait()
			NSUpdated(bucket, object)
		}
	}

	// Prepare object creation in all disks
	for i, err := range errs {
		drive := storageEndpoints[i]
		switch err {
		case nil:
			hr.Before.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateOk}
			hr.After.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateOk}
		case errDiskNotFound:
			hr.Before.Drives[i] = madmin.HealDriveInfo{State: madmin.DriveStateOffline}
			hr.After.Drives[i] = madmin.HealDriveInfo{State: madmin.DriveStateOffline}
		case errVolumeNotFound, errFileNotFound:
			// Bucket or prefix/directory not found
			hr.Before.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateMissing}
			hr.After.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateMissing}
		default:
			hr.Before.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateCorrupt}
			hr.After.Drives[i] = madmin.HealDriveInfo{Endpoint: drive, State: madmin.DriveStateCorrupt}
		}
	}
	if dryRun || danglingObject || isAllNotFound(errs) {
		// Nothing to do, file is already gone.
		return hr, toObjectErr(errFileNotFound, bucket, object)
	}
	for i, err := range errs {
		if err == errVolumeNotFound || err == errFileNotFound {
			// Bucket or prefix/directory not found
			merr := storageDisks[i].MakeVol(ctx, pathJoin(bucket, object))
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
func defaultHealResult(lfi FileInfo, storageDisks []StorageAPI, storageEndpoints []string, errs []error, bucket, object, versionID string, defaultParityCount int) madmin.HealResultItem {
	// Initialize heal result object
	result := madmin.HealResultItem{
		Type:      madmin.HealItemObject,
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		DiskCount: len(storageDisks),
	}

	if lfi.IsValid() {
		result.ObjectSize = lfi.Size
		result.ParityBlocks = lfi.Erasure.ParityBlocks
	} else {
		// Default to most common configuration for erasure blocks.
		result.ParityBlocks = defaultParityCount
	}
	result.DataBlocks = len(storageDisks) - result.ParityBlocks

	if errs == nil {
		// No disks related errors are provided, quit
		return result
	}

	for index, disk := range storageDisks {
		if disk == nil {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[index],
				State:    madmin.DriveStateOffline,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[index],
				State:    madmin.DriveStateOffline,
			})
			continue
		}
		driveState := madmin.DriveStateCorrupt
		switch errs[index] {
		case errFileNotFound, errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		}
		result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[index],
			State:    driveState,
		})
		result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[index],
			State:    driveState,
		})
	}

	if !lfi.IsValid() {
		// Default to most common configuration for erasure blocks.
		result.ParityBlocks = defaultParityCount
		result.DataBlocks = len(storageDisks) - defaultParityCount
	} else {
		result.ParityBlocks = lfi.Erasure.ParityBlocks
		result.DataBlocks = lfi.Erasure.DataBlocks
	}

	return result
}

// Stat all directories.
func statAllDirs(ctx context.Context, storageDisks []StorageAPI, bucket, prefix string) []error {
	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			entries, err := storageDisks[index].ListDir(ctx, bucket, prefix, 1)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return errVolumeNotEmpty
			}
			return nil
		}, index)
	}

	return g.Wait()
}

// isAllNotFound will return if any element of the error slice is not
// errFileNotFound, errFileVersionNotFound or errVolumeNotFound.
// A 0 length slice will always return false.
func isAllNotFound(errs []error) bool {
	for _, err := range errs {
		if errors.Is(err, errFileNotFound) || errors.Is(err, errVolumeNotFound) || errors.Is(err, errFileVersionNotFound) {
			continue
		}
		return false
	}
	return len(errs) > 0
}

// ObjectDir is considered dangling/corrupted if any only
// if total disks - a combination of corrupted and missing
// files is lesser than N/2+1 number of disks.
// If no files were found false will be returned.
func isObjectDirDangling(errs []error) (ok bool) {
	var found int
	var notFound int
	var foundNotEmpty int
	var otherFound int
	for _, readErr := range errs {
		if readErr == nil {
			found++
		} else if readErr == errFileNotFound || readErr == errVolumeNotFound {
			notFound++
		} else if readErr == errVolumeNotEmpty {
			foundNotEmpty++
		} else {
			otherFound++
		}
	}
	found = found + foundNotEmpty + otherFound
	return found < notFound && found > 0
}

func (er erasureObjects) purgeObjectDangling(ctx context.Context, bucket, object, versionID string,
	metaArr []FileInfo, errs []error, dataErrs []error, opts madmin.HealOpts) (madmin.HealResultItem, error) {

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()
	// Check if the object is dangling, if yes and user requested
	// remove we simply delete it from namespace.
	m, ok := isObjectDangling(metaArr, errs, dataErrs)
	if ok {
		parityBlocks := m.Erasure.ParityBlocks
		if m.Erasure.ParityBlocks == 0 {
			parityBlocks = er.defaultParityCount
		}
		dataBlocks := m.Erasure.DataBlocks
		if m.Erasure.DataBlocks == 0 {
			dataBlocks = len(storageDisks) - parityBlocks
		}
		writeQuorum := dataBlocks
		if dataBlocks == parityBlocks {
			writeQuorum++
		}
		var err error
		var returnNotFound bool
		if !opts.DryRun && opts.Remove {
			if versionID == "" {
				err = er.deleteObject(ctx, bucket, object, writeQuorum)
			} else {
				err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, FileInfo{VersionID: versionID}, false)
			}

			// If Delete was successful, make sure to return the appropriate error
			// and heal result appropriate with delete's error messages
			errs = make([]error, len(errs))
			for i := range errs {
				errs[i] = err
			}
			if err == nil {
				// Dangling object successfully purged, size is '0'
				m.Size = 0
			}

			// Delete successfully purged dangling content, return ObjectNotFound/VersionNotFound instead.
			if countErrs(errs, nil) == len(errs) {
				returnNotFound = true
			}
		}
		if returnNotFound {
			err = toObjectErr(errFileNotFound, bucket, object)
			if versionID != "" {
				err = toObjectErr(errFileVersionNotFound, bucket, object, versionID)
			}
			return defaultHealResult(m, storageDisks, storageEndpoints, errs, bucket, object, versionID, er.defaultParityCount), err
		}
		return defaultHealResult(m, storageDisks, storageEndpoints, errs, bucket, object, versionID, er.defaultParityCount), toObjectErr(err, bucket, object, versionID)
	}

	readQuorum := len(storageDisks) - er.defaultParityCount

	err := toObjectErr(reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum), bucket, object, versionID)
	return defaultHealResult(m, storageDisks, storageEndpoints, errs, bucket, object, versionID, er.defaultParityCount), err
}

// Object is considered dangling/corrupted if any only
// if total disks - a combination of corrupted and missing
// files is lesser than number of data blocks.
func isObjectDangling(metaArr []FileInfo, errs []error, dataErrs []error) (validMeta FileInfo, ok bool) {
	// We can consider an object data not reliable
	// when er.meta is not found in read quorum disks.
	// or when er.meta is not readable in read quorum disks.
	var notFoundErasureMeta, corruptedErasureMeta int
	for _, readErr := range errs {
		if errors.Is(readErr, errFileNotFound) || errors.Is(readErr, errFileVersionNotFound) {
			notFoundErasureMeta++
		} else if errors.Is(readErr, errCorruptedFormat) {
			corruptedErasureMeta++
		}
	}
	var notFoundParts int
	for i := range dataErrs {
		// Only count part errors, if the error is not
		// same as er.meta error. This is to avoid
		// double counting when both parts and er.meta
		// are not available.
		if errs[i] != dataErrs[i] {
			if IsErr(dataErrs[i], []error{
				errFileNotFound,
				errFileVersionNotFound,
			}...) {
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

	if validMeta.Deleted || validMeta.IsRemote() {
		// notFoundParts is ignored since a
		// - delete marker does not have any parts
		// - transition status of complete has no parts
		return validMeta, corruptedErasureMeta+notFoundErasureMeta > len(errs)/2
	}

	// We couldn't find any valid meta we are indeed corrupted, return true right away.
	if validMeta.Erasure.DataBlocks == 0 {
		return validMeta, true
	}

	// We have valid meta, now verify if we have enough files with parity blocks.
	return validMeta, corruptedErasureMeta+notFoundErasureMeta+notFoundParts > validMeta.Erasure.ParityBlocks
}

// HealObject - heal the given object, automatically deletes the object if stale/corrupted if `remove` is true.
func (er erasureObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (hr madmin.HealResultItem, err error) {
	// Create context that also contains information about the object and bucket.
	// The top level handler might not have this information.
	reqInfo := logger.GetReqInfo(ctx)
	var newReqInfo *logger.ReqInfo
	if reqInfo != nil {
		newReqInfo = logger.NewReqInfo(reqInfo.RemoteHost, reqInfo.UserAgent, reqInfo.DeploymentID, reqInfo.RequestID, reqInfo.API, bucket, object)
	} else {
		newReqInfo = logger.NewReqInfo("", "", globalDeploymentID, "", "Heal", bucket, object)
	}
	healCtx := logger.SetReqInfo(GlobalContext, newReqInfo)

	// Healing directories handle it separately.
	if HasSuffix(object, SlashSeparator) {
		return er.healObjectDir(healCtx, bucket, object, opts.DryRun, opts.Remove)
	}

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// When versionID is empty, we read directly from the `null` versionID for healing.
	if versionID == "" {
		versionID = nullVersionID
	}

	// Perform quick read without lock.
	// This allows to quickly check if all is ok or all are missing.
	partsMetadata, errs := readAllFileInfo(healCtx, storageDisks, bucket, object, versionID, false)
	if isAllNotFound(errs) {
		err = toObjectErr(errFileNotFound, bucket, object)
		if versionID != "" {
			err = toObjectErr(errFileVersionNotFound, bucket, object, versionID)
		}
		// Nothing to do, file is already gone.
		return defaultHealResult(FileInfo{}, storageDisks, storageEndpoints, errs, bucket, object, versionID, er.defaultParityCount), err
	}

	// Return early if all ok and not deep scanning.
	if opts.ScanMode == madmin.HealNormalScan && fileInfoConsistent(ctx, partsMetadata, errs) {
		fi, err := getLatestFileInfo(ctx, partsMetadata, errs)
		if err == nil && fi.VersionID == versionID {
			return defaultHealResult(fi, storageDisks, storageEndpoints, errs, bucket, object, versionID, fi.Erasure.ParityBlocks), nil
		}
	}

	// Heal the object.
	return er.healObject(healCtx, bucket, object, versionID, opts)
}
