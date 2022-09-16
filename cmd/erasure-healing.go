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

	"github.com/minio/madmin-go/v2"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

const reservedMetadataPrefixLowerDataShardFix = ReservedMetadataPrefixLower + "data-shard-fix"

//go:generate stringer -type=healingMetric -trimprefix=healingMetric $GOFILE

type healingMetric uint8

const (
	healingMetricBucket healingMetric = iota
	healingMetricObject
	healingMetricCheckAbandonedParts
)

// AcceptableDelta returns 'true' if the fi.DiskMTime is under
// acceptable delta of "delta" duration with maxTime.
//
// This code is primarily used for heuristic detection of
// incorrect shards, as per https://github.com/minio/minio/pull/13803
//
// This check only is active if we could find maximally
// occurring disk mtimes that are somewhat same across
// the quorum. Allowing to skip those shards which we
// might think are wrong.
func (fi FileInfo) AcceptableDelta(maxTime time.Time, delta time.Duration) bool {
	diff := maxTime.Sub(fi.DiskMTime)
	if diff < 0 {
		diff = -diff
	}
	return diff < delta
}

// DataShardFixed - data shard fixed?
func (fi FileInfo) DataShardFixed() bool {
	return fi.Metadata[reservedMetadataPrefixLowerDataShardFix] == "true"
}

// Heals a bucket if it doesn't exist on one of the disks, additionally
// also heals the missing entries for bucket metadata files
// `policy.json, notification.xml, listeners.json`.
func (er erasureObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (
	result madmin.HealResultItem, err error,
) {
	if !opts.DryRun {
		defer NSUpdated(bucket, slashSeparator)
	}

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// Heal bucket.
	return er.healBucket(ctx, storageDisks, storageEndpoints, bucket, opts)
}

// Heal bucket - create buckets on disks where it does not exist.
func (er erasureObjects) healBucket(ctx context.Context, storageDisks []StorageAPI, storageEndpoints []Endpoint, bucket string, opts madmin.HealOpts) (res madmin.HealResultItem, err error) {
	// get write quorum for an object
	writeQuorum := len(storageDisks) - er.defaultParityCount
	if writeQuorum == er.defaultParityCount {
		writeQuorum++
	}

	if globalTrace.NumSubscribers(madmin.TraceHealing) > 0 {
		startTime := time.Now()
		defer func() {
			healTrace(healingMetricBucket, startTime, bucket, "", &opts, err, &res)
		}()
	}

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

			beforeState[index] = madmin.DriveStateOk
			afterState[index] = madmin.DriveStateOk

			if bucket == minioReservedBucket {
				return nil
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
			return nil
		}, index)
	}

	errs := g.Wait()

	// Initialize heal result info
	res = madmin.HealResultItem{
		Type:         madmin.HealItemBucket,
		Bucket:       bucket,
		DiskCount:    len(storageDisks),
		ParityBlocks: er.defaultParityCount,
		DataBlocks:   len(storageDisks) - er.defaultParityCount,
	}

	for i := range beforeState {
		res.Before.Drives = append(res.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i].String(),
			State:    beforeState[i],
		})
	}

	reducedErr := reduceReadQuorumErrs(ctx, errs, bucketOpIgnoredErrs, res.DataBlocks)
	if errors.Is(reducedErr, errVolumeNotFound) && !opts.Recreate {
		for i := range beforeState {
			res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i].String(),
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
		// If we have exactly half the drives not available,
		// we should still allow HealBucket to not return error.
		// this is necessary for starting the server.
		readQuorum := res.DataBlocks
		switch reduceReadQuorumErrs(ctx, errs, nil, readQuorum) {
		case nil:
		case errDiskNotFound:
		default:
			return res, reducedErr
		}
	}

	for i := range afterState {
		res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i].String(),
			State:    afterState[i],
		})
	}
	return res, nil
}

// listAllBuckets lists all buckets from all disks. It also
// returns the occurrence of each buckets in all disks
func listAllBuckets(ctx context.Context, storageDisks []StorageAPI, healBuckets map[string]VolInfo, readQuorum int) error {
	g := errgroup.WithNErrs(len(storageDisks))
	var mu sync.Mutex
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				// we ignore disk not found errors
				return nil
			}
			if storageDisks[index].Healing() != nil {
				// we ignore disks under healing
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
	return reduceReadQuorumErrs(ctx, g.Wait(), bucketMetadataOpIgnoredErrs, readQuorum)
}

// Only heal on disks where we are sure that healing is needed. We can expand
// this list as and when we figure out more errors can be added to this list safely.
func shouldHealObjectOnDisk(erErr, dataErr error, meta FileInfo, latestMeta FileInfo, doinline bool) bool {
	switch {
	case errors.Is(erErr, errFileNotFound) || errors.Is(erErr, errFileVersionNotFound):
		return true
	case errors.Is(erErr, errFileCorrupt):
		return true
	}
	if erErr == nil {
		if meta.XLV1 {
			// Legacy means heal always
			// always check first.
			return true
		}
		if doinline {
			// convert small files to 'inline'
			return true
		}
		if !meta.Deleted && !meta.IsRemote() {
			// If xl.meta was read fine but there may be problem with the part.N files.
			if IsErr(dataErr, []error{
				errFileNotFound,
				errFileVersionNotFound,
				errFileCorrupt,
			}...) {
				return true
			}
		}
		if !latestMeta.Equals(meta) {
			return true
		}
	}
	return false
}

// Heals an object by re-writing corrupt/missing erasure blocks.
func (er *erasureObjects) healObject(ctx context.Context, bucket string, object string, versionID string, opts madmin.HealOpts) (result madmin.HealResultItem, err error) {
	dryRun := opts.DryRun
	scanMode := opts.ScanMode

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	if globalTrace.NumSubscribers(madmin.TraceHealing) > 0 {
		startTime := time.Now()
		defer func() {
			healTrace(healingMetricObject, startTime, bucket, object, &opts, err, &result)
		}()
	}
	// Initialize heal result object
	result = madmin.HealResultItem{
		Type:      madmin.HealItemObject,
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		DiskCount: len(storageDisks),
	}

	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return result, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	// Re-read when we have lock...
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, bucket, object, versionID, true)
	if isAllNotFound(errs) {
		err := errFileNotFound
		if versionID != "" {
			err = errFileVersionNotFound
		}
		// Nothing to do, file is already gone.
		return er.defaultHealResult(FileInfo{}, storageDisks, storageEndpoints,
			errs, bucket, object, versionID), err
	}

	readQuorum, _, err := objectQuorumFromMeta(ctx, partsMetadata, errs, er.defaultParityCount)
	if err != nil {
		m, err := er.deleteIfDangling(ctx, bucket, object, partsMetadata, errs, nil, ObjectOptions{
			VersionID: versionID,
		})
		errs = make([]error, len(errs))
		for i := range errs {
			errs[i] = err
		}
		if err == nil {
			// Dangling object successfully purged, size is '0'
			m.Size = 0
		}
		// Generate file/version not found with default heal result
		err = errFileNotFound
		if versionID != "" {
			err = errFileVersionNotFound
		}
		return er.defaultHealResult(m, storageDisks, storageEndpoints,
			errs, bucket, object, versionID), err
	}

	result.ParityBlocks = result.DiskCount - readQuorum
	result.DataBlocks = readQuorum

	// List of disks having latest version of the object xl.meta
	// (by modtime).
	onlineDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Latest FileInfo for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, err := pickValidFileInfo(ctx, partsMetadata, modTime, readQuorum)
	if err != nil {
		return result, err
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
	availableDisks, dataErrs, diskMTime := disksWithAllParts(ctx, onlineDisks, partsMetadata,
		errs, latestMeta, bucket, object, scanMode)

	var erasure Erasure
	var recreate bool
	if !latestMeta.Deleted && !latestMeta.IsRemote() {
		// Initialize erasure coding
		erasure, err = NewErasure(ctx, latestMeta.Erasure.DataBlocks,
			latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
		if err != nil {
			return result, err
		}

		// Is only 'true' if the opts.Recreate is true and
		// the object shardSize < smallFileThreshold do not
		// set this to 'true' arbitrarily and must be only
		// 'true' with caller ask.
		recreate = (opts.Recreate &&
			!latestMeta.InlineData() &&
			len(latestMeta.Parts) == 1 &&
			erasure.ShardFileSize(latestMeta.Parts[0].ActualSize) < smallFileThreshold)
	}

	// Loop to find number of disks with valid data, per-drive
	// data state and a list of outdated disks on which data needs
	// to be healed.
	outDatedDisks := make([]StorageAPI, len(storageDisks))
	disksToHealCount := 0
	for i, v := range availableDisks {
		driveState := ""
		switch {
		case v != nil:
			driveState = madmin.DriveStateOk
			// If data is sane on any one disk, we can
			// extract the correct object size.
			result.ObjectSize = partsMetadata[i].Size
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

		if shouldHealObjectOnDisk(errs[i], dataErrs[i], partsMetadata[i], latestMeta, recreate) {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i].String(),
				State:    driveState,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[i].String(),
				State:    driveState,
			})
			continue
		}
		result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i].String(),
			State:    driveState,
		})
		result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[i].String(),
			State:    driveState,
		})
	}

	if isAllNotFound(errs) {
		// File is fully gone, fileInfo is empty.
		err := errFileNotFound
		if versionID != "" {
			err = errFileVersionNotFound
		}
		return er.defaultHealResult(FileInfo{}, storageDisks, storageEndpoints, errs,
			bucket, object, versionID), err
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

	if !latestMeta.XLV1 && !latestMeta.Deleted && !recreate && disksToHealCount > latestMeta.Erasure.ParityBlocks {
		// When disk to heal count is greater than parity blocks we should simply error out.
		err := fmt.Errorf("more drives are expected to heal than parity, returned errors: %v (dataErrs %v) -> %s/%s(%s)", errs, dataErrs, bucket, object, versionID)
		logger.LogIf(ctx, err)
		return er.defaultHealResult(latestMeta, storageDisks, storageEndpoints, errs,
			bucket, object, versionID), err
	}

	cleanFileInfo := func(fi FileInfo) FileInfo {
		// Returns a copy of the 'fi' with erasure index, checksums and inline data niled.
		nfi := fi
		if !nfi.IsRemote() {
			nfi.Data = nil
			nfi.Erasure.Index = 0
			nfi.Erasure.Checksums = nil
		}
		return nfi
	}

	// We write at temporary location and then rename to final location.
	tmpID := mustGetUUID()
	migrateDataDir := mustGetUUID()

	// Reorder so that we have data disks first and parity disks next.
	latestDisks := shuffleDisks(availableDisks, latestMeta.Erasure.Distribution)
	outDatedDisks = shuffleDisks(outDatedDisks, latestMeta.Erasure.Distribution)
	partsMetadata = shufflePartsMetadata(partsMetadata, latestMeta.Erasure.Distribution)

	copyPartsMetadata := make([]FileInfo, len(partsMetadata))
	for i := range latestDisks {
		if latestDisks[i] == nil {
			continue
		}
		copyPartsMetadata[i] = partsMetadata[i]
	}

	for i := range outDatedDisks {
		if outDatedDisks[i] == nil {
			continue
		}
		// Make sure to write the FileInfo information
		// that is expected to be in quorum.
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
	if !latestMeta.Deleted && !latestMeta.IsRemote() {
		if latestMeta.InlineData() || recreate {
			inlineBuffers = make([]*bytes.Buffer, len(outDatedDisks))
		}

		erasureInfo := latestMeta.Erasure
		for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
			partSize := latestMeta.Parts[partIndex].Size
			partActualSize := latestMeta.Parts[partIndex].ActualSize
			partModTime := latestMeta.Parts[partIndex].ModTime
			partNumber := latestMeta.Parts[partIndex].Number
			partIdx := latestMeta.Parts[partIndex].Index
			partChecksums := latestMeta.Parts[partIndex].Checksums
			tillOffset := erasure.ShardFileOffset(0, partSize, partSize)
			readers := make([]io.ReaderAt, len(latestDisks))
			checksumAlgo := erasureInfo.GetChecksumInfo(partNumber).Algorithm
			for i, disk := range latestDisks {
				if disk == OfflineDisk {
					continue
				}
				checksumInfo := copyPartsMetadata[i].Erasure.GetChecksumInfo(partNumber)
				partPath := pathJoin(object, srcDataDir, fmt.Sprintf("part.%d", partNumber))
				readers[i] = newBitrotReader(disk, copyPartsMetadata[i].Data, bucket, partPath, tillOffset, checksumAlgo,
					checksumInfo.Hash, erasure.ShardSize())
			}
			writers := make([]io.Writer, len(outDatedDisks))
			for i, disk := range outDatedDisks {
				if disk == OfflineDisk {
					continue
				}
				partPath := pathJoin(tmpID, dstDataDir, fmt.Sprintf("part.%d", partNumber))
				if len(inlineBuffers) > 0 {
					inlineBuffers[i] = bytes.NewBuffer(make([]byte, 0, erasure.ShardFileSize(latestMeta.Size)+32))
					writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
				} else {
					writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, partPath,
						tillOffset, DefaultBitrotAlgorithm, erasure.ShardSize())
				}
			}

			// Heal each part. erasure.Heal() will write the healed
			// part to .minio/tmp/uuid/ which needs to be renamed
			// later to the final location.
			err = erasure.Heal(ctx, writers, readers, partSize)
			closeBitrotReaders(readers)
			closeBitrotWriters(writers)
			if err != nil {
				return result, err
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
				partsMetadata[i].AddObjectPart(partNumber, "", partSize, partActualSize, partModTime, partIdx, partChecksums, 0)
				partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
					PartNumber: partNumber,
					Algorithm:  checksumAlgo,
					Hash:       bitrotWriterSum(writers[i]),
				})
				if len(inlineBuffers) > 0 && inlineBuffers[i] != nil {
					partsMetadata[i].Data = inlineBuffers[i].Bytes()
					partsMetadata[i].SetInlineData()
				} else {
					partsMetadata[i].Data = nil
				}
			}

			// If all disks are having errors, we give up.
			if disksToHealCount == 0 {
				return result, fmt.Errorf("all drives had write errors, unable to heal %s/%s", bucket, object)
			}

		}

	}

	defer er.deleteAll(context.Background(), minioMetaTmpBucket, tmpID)

	// Rename from tmp location to the actual location.
	for i, disk := range outDatedDisks {
		if disk == OfflineDisk {
			continue
		}

		// record the index of the updated disks
		partsMetadata[i].Erasure.Index = i + 1

		// Attempt a rename now from healed data to final location.
		if _, err = disk.RenameData(ctx, minioMetaTmpBucket, tmpID, partsMetadata[i], bucket, object); err != nil {
			logger.LogIf(ctx, err)
			return result, err
		}

		// - Remove any parts from healed disks after its been inlined.
		// - Remove any remaining parts from outdated disks from before transition.
		if recreate || partsMetadata[i].IsRemote() {
			rmDataDir := partsMetadata[i].DataDir
			disk.DeleteVol(ctx, pathJoin(bucket, encodeDirObject(object), rmDataDir), true)
		}

		for i, v := range result.Before.Drives {
			if v.Endpoint == disk.String() {
				result.After.Drives[i].State = madmin.DriveStateOk
			}
		}
	}

	if !diskMTime.Equal(timeSentinel) && !diskMTime.IsZero() {
		// Update metadata to indicate special fix.
		_, err = er.PutObjectMetadata(ctx, bucket, object, ObjectOptions{
			NoLock: true,
			UserDefined: map[string]string{
				reservedMetadataPrefixLowerDataShardFix: "true",
				// another reserved metadata to capture original disk-mtime
				// captured for this version of the object, to be used
				// possibly in future to heal other versions if possible.
				ReservedMetadataPrefixLower + "disk-mtime": diskMTime.String(),
			},
		})
	}

	// Set the size of the object in the heal result
	result.ObjectSize = latestMeta.Size

	return result, nil
}

// checkAbandonedParts will check if an object has abandoned parts,
// meaning data-dirs or inlined data that are no longer referenced by the xl.meta
// Errors are generally ignored by this function.
func (er *erasureObjects) checkAbandonedParts(ctx context.Context, bucket string, object string, opts madmin.HealOpts) (err error) {
	if !opts.Remove || opts.DryRun {
		return nil
	}
	if globalTrace.NumSubscribers(madmin.TraceHealing) > 0 {
		startTime := time.Now()
		defer func() {
			healTrace(healingMetricCheckAbandonedParts, startTime, bucket, object, nil, err, nil)
		}()
	}
	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}
	var wg sync.WaitGroup
	for _, disk := range er.getDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				_ = disk.CleanAbandonedData(ctx, bucket, object)
			}(disk)
		}
	}
	wg.Wait()
	return nil
}

// healObjectDir - heals object directory specifically, this special call
// is needed since we do not have a special backend format for directories.
func (er *erasureObjects) healObjectDir(ctx context.Context, bucket, object string, dryRun bool, remove bool) (hr madmin.HealResultItem, err error) {
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
					_ = disk.Delete(ctx, bucket, object, DeleteOptions{
						Recursive: false,
						Force:     false,
					})
				}(index, disk)
			}
			wg.Wait()
			NSUpdated(bucket, object)
		}
	}

	// Prepare object creation in all disks
	for i, err := range errs {
		drive := storageEndpoints[i].String()
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
	if danglingObject || isAllNotFound(errs) {
		// Nothing to do, file is already gone.
		return hr, errFileNotFound
	}

	if dryRun {
		// Quit without try to heal the object dir
		return hr, nil
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
func (er *erasureObjects) defaultHealResult(lfi FileInfo, storageDisks []StorageAPI, storageEndpoints []Endpoint, errs []error, bucket, object, versionID string) madmin.HealResultItem {
	// Initialize heal result object
	result := madmin.HealResultItem{
		Type:       madmin.HealItemObject,
		Bucket:     bucket,
		Object:     object,
		ObjectSize: lfi.Size,
		VersionID:  versionID,
		DiskCount:  len(storageDisks),
	}

	if lfi.IsValid() {
		result.ParityBlocks = lfi.Erasure.ParityBlocks
	} else {
		// Default to most common configuration for erasure blocks.
		result.ParityBlocks = er.defaultParityCount
	}
	result.DataBlocks = len(storageDisks) - result.ParityBlocks

	for index, disk := range storageDisks {
		if disk == nil {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[index].String(),
				State:    madmin.DriveStateOffline,
			})
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: storageEndpoints[index].String(),
				State:    madmin.DriveStateOffline,
			})
			continue
		}
		driveState := madmin.DriveStateCorrupt
		switch errs[index] {
		case errFileNotFound, errVolumeNotFound:
			driveState = madmin.DriveStateMissing
		case nil:
			driveState = madmin.DriveStateOk
		}
		result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[index].String(),
			State:    driveState,
		})
		result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
			UUID:     "",
			Endpoint: storageEndpoints[index].String(),
			State:    driveState,
		})
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
		if err != nil {
			switch err.Error() {
			case errFileNotFound.Error():
				fallthrough
			case errVolumeNotFound.Error():
				fallthrough
			case errFileVersionNotFound.Error():
				continue
			}
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

// Object is considered dangling/corrupted if any only
// if total disks - a combination of corrupted and missing
// files is lesser than number of data blocks.
func isObjectDangling(metaArr []FileInfo, errs []error, dataErrs []error) (validMeta FileInfo, ok bool) {
	// We can consider an object data not reliable
	// when xl.meta is not found in read quorum disks.
	// or when xl.meta is not readable in read quorum disks.
	danglingErrsCount := func(cerrs []error) (int, int, int) {
		var (
			notFoundCount     int
			corruptedCount    int
			diskNotFoundCount int
		)
		for _, readErr := range cerrs {
			if errors.Is(readErr, errFileNotFound) || errors.Is(readErr, errFileVersionNotFound) {
				notFoundCount++
			} else if errors.Is(readErr, errFileCorrupt) {
				corruptedCount++
			} else if errors.Is(readErr, errDiskNotFound) {
				diskNotFoundCount++
			}
		}
		return notFoundCount, corruptedCount, diskNotFoundCount
	}

	ndataErrs := make([]error, len(dataErrs))
	for i := range dataErrs {
		if errs[i] != dataErrs[i] {
			// Only count part errors, if the error is not
			// same as xl.meta error. This is to avoid
			// double counting when both parts and xl.meta
			// are not available.
			ndataErrs[i] = dataErrs[i]
		}
	}

	notFoundMetaErrs, corruptedMetaErrs, driveNotFoundMetaErrs := danglingErrsCount(errs)
	notFoundPartsErrs, corruptedPartsErrs, driveNotFoundPartsErrs := danglingErrsCount(ndataErrs)

	if driveNotFoundMetaErrs > 0 || driveNotFoundPartsErrs > 0 {
		return validMeta, false
	}
	for _, m := range metaArr {
		if m.IsValid() {
			validMeta = m
			break
		}
	}

	if !validMeta.IsValid() {
		// We have no idea what this file is, leave it as is.
		return validMeta, false
	}

	if validMeta.Deleted {
		// notFoundPartsErrs is ignored since
		// - delete marker does not have any parts
		return validMeta, corruptedMetaErrs+notFoundMetaErrs > len(errs)/2
	}

	totalErrs := notFoundMetaErrs + corruptedMetaErrs + notFoundPartsErrs + corruptedPartsErrs
	if validMeta.IsRemote() {
		// notFoundPartsErrs is ignored since
		// - transition status of complete has no parts
		totalErrs = notFoundMetaErrs + corruptedMetaErrs
	}

	// We have valid meta, now verify if we have enough files with parity blocks.
	return validMeta, totalErrs > validMeta.Erasure.ParityBlocks
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
		hr, err := er.healObjectDir(healCtx, bucket, object, opts.DryRun, opts.Remove)
		return hr, toObjectErr(err, bucket, object)
	}

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	// When versionID is empty, we read directly from the `null` versionID for healing.
	if versionID == "" {
		versionID = nullVersionID
	}

	// Perform quick read without lock.
	// This allows to quickly check if all is ok or all are missing.
	_, errs := readAllFileInfo(healCtx, storageDisks, bucket, object, versionID, false)
	if isAllNotFound(errs) {
		err := errFileNotFound
		if versionID != "" {
			err = errFileVersionNotFound
		}
		// Nothing to do, file is already gone.
		return er.defaultHealResult(FileInfo{}, storageDisks, storageEndpoints,
			errs, bucket, object, versionID), toObjectErr(err, bucket, object, versionID)
	}

	// Heal the object.
	hr, err = er.healObject(healCtx, bucket, object, versionID, opts)
	if errors.Is(err, errFileCorrupt) && opts.ScanMode != madmin.HealDeepScan {
		// Instead of returning an error when a bitrot error is detected
		// during a normal heal scan, heal again with bitrot flag enabled.
		opts.ScanMode = madmin.HealDeepScan
		hr, err = er.healObject(healCtx, bucket, object, versionID, opts)
	}
	return hr, toObjectErr(err, bucket, object, versionID)
}

// healTrace sends healing results to trace output.
func healTrace(funcName healingMetric, startTime time.Time, bucket, object string, opts *madmin.HealOpts, err error, result *madmin.HealResultItem) {
	tr := madmin.TraceInfo{
		TraceType: madmin.TraceHealing,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  "heal." + funcName.String(),
		Duration:  time.Since(startTime),
		Path:      pathJoin(bucket, decodeDirObject(object)),
	}
	if opts != nil {
		tr.Message = fmt.Sprintf("dry:%v, rm:%v, recreate:%v mode:%v", opts.DryRun, opts.Remove, opts.Recreate, opts.ScanMode)
	}
	if err != nil {
		tr.Error = err.Error()
	} else {
		tr.HealResult = result
	}
	globalTrace.Publish(tr)
}
