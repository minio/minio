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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/puzpuzpuz/xsync/v3"
)

//go:generate stringer -type=healingMetric -trimprefix=healingMetric $GOFILE

type healingMetric uint8

const (
	healingMetricBucket healingMetric = iota
	healingMetricObject
	healingMetricCheckAbandonedParts
)

// List a prefix or a single object versions and heal
func (er erasureObjects) listAndHeal(ctx context.Context, bucket, prefix string, recursive bool, scanMode madmin.HealScanMode, healEntry func(string, metaCacheEntry, madmin.HealScanMode) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	disks, _ := er.getOnlineDisksWithHealing(false)
	if len(disks) == 0 {
		return errors.New("listAndHeal: No non-healing drives found")
	}

	// How to resolve partial results.
	resolver := metadataResolutionParams{
		dirQuorum: 1,
		objQuorum: 1,
		bucket:    bucket,
		strict:    false, // Allow less strict matching.
	}

	path := baseDirFromPrefix(prefix)
	filterPrefix := strings.Trim(strings.TrimPrefix(prefix, path), slashSeparator)
	if path == prefix {
		filterPrefix = ""
	}

	lopts := listPathRawOptions{
		disks:          disks,
		bucket:         bucket,
		path:           path,
		filterPrefix:   filterPrefix,
		recursive:      recursive,
		forwardTo:      "",
		minDisks:       1,
		reportNotFound: false,
		agreed: func(entry metaCacheEntry) {
			if !recursive && prefix != entry.name {
				return
			}
			if err := healEntry(bucket, entry, scanMode); err != nil {
				cancel()
			}
		},
		partial: func(entries metaCacheEntries, _ []error) {
			entry, ok := entries.resolve(&resolver)
			if !ok {
				// check if we can get one entry at least
				// proceed to heal nonetheless.
				entry, _ = entries.firstFound()
			}
			if !recursive && prefix != entry.name {
				return
			}
			if err := healEntry(bucket, *entry, scanMode); err != nil {
				cancel()
				return
			}
		},
		finished: nil,
	}

	if err := listPathRaw(ctx, lopts); err != nil {
		return fmt.Errorf("listPathRaw returned %w: opts(%#v)", err, lopts)
	}

	return nil
}

// listAllBuckets lists all buckets from all disks. It also
// returns the occurrence of each buckets in all disks
func listAllBuckets(ctx context.Context, storageDisks []StorageAPI, healBuckets *xsync.MapOf[string, VolInfo], readQuorum int) error {
	g := errgroup.WithNErrs(len(storageDisks))
	for index := range storageDisks {
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

				healBuckets.Compute(volInfo.Name, func(oldValue VolInfo, loaded bool) (newValue VolInfo, del bool) {
					if loaded {
						newValue = oldValue
						newValue.count = oldValue.count + 1
						return newValue, false
					}
					return VolInfo{
						Name:    volInfo.Name,
						Created: volInfo.Created,
						count:   1,
					}, false
				})
			}

			return nil
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), bucketMetadataOpIgnoredErrs, readQuorum); err != nil {
		return err
	}

	healBuckets.Range(func(volName string, volInfo VolInfo) bool {
		if volInfo.count < readQuorum {
			healBuckets.Delete(volName)
		}
		return true
	})

	return nil
}

var (
	errLegacyXLMeta   = errors.New("legacy XL meta")
	errOutdatedXLMeta = errors.New("outdated XL meta")
	errPartCorrupt    = errors.New("part corrupt")
	errPartMissing    = errors.New("part missing")
)

// Only heal on disks where we are sure that healing is needed. We can expand
// this list as and when we figure out more errors can be added to this list safely.
func shouldHealObjectOnDisk(erErr error, partsErrs []int, meta FileInfo, latestMeta FileInfo) (bool, bool, error) {
	if errors.Is(erErr, errFileNotFound) || errors.Is(erErr, errFileVersionNotFound) || errors.Is(erErr, errFileCorrupt) {
		return true, true, erErr
	}
	if erErr == nil {
		if meta.XLV1 {
			// Legacy means heal always
			// always check first.
			return true, true, errLegacyXLMeta
		}
		if !latestMeta.Equals(meta) {
			return true, true, errOutdatedXLMeta
		}
		if !meta.Deleted && !meta.IsRemote() {
			// If xl.meta was read fine but there may be problem with the part.N files.
			for _, partErr := range partsErrs {
				if partErr == checkPartFileNotFound {
					return true, false, errPartMissing
				}
				if partErr == checkPartFileCorrupt {
					return true, false, errPartCorrupt
				}
			}
		}
		return false, false, nil
	}
	return false, false, erErr
}

const (
	xMinIOHealing = ReservedMetadataPrefix + "healing"
	xMinIODataMov = ReservedMetadataPrefix + "data-mov"
)

// SetHealing marks object (version) as being healed.
// Note: this is to be used only from healObject
func (fi *FileInfo) SetHealing() {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string)
	}
	fi.Metadata[xMinIOHealing] = "true"
}

// Healing returns true if object is being healed (i.e fi is being passed down
// from healObject)
func (fi FileInfo) Healing() bool {
	_, ok := fi.Metadata[xMinIOHealing]
	return ok
}

// SetDataMov marks object (version) as being currently
// in movement, such as decommissioning or rebalance.
func (fi *FileInfo) SetDataMov() {
	if fi.Metadata == nil {
		fi.Metadata = make(map[string]string)
	}
	fi.Metadata[xMinIODataMov] = "true"
}

// DataMov returns true if object is being in movement
func (fi FileInfo) DataMov() bool {
	_, ok := fi.Metadata[xMinIODataMov]
	return ok
}

func (er *erasureObjects) auditHealObject(ctx context.Context, bucket, object, versionID string, result madmin.HealResultItem, err error) {
	if len(logger.AuditTargets()) == 0 {
		return
	}

	opts := AuditLogOptions{
		Event:     "HealObject",
		Bucket:    bucket,
		Object:    decodeDirObject(object),
		VersionID: versionID,
	}
	if err != nil {
		opts.Error = err.Error()
	}

	b, a := result.GetCorruptedCounts()
	if b > 0 && b == a {
		opts.Error = fmt.Sprintf("unable to heal %d corrupted blocks on drives", b)
	}

	b, a = result.GetMissingCounts()
	if b > 0 && b == a {
		opts.Error = fmt.Sprintf("unable to heal %d missing blocks on drives", b)
	}

	opts.Tags = map[string]string{
		"healObject": auditObjectOp{
			Name: opts.Object,
			Pool: er.poolIndex + 1,
			Set:  er.setIndex + 1,
		}.String(),
	}

	auditLogInternal(ctx, opts)
}

func objectErrToDriveState(reason error) string {
	switch {
	case reason == nil:
		return madmin.DriveStateOk
	case IsErr(reason, errDiskNotFound):
		return madmin.DriveStateOffline
	case IsErr(reason, errFileNotFound, errFileVersionNotFound, errVolumeNotFound, errPartMissing, errOutdatedXLMeta, errLegacyXLMeta):
		return madmin.DriveStateMissing
	case IsErr(reason, errFileCorrupt, errPartCorrupt):
		return madmin.DriveStateCorrupt
	default:
		return fmt.Sprintf("%s (%s)", madmin.DriveStateUnknown, reason.Error())
	}
}

// Heals an object by re-writing corrupt/missing erasure blocks.
func (er *erasureObjects) healObject(ctx context.Context, bucket string, object string, versionID string, opts madmin.HealOpts) (result madmin.HealResultItem, err error) {
	dryRun := opts.DryRun
	scanMode := opts.ScanMode

	storageDisks := er.getDisks()
	storageEndpoints := er.getEndpoints()

	defer func() {
		er.auditHealObject(ctx, bucket, object, versionID, result, err)
	}()

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
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, "", bucket, object, versionID, true, true)
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
		m, derr := er.deleteIfDangling(ctx, bucket, object, partsMetadata, errs, nil, ObjectOptions{
			VersionID: versionID,
		})
		errs = make([]error, len(errs))
		if derr == nil {
			derr = errFileNotFound
			if versionID != "" {
				derr = errFileVersionNotFound
			}
			// We did find a new danging object
			return er.defaultHealResult(m, storageDisks, storageEndpoints,
				errs, bucket, object, versionID), derr
		}
		return er.defaultHealResult(m, storageDisks, storageEndpoints,
			errs, bucket, object, versionID), err
	}

	result.ParityBlocks = result.DiskCount - readQuorum
	result.DataBlocks = readQuorum

	// List of disks having latest version of the object xl.meta
	// (by modtime).
	onlineDisks, quorumModTime, quorumETag := listOnlineDisks(storageDisks, partsMetadata, errs, readQuorum)

	// Latest FileInfo for reference. If a valid metadata is not
	// present, it is as good as object not found.
	latestMeta, err := pickValidFileInfo(ctx, partsMetadata, quorumModTime, quorumETag, readQuorum)
	if err != nil {
		return result, err
	}

	// No modtime quorum
	filterDisksByETag := quorumETag != ""

	dataErrsByDisk, dataErrsByPart := checkObjectWithAllParts(ctx, onlineDisks, partsMetadata,
		errs, latestMeta, filterDisksByETag, bucket, object, scanMode)

	var erasure Erasure
	if !latestMeta.Deleted && !latestMeta.IsRemote() {
		// Initialize erasure coding
		erasure, err = NewErasure(ctx, latestMeta.Erasure.DataBlocks,
			latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
		if err != nil {
			return result, err
		}
	}

	result.ObjectSize, err = latestMeta.ToObjectInfo(bucket, object, true).GetActualSize()
	if err != nil {
		return result, err
	}

	// Loop to find number of disks with valid data, per-drive
	// data state and a list of outdated disks on which data needs
	// to be healed.
	outDatedDisks := make([]StorageAPI, len(storageDisks))
	disksToHealCount, xlMetaToHealCount := 0, 0
	for i := range onlineDisks {
		yes, isMeta, reason := shouldHealObjectOnDisk(errs[i], dataErrsByDisk[i], partsMetadata[i], latestMeta)
		if yes {
			outDatedDisks[i] = storageDisks[i]
			disksToHealCount++
			if isMeta {
				xlMetaToHealCount++
			}
		}

		driveState := objectErrToDriveState(reason)

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

	if disksToHealCount == 0 {
		// Nothing to heal!
		return result, nil
	}

	// After this point, only have to repair data on disk - so
	// return if it is a dry-run
	if dryRun {
		return result, nil
	}

	cannotHeal := !latestMeta.XLV1 && !latestMeta.Deleted && xlMetaToHealCount > latestMeta.Erasure.ParityBlocks
	if cannotHeal && quorumETag != "" {
		// This is an object that is supposed to be removed by the dangling code
		// but we noticed that ETag is the same for all objects, let's give it a shot
		cannotHeal = false
	}

	if !latestMeta.Deleted && !latestMeta.IsRemote() {
		// check if there is a part that lost its quorum
		for _, partErrs := range dataErrsByPart {
			if countPartNotSuccess(partErrs) > latestMeta.Erasure.ParityBlocks {
				cannotHeal = true
				break
			}
		}
	}

	if cannotHeal {
		// Allow for dangling deletes, on versions that have DataDir missing etc.
		// this would end up restoring the correct readable versions.
		m, err := er.deleteIfDangling(ctx, bucket, object, partsMetadata, errs, dataErrsByPart, ObjectOptions{
			VersionID: versionID,
		})
		errs = make([]error, len(errs))
		if err == nil {
			err = errFileNotFound
			if versionID != "" {
				err = errFileVersionNotFound
			}
			// We did find a new danging object
			return er.defaultHealResult(m, storageDisks, storageEndpoints,
				errs, bucket, object, versionID), err
		}
		for i := range errs {
			errs[i] = err
		}
		return er.defaultHealResult(m, storageDisks, storageEndpoints,
			errs, bucket, object, versionID), err
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

	if !latestMeta.Deleted && len(latestMeta.Erasure.Distribution) != len(onlineDisks) {
		err := fmt.Errorf("unexpected file distribution (%v) from online disks (%v), looks like backend disks have been manually modified refusing to heal %s/%s(%s)",
			latestMeta.Erasure.Distribution, onlineDisks, bucket, object, versionID)
		healingLogOnceIf(ctx, err, "heal-object-online-disks")
		return er.defaultHealResult(latestMeta, storageDisks, storageEndpoints, errs,
			bucket, object, versionID), err
	}

	latestDisks := shuffleDisks(onlineDisks, latestMeta.Erasure.Distribution)

	if !latestMeta.Deleted && len(latestMeta.Erasure.Distribution) != len(outDatedDisks) {
		err := fmt.Errorf("unexpected file distribution (%v) from outdated disks (%v), looks like backend disks have been manually modified refusing to heal %s/%s(%s)",
			latestMeta.Erasure.Distribution, outDatedDisks, bucket, object, versionID)
		healingLogOnceIf(ctx, err, "heal-object-outdated-disks")
		return er.defaultHealResult(latestMeta, storageDisks, storageEndpoints, errs,
			bucket, object, versionID), err
	}

	outDatedDisks = shuffleDisks(outDatedDisks, latestMeta.Erasure.Distribution)

	if !latestMeta.Deleted && len(latestMeta.Erasure.Distribution) != len(partsMetadata) {
		err := fmt.Errorf("unexpected file distribution (%v) from metadata entries (%v), looks like backend disks have been manually modified refusing to heal %s/%s(%s)",
			latestMeta.Erasure.Distribution, len(partsMetadata), bucket, object, versionID)
		healingLogOnceIf(ctx, err, "heal-object-metadata-entries")
		return er.defaultHealResult(latestMeta, storageDisks, storageEndpoints, errs,
			bucket, object, versionID), err
	}

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
		if latestMeta.InlineData() {
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
			prefer := make([]bool, len(latestDisks))
			checksumAlgo := erasureInfo.GetChecksumInfo(partNumber).Algorithm
			for i, disk := range latestDisks {
				if disk == OfflineDisk {
					continue
				}
				thisPartErrs := shuffleCheckParts(dataErrsByPart[partIndex], latestMeta.Erasure.Distribution)
				if thisPartErrs[i] != checkPartSuccess {
					continue
				}
				checksumInfo := copyPartsMetadata[i].Erasure.GetChecksumInfo(partNumber)
				partPath := pathJoin(object, srcDataDir, fmt.Sprintf("part.%d", partNumber))
				readers[i] = newBitrotReader(disk, copyPartsMetadata[i].Data, bucket, partPath, tillOffset, checksumAlgo,
					checksumInfo.Hash, erasure.ShardSize())
				prefer[i] = disk.Hostname() == ""
			}
			writers := make([]io.Writer, len(outDatedDisks))
			for i, disk := range outDatedDisks {
				if disk == OfflineDisk {
					continue
				}
				partPath := pathJoin(tmpID, dstDataDir, fmt.Sprintf("part.%d", partNumber))
				if len(inlineBuffers) > 0 {
					buf := grid.GetByteBufferCap(int(erasure.ShardFileSize(latestMeta.Size)) + 64)
					inlineBuffers[i] = bytes.NewBuffer(buf[:0])
					defer grid.PutByteBuffer(buf)

					writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
				} else {
					writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, partPath,
						tillOffset, DefaultBitrotAlgorithm, erasure.ShardSize())
				}
			}

			// Heal each part. erasure.Heal() will write the healed
			// part to .minio/tmp/uuid/ which needs to be renamed
			// later to the final location.
			err = erasure.Heal(ctx, writers, readers, partSize, prefer)
			closeBitrotReaders(readers)
			closeErrs := closeBitrotWriters(writers)
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

				// A non-nil stale disk which got error on Close()
				if closeErrs[i] != nil {
					outDatedDisks[i] = nil
					disksToHealCount--
					continue
				}

				partsMetadata[i].DataDir = dstDataDir
				partsMetadata[i].AddObjectPart(partNumber, "", partSize, partActualSize, partModTime, partIdx, partChecksums)
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
		partsMetadata[i].SetHealing()

		if _, err = disk.RenameData(ctx, minioMetaTmpBucket, tmpID, partsMetadata[i], bucket, object, RenameOptions{}); err != nil {
			return result, err
		}

		// - Remove any remaining parts from outdated disks from before transition.
		if partsMetadata[i].IsRemote() {
			rmDataDir := partsMetadata[i].DataDir
			disk.Delete(ctx, bucket, pathJoin(encodeDirObject(object), rmDataDir), DeleteOptions{
				Immediate: true,
				Recursive: true,
			})
		}

		for i, v := range result.Before.Drives {
			if v.Endpoint == disk.Endpoint().String() {
				result.After.Drives[i].State = madmin.DriveStateOk
			}
		}
	}

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
						Immediate: false,
					})
				}(index, disk)
			}
			wg.Wait()
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
		driveState := objectErrToDriveState(errs[index])
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
			entries, err := storageDisks[index].ListDir(ctx, "", bucket, prefix, 1)
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

func isAllVolumeNotFound(errs []error) bool {
	return countErrs(errs, errVolumeNotFound) == len(errs)
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

// isAllBucketsNotFound will return true if all the errors are either errFileNotFound
// or errFileCorrupt
// A 0 length slice will always return false.
func isAllBucketsNotFound(errs []error) bool {
	if len(errs) == 0 {
		return false
	}
	notFoundCount := 0
	for _, err := range errs {
		if err != nil {
			if errors.Is(err, errVolumeNotFound) {
				notFoundCount++
			} else if isErrBucketNotFound(err) {
				notFoundCount++
			}
		}
	}
	return len(errs) == notFoundCount
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
		switch readErr {
		case nil:
			found++
		case errFileNotFound, errVolumeNotFound:
			notFound++
		case errVolumeNotEmpty:
			foundNotEmpty++
		default:
			otherFound++
		}
	}
	found = found + foundNotEmpty + otherFound
	return found < notFound && found > 0
}

func danglingMetaErrsCount(cerrs []error) (notFoundCount int, nonActionableCount int) {
	for _, readErr := range cerrs {
		if readErr == nil {
			continue
		}
		switch {
		case errors.Is(readErr, errFileNotFound) || errors.Is(readErr, errFileVersionNotFound):
			notFoundCount++
		default:
			// All other errors are non-actionable
			nonActionableCount++
		}
	}
	return notFoundCount, nonActionableCount
}

func danglingPartErrsCount(results []int) (notFoundCount int, nonActionableCount int) {
	for _, partResult := range results {
		switch partResult {
		case checkPartSuccess:
			continue
		case checkPartFileNotFound:
			notFoundCount++
		default:
			// All other errors are non-actionable
			nonActionableCount++
		}
	}
	return notFoundCount, nonActionableCount
}

// Object is considered dangling/corrupted if and only
// if total disks - a combination of corrupted and missing
// files is lesser than number of data blocks.
func isObjectDangling(metaArr []FileInfo, errs []error, dataErrsByPart map[int][]int) (validMeta FileInfo, ok bool) {
	// We can consider an object data not reliable
	// when xl.meta is not found in read quorum disks.
	// or when xl.meta is not readable in read quorum disks.
	notFoundMetaErrs, nonActionableMetaErrs := danglingMetaErrsCount(errs)

	notFoundPartsErrs, nonActionablePartsErrs := 0, 0
	for _, dataErrs := range dataErrsByPart {
		if nf, na := danglingPartErrsCount(dataErrs); nf > notFoundPartsErrs {
			notFoundPartsErrs, nonActionablePartsErrs = nf, na
		}
	}

	for _, m := range metaArr {
		if m.IsValid() {
			validMeta = m
			break
		}
	}

	if !validMeta.IsValid() {
		// validMeta is invalid because all xl.meta is missing apparently
		// we should figure out if dataDirs are also missing > dataBlocks.
		dataBlocks := (len(metaArr) + 1) / 2
		if notFoundPartsErrs > dataBlocks {
			// Not using parity to ensure that we do not delete
			// any valid content, if any is recoverable. But if
			// notFoundDataDirs are already greater than the data
			// blocks all bets are off and it is safe to purge.
			//
			// This is purely a defensive code, ideally parityBlocks
			// is sufficient, however we can't know that since we
			// do have the FileInfo{}.
			return validMeta, true
		}

		// We have no idea what this file is, leave it as is.
		return validMeta, false
	}

	if nonActionableMetaErrs > 0 || nonActionablePartsErrs > 0 {
		return validMeta, false
	}

	if validMeta.Deleted {
		// notFoundPartsErrs is ignored since
		// - delete marker does not have any parts
		dataBlocks := (len(errs) + 1) / 2
		return validMeta, notFoundMetaErrs > dataBlocks
	}

	// TODO: It is possible to replay the object via just single
	// xl.meta file, considering quorum number of data-dirs are still
	// present on other drives.
	//
	// However this requires a bit of a rewrite, leave this up for
	// future work.
	if notFoundMetaErrs > 0 && notFoundMetaErrs > validMeta.Erasure.ParityBlocks {
		// All xl.meta is beyond parity blocks missing, this is dangling
		return validMeta, true
	}

	if !validMeta.IsRemote() && notFoundPartsErrs > 0 && notFoundPartsErrs > validMeta.Erasure.ParityBlocks {
		// All data-dir is beyond parity blocks missing, this is dangling
		return validMeta, true
	}

	return validMeta, false
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
		newReqInfo = logger.NewReqInfo("", "", globalDeploymentID(), "", "Heal", bucket, object)
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
	_, errs := readAllFileInfo(healCtx, storageDisks, "", bucket, object, versionID, false, false)
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
		tr.Custom = map[string]string{
			"dry":    fmt.Sprint(opts.DryRun),
			"remove": fmt.Sprint(opts.Remove),
			"mode":   fmt.Sprint(opts.ScanMode),
		}
		if result != nil {
			tr.Custom["version-id"] = result.VersionID
			tr.Custom["disks"] = strconv.Itoa(result.DiskCount)
			tr.Bytes = result.ObjectSize
		}
	}
	if err != nil {
		tr.Error = err.Error()
	}
	tr.HealResult = result
	globalTrace.Publish(tr)
}
