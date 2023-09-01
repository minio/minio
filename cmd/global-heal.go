// Copyright (c) 2015-2022 MinIO, Inc.
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
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/console"
	"github.com/minio/pkg/v2/wildcard"
)

const (
	bgHealingUUID = "0000-0000-0000-0000"
)

// NewBgHealSequence creates a background healing sequence
// operation which scans all objects and heal them.
func newBgHealSequence() *healSequence {
	reqInfo := &logger.ReqInfo{API: "BackgroundHeal"}
	ctx, cancelCtx := context.WithCancel(logger.SetReqInfo(GlobalContext, reqInfo))

	hs := madmin.HealOpts{
		// Remove objects that do not have read-quorum
		Remove: healDeleteDangling,
	}

	return &healSequence{
		startTime:   UTCNow(),
		clientToken: bgHealingUUID,
		// run-background heal with reserved bucket
		bucket:   minioReservedBucket,
		settings: hs,
		currentStatus: healSequenceStatus{
			Summary:      healNotStartedStatus,
			HealSettings: hs,
		},
		cancelCtx:          cancelCtx,
		ctx:                ctx,
		reportProgress:     false,
		scannedItemsMap:    make(map[madmin.HealItemType]int64),
		healedItemsMap:     make(map[madmin.HealItemType]int64),
		healFailedItemsMap: make(map[string]int64),
	}
}

// getLocalBackgroundHealStatus will return the heal status of the local node
func getLocalBackgroundHealStatus(ctx context.Context, o ObjectLayer) (madmin.BgHealState, bool) {
	if globalBackgroundHealState == nil {
		return madmin.BgHealState{}, false
	}

	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !ok {
		return madmin.BgHealState{}, false
	}

	status := madmin.BgHealState{
		ScannedItemsCount: bgSeq.getScannedItemsCount(),
	}

	healDisksMap := map[string]struct{}{}
	for _, ep := range getLocalDisksToHeal() {
		healDisksMap[ep.String()] = struct{}{}
	}

	if o == nil {
		healing := globalBackgroundHealState.getLocalHealingDisks()
		for _, disk := range healing {
			status.HealDisks = append(status.HealDisks, disk.Endpoint)
		}

		return status, true
	}

	si := o.LocalStorageInfo(ctx)

	indexed := make(map[string][]madmin.Disk)
	for _, disk := range si.Disks {
		setIdx := fmt.Sprintf("%d-%d", disk.PoolIndex, disk.SetIndex)
		indexed[setIdx] = append(indexed[setIdx], disk)
	}

	for id, disks := range indexed {
		ss := madmin.SetStatus{
			ID:        id,
			SetIndex:  disks[0].SetIndex,
			PoolIndex: disks[0].PoolIndex,
		}
		for _, disk := range disks {
			ss.Disks = append(ss.Disks, disk)
			if disk.Healing {
				ss.HealStatus = "Healing"
				ss.HealPriority = "high"
				status.HealDisks = append(status.HealDisks, disk.Endpoint)
			}
		}
		sortDisks(ss.Disks)
		status.Sets = append(status.Sets, ss)
	}
	sort.Slice(status.Sets, func(i, j int) bool {
		return status.Sets[i].ID < status.Sets[j].ID
	})

	backendInfo := o.BackendInfo()
	status.SCParity = make(map[string]int)
	status.SCParity[storageclass.STANDARD] = backendInfo.StandardSCParity
	status.SCParity[storageclass.RRS] = backendInfo.RRSCParity

	return status, true
}

func mustGetHealSequence(ctx context.Context) *healSequence {
	// Get background heal sequence to send elements to heal
	for {
		globalHealStateLK.RLock()
		hstate := globalBackgroundHealState
		globalHealStateLK.RUnlock()

		if hstate == nil {
			time.Sleep(time.Second)
			continue
		}

		bgSeq, ok := hstate.getHealSequenceByToken(bgHealingUUID)
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		return bgSeq
	}
}

// healErasureSet lists and heals all objects in a specific erasure set
func (er *erasureObjects) healErasureSet(ctx context.Context, buckets []string, tracker *healingTracker) error {
	bgSeq := mustGetHealSequence(ctx)
	scanMode := madmin.HealNormalScan

	// Make sure to copy since `buckets slice`
	// is modified in place by tracker.
	healBuckets := make([]string, len(buckets))
	copy(healBuckets, buckets)

	// Heal all buckets first in this erasure set - this is useful
	// for new objects upload in different buckets to be successful
	for _, bucket := range healBuckets {
		_, err := er.HealBucket(ctx, bucket, madmin.HealOpts{ScanMode: scanMode})
		if err != nil {
			// Log bucket healing error if any, we shall retry again.
			logger.LogIf(ctx, err)
		}
	}

	var retErr error
	// Heal all buckets with all objects
	for _, bucket := range healBuckets {
		if tracker.isHealed(bucket) {
			continue
		}
		var forwardTo string
		// If we resume to the same bucket, forward to last known item.
		if b := tracker.getBucket(); b != "" {
			if b == bucket {
				forwardTo = tracker.getObject()
			} else {
				// Reset to where last bucket ended if resuming.
				tracker.resume()
			}
		}
		tracker.setObject("")
		tracker.setBucket(bucket)
		// Heal current bucket again in case if it is failed
		// in the beginning of erasure set healing
		if _, err := er.HealBucket(ctx, bucket, madmin.HealOpts{
			ScanMode: scanMode,
		}); err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		if serverDebugLog {
			console.Debugf(color.Green("healDrive:")+" healing bucket %s content on %s erasure set\n",
				bucket, humanize.Ordinal(er.setIndex+1))
		}

		disks, _ := er.getOnlineDisksWithHealing()
		if len(disks) == 0 {
			logger.LogIf(ctx, fmt.Errorf("no online disks found to heal the bucket `%s`", bucket))
			continue
		}

		// Limit listing to 3 drives.
		if len(disks) > 3 {
			disks = disks[:3]
		}

		type healEntryResult struct {
			bytes     uint64
			success   bool
			entryDone bool
			name      string
		}
		healEntryDone := func(name string) healEntryResult {
			return healEntryResult{
				entryDone: true,
				name:      name,
			}
		}
		healEntrySuccess := func(sz uint64) healEntryResult {
			return healEntryResult{
				bytes:   sz,
				success: true,
			}
		}
		healEntryFailure := func(sz uint64) healEntryResult {
			return healEntryResult{
				bytes: sz,
			}
		}

		// Collect updates to tracker from concurrent healEntry calls
		results := make(chan healEntryResult, 1000)
		go func() {
			for res := range results {
				if res.entryDone {
					tracker.setObject(res.name)
					if time.Since(tracker.getLastUpdate()) > time.Minute {
						logger.LogIf(ctx, tracker.update(ctx))
					}
					continue
				}

				tracker.updateProgress(res.success, res.bytes)
			}
		}()

		send := func(result healEntryResult) bool {
			select {
			case <-ctx.Done():
				return false
			case results <- result:
				return true
			}
		}

		// Note: updates from healEntry to tracker must be sent on results channel.
		healEntry := func(bucket string, entry metaCacheEntry) {
			if entry.name == "" && len(entry.metadata) == 0 {
				// ignore entries that don't have metadata.
				return
			}
			if entry.isDir() {
				// ignore healing entry.name's with `/` suffix.
				return
			}

			// We might land at .metacache, .trash, .multipart
			// no need to heal them skip, only when bucket
			// is '.minio.sys'
			if bucket == minioMetaBucket {
				if wildcard.Match("buckets/*/.metacache/*", entry.name) {
					return
				}
				if wildcard.Match("tmp/.trash/*", entry.name) {
					return
				}
				if wildcard.Match("multipart/*", entry.name) {
					return
				}
			}

			var result healEntryResult
			fivs, err := entry.fileInfoVersions(bucket)
			if err != nil {
				err := bgSeq.queueHealTask(healSource{
					bucket:    bucket,
					object:    entry.name,
					versionID: "",
				}, madmin.HealItemObject)
				if err != nil {
					if isErrObjectNotFound(err) {
						// queueing happens across namespace, ignore
						// objects that are not found.
						return
					}
					result = healEntryFailure(0)
					logger.LogIf(ctx, fmt.Errorf("unable to heal object %s/%s: %w", bucket, entry.name, err))
				} else {
					result = healEntrySuccess(0)
				}

				send(result)
				return
			}

			var versionNotFound int
			for _, version := range fivs.Versions {
				// Ignore a version with a modtime newer than healing start
				if version.ModTime.After(tracker.Started) {
					continue
				}
				if err := bgSeq.queueHealTask(healSource{
					bucket:    bucket,
					object:    version.Name,
					versionID: version.VersionID,
				}, madmin.HealItemObject); err != nil {
					if isErrObjectNotFound(err) {
						// queueing happens across namespace, ignore
						// objects that are not found.
						versionNotFound++
						continue
					}
					// If not deleted, assume they failed.
					result = healEntryFailure(uint64(version.Size))
					if version.VersionID != "" {
						if !isErrVersionNotFound(err) {
							logger.LogIf(ctx, fmt.Errorf("unable to heal object %s/%s-v(%s): %w", bucket, version.Name, version.VersionID, err))
						}
					} else {
						logger.LogIf(ctx, fmt.Errorf("unable to heal object %s/%s: %w", bucket, version.Name, err))
					}
				} else {
					result = healEntrySuccess(uint64(version.Size))
				}
				bgSeq.logHeal(madmin.HealItemObject)

				if !send(result) {
					return
				}
			}
			// All versions resulted in 'ObjectNotFound'
			if versionNotFound == len(fivs.Versions) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case results <- healEntryDone(entry.name):
			}

			// Wait and proceed if there are active requests
			waitForLowHTTPReq()
		}

		actualBucket, prefix := path2BucketObject(bucket)

		// How to resolve partial results.
		resolver := metadataResolutionParams{
			dirQuorum: 1,
			objQuorum: 1,
			bucket:    actualBucket,
		}

		err := listPathRaw(ctx, listPathRawOptions{
			disks:          disks,
			bucket:         actualBucket,
			path:           prefix,
			recursive:      true,
			forwardTo:      forwardTo,
			minDisks:       1,
			reportNotFound: false,
			agreed: func(entry metaCacheEntry) {
				healEntry(actualBucket, entry)
			},
			partial: func(entries metaCacheEntries, _ []error) {
				entry, ok := entries.resolve(&resolver)
				if !ok {
					// check if we can get one entry atleast
					// proceed to heal nonetheless.
					entry, _ = entries.firstFound()
				}
				healEntry(actualBucket, *entry)
			},
			finished: nil,
		})
		close(results)
		if err != nil {
			// Set this such that when we return this function
			// we let the caller retry this disk again for the
			// buckets it failed to list.
			retErr = err
			logger.LogIf(ctx, err)
			continue
		}

		select {
		// If context is canceled don't mark as done...
		case <-ctx.Done():
			return ctx.Err()
		default:
			tracker.bucketDone(bucket)
			logger.LogIf(ctx, tracker.update(ctx))
		}
	}

	tracker.setObject("")
	tracker.setBucket("")

	return retErr
}

func healBucket(bucket string, scan madmin.HealScanMode) error {
	// Get background heal sequence to send elements to heal
	globalHealStateLK.Lock()
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	globalHealStateLK.Unlock()
	if ok {
		return bgSeq.queueHealTask(healSource{bucket: bucket}, madmin.HealItemBucket)
	}
	return nil
}

// healObject sends the given object/version to the background healing workers
func healObject(bucket, object, versionID string, scan madmin.HealScanMode) error {
	// Get background heal sequence to send elements to heal
	globalHealStateLK.Lock()
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	globalHealStateLK.Unlock()
	if ok {
		return bgSeq.queueHealTask(healSource{
			bucket:    bucket,
			object:    object,
			versionID: versionID,
			noWait:    true, // do not block callers.
			opts: &madmin.HealOpts{
				Remove:   healDeleteDangling, // if found dangling purge it.
				ScanMode: scan,
			},
		}, madmin.HealItemObject)
	}
	return nil
}
