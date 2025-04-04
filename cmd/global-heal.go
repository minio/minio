// Copyright (c) 2015-2024 MinIO, Inc.
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
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/minio/pkg/v3/workers"
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
		healFailedItemsMap: make(map[madmin.HealItemType]int64),
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

	si := o.LocalStorageInfo(ctx, true)

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

type healEntryResult struct {
	bytes     uint64
	success   bool
	skipped   bool
	entryDone bool
	name      string
}

// healErasureSet lists and heals all objects in a specific erasure set
func (er *erasureObjects) healErasureSet(ctx context.Context, buckets []string, tracker *healingTracker) error {
	bgSeq, found := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !found {
		return errors.New("no local healing sequence initialized, unable to heal the drive")
	}

	scanMode := madmin.HealNormalScan

	// Make sure to copy since `buckets slice`
	// is modified in place by tracker.
	healBuckets := make([]string, len(buckets))
	copy(healBuckets, buckets)

	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	started := tracker.Started
	if started.IsZero() || started.Equal(timeSentinel) {
		healingLogIf(ctx, fmt.Errorf("unexpected tracker healing start time found: %v", started))
		started = time.Time{}
	}

	// Final tracer update before quitting
	defer func() {
		tracker.setObject("")
		tracker.setBucket("")
		healingLogIf(ctx, tracker.update(ctx))
	}()

	for _, bucket := range healBuckets {
		if err := bgSeq.healBucket(objAPI, bucket, true); err != nil {
			// Log bucket healing error if any, we shall retry again.
			healingLogIf(ctx, err)
		}
	}

	info, err := tracker.disk.DiskInfo(ctx, DiskInfoOptions{})
	if err != nil {
		return fmt.Errorf("unable to get disk information before healing it: %w", err)
	}

	var numHealers uint64

	if numCores := uint64(runtime.GOMAXPROCS(0)); info.NRRequests > numCores {
		numHealers = numCores / 4
	} else {
		numHealers = info.NRRequests / 4
	}
	if numHealers < 4 {
		numHealers = 4
	}
	// allow overriding this value as well..
	if v := globalHealConfig.GetWorkers(); v > 0 {
		numHealers = uint64(v)
	}

	healingLogEvent(ctx, "Healing drive '%s' - use %d parallel workers.", tracker.disk.String(), numHealers)

	jt, _ := workers.New(int(numHealers))

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

	healEntrySkipped := func(sz uint64) healEntryResult {
		return healEntryResult{
			bytes:   sz,
			skipped: true,
		}
	}

	// Collect updates to tracker from concurrent healEntry calls
	results := make(chan healEntryResult, 1000)
	quitting := make(chan struct{})
	defer func() {
		close(results)
		<-quitting
	}()

	go func() {
		for res := range results {
			if res.entryDone {
				tracker.setObject(res.name)
				if time.Since(tracker.getLastUpdate()) > time.Minute {
					healingLogIf(ctx, tracker.update(ctx))
				}
				continue
			}

			tracker.updateProgress(res.success, res.skipped, res.bytes)
		}

		healingLogIf(ctx, tracker.update(ctx))
		close(quitting)
	}()

	var retErr error

	// Heal all buckets with all objects
	for _, bucket := range healBuckets {
		if tracker.isHealed(bucket) {
			continue
		}

		var forwardTo string
		// If we resume to the same bucket, forward to last known item.
		b := tracker.getBucket()
		if b == bucket {
			forwardTo = tracker.getObject()
		}
		if b != "" {
			// Reset to where last bucket ended if resuming.
			tracker.resume()
		}
		tracker.setObject("")
		tracker.setBucket(bucket)
		// Heal current bucket again in case if it is failed
		// in the beginning of erasure set healing
		if err := bgSeq.healBucket(objAPI, bucket, true); err != nil {
			// Set this such that when we return this function
			// we let the caller retry this disk again for the
			// buckets that failed healing.
			retErr = err
			healingLogIf(ctx, err)
			continue
		}

		var (
			vc   *versioning.Versioning
			lc   *lifecycle.Lifecycle
			lr   objectlock.Retention
			rcfg *replication.Config
		)

		if !isMinioMetaBucketName(bucket) {
			vc, err = globalBucketVersioningSys.Get(bucket)
			if err != nil {
				retErr = err
				healingLogIf(ctx, err)
				continue
			}
			// Check if the current bucket has a configured lifecycle policy
			lc, err = globalLifecycleSys.Get(bucket)
			if err != nil && !errors.Is(err, BucketLifecycleNotFound{Bucket: bucket}) {
				retErr = err
				healingLogIf(ctx, err)
				continue
			}
			// Check if bucket is object locked.
			lr, err = globalBucketObjectLockSys.Get(bucket)
			if err != nil {
				retErr = err
				healingLogIf(ctx, err)
				continue
			}
			rcfg, err = getReplicationConfig(ctx, bucket)
			if err != nil {
				retErr = err
				healingLogIf(ctx, err)
				continue
			}
		}

		if serverDebugLog {
			console.Debugf(color.Green("healDrive:")+" healing bucket %s content on %s erasure set\n",
				bucket, humanize.Ordinal(er.setIndex+1))
		}

		disks, _, healing := er.getOnlineDisksWithHealingAndInfo(true)
		if len(disks) == healing {
			// All drives in this erasure set were reformatted for some reasons, abort healing and mark it as successful
			healingLogIf(ctx, errors.New("all drives are in healing state, aborting.."))
			return nil
		}

		disks = disks[:len(disks)-healing] // healing drives are always at the end of the list

		if len(disks) < er.setDriveCount/2 {
			return fmt.Errorf("not enough drives (found=%d, healing=%d, total=%d) are available to heal `%s`", len(disks), healing, er.setDriveCount, tracker.disk.String())
		}

		rand.Shuffle(len(disks), func(i, j int) {
			disks[i], disks[j] = disks[j], disks[i]
		})

		filterLifecycle := func(bucket, object string, fi FileInfo) bool {
			if lc == nil {
				return false
			}
			versioned := vc != nil && vc.Versioned(object)
			objInfo := fi.ToObjectInfo(bucket, object, versioned)

			evt := evalActionFromLifecycle(ctx, *lc, lr, rcfg, objInfo)
			switch {
			case evt.Action.DeleteRestored(): // if restored copy has expired,delete it synchronously
				applyExpiryOnTransitionedObject(ctx, newObjectLayerFn(), objInfo, evt, lcEventSrc_Heal)
				return false
			case evt.Action.Delete():
				globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_Heal)
				return true
			default:
				return false
			}
		}

		send := func(result healEntryResult) bool {
			select {
			case <-ctx.Done():
				if !contextCanceled(ctx) {
					healingLogIf(ctx, ctx.Err())
				}
				return false
			case results <- result:
				bgSeq.countScanned(madmin.HealItemObject)
				return true
			}
		}

		// Note: updates from healEntry to tracker must be sent on results channel.
		healEntry := func(bucket string, entry metaCacheEntry) {
			defer jt.Give()

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

			// erasureObjects layer needs object names to be encoded
			encodedEntryName := encodeDirObject(entry.name)

			var result healEntryResult
			fivs, err := entry.fileInfoVersions(bucket)
			if err != nil {
				res, err := er.HealObject(ctx, bucket, encodedEntryName, "",
					madmin.HealOpts{
						ScanMode: scanMode,
						Remove:   healDeleteDangling,
					})
				if err != nil {
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						// queueing happens across namespace, ignore
						// objects that are not found.
						return
					}
					result = healEntryFailure(0)
					bgSeq.countFailed(madmin.HealItemObject)
					healingLogIf(ctx, fmt.Errorf("unable to heal object %s/%s: %w", bucket, entry.name, err))
				} else {
					bgSeq.countHealed(madmin.HealItemObject)
					result = healEntrySuccess(uint64(res.ObjectSize))
				}

				send(result)
				return
			}

			var versionNotFound int
			for _, version := range fivs.Versions {
				// Ignore healing a version if:
				// - It is uploaded after the drive healing is started
				// - An object that is already expired by ILM rule.
				if !started.IsZero() && version.ModTime.After(started) || filterLifecycle(bucket, version.Name, version) {
					versionNotFound++
					if !send(healEntrySkipped(uint64(version.Size))) {
						return
					}
					continue
				}

				res, err := er.HealObject(ctx, bucket, encodedEntryName,
					version.VersionID, madmin.HealOpts{
						ScanMode: scanMode,
						Remove:   healDeleteDangling,
					})
				if err != nil {
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						// queueing happens across namespace, ignore
						// objects that are not found.
						versionNotFound++
						continue
					}
				} else {
					// Look for the healing results
					if res.After.Drives[tracker.DiskIndex].State != madmin.DriveStateOk {
						err = fmt.Errorf("unexpected after heal state: %s", res.After.Drives[tracker.DiskIndex].State)
					}
				}

				if err == nil {
					bgSeq.countHealed(madmin.HealItemObject)
					result = healEntrySuccess(uint64(version.Size))
				} else {
					bgSeq.countFailed(madmin.HealItemObject)
					result = healEntryFailure(uint64(version.Size))
					if version.VersionID != "" {
						healingLogIf(ctx, fmt.Errorf("unable to heal object %s/%s (version-id=%s): %w",
							bucket, version.Name, version.VersionID, err))
					} else {
						healingLogIf(ctx, fmt.Errorf("unable to heal object %s/%s: %w",
							bucket, version.Name, err))
					}
				}

				if !send(result) {
					return
				}
			}

			// All versions resulted in 'ObjectNotFound/VersionNotFound'
			if versionNotFound == len(fivs.Versions) {
				return
			}

			send(healEntryDone(entry.name))

			// Wait and proceed if there are active requests
			waitForLowHTTPReq()
		}

		// How to resolve partial results.
		resolver := metadataResolutionParams{
			dirQuorum: 1,
			objQuorum: 1,
			bucket:    bucket,
		}

		err = listPathRaw(ctx, listPathRawOptions{
			disks:          disks,
			bucket:         bucket,
			recursive:      true,
			forwardTo:      forwardTo,
			minDisks:       1,
			reportNotFound: false,
			agreed: func(entry metaCacheEntry) {
				jt.Take()
				go healEntry(bucket, entry)
			},
			partial: func(entries metaCacheEntries, _ []error) {
				entry, ok := entries.resolve(&resolver)
				if !ok {
					// check if we can get one entry at least
					// proceed to heal nonetheless.
					entry, _ = entries.firstFound()
				}
				jt.Take()
				go healEntry(bucket, *entry)
			},
			finished: func(errs []error) {
				success := countErrs(errs, nil)
				if success < len(disks)/2+1 {
					retErr = fmt.Errorf("one or more errors reported during listing: %v", errors.Join(errs...))
				}
			},
		})
		jt.Wait() // synchronize all the concurrent heal jobs
		if err != nil {
			// Set this such that when we return this function
			// we let the caller retry this disk again for the
			// buckets it failed to list.
			retErr = err
		}

		if retErr != nil {
			healingLogIf(ctx, fmt.Errorf("listing failed with: %v on bucket: %v", retErr, bucket))
			continue
		}

		select {
		// If context is canceled don't mark as done...
		case <-ctx.Done():
			return ctx.Err()
		default:
			tracker.bucketDone(bucket)
			healingLogIf(ctx, tracker.update(ctx))
		}
	}
	if retErr != nil {
		return retErr
	}

	// Last sanity check
	if len(tracker.QueuedBuckets) > 0 {
		return fmt.Errorf("not all buckets were healed: %v", tracker.QueuedBuckets)
	}

	return nil
}

func healBucket(bucket string, scan madmin.HealScanMode) error {
	// Get background heal sequence to send elements to heal
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if ok {
		return bgSeq.queueHealTask(healSource{bucket: bucket}, madmin.HealItemBucket)
	}
	return nil
}

// healObject sends the given object/version to the background healing workers
func healObject(bucket, object, versionID string, scan madmin.HealScanMode) error {
	// Get background heal sequence to send elements to heal
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if ok {
		return bgSeq.healObject(bucket, object, versionID, scan)
	}
	return nil
}
