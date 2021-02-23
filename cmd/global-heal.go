/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/madmin"
)

const (
	bgHealingUUID = "0000-0000-0000-0000"
)

// NewBgHealSequence creates a background healing sequence
// operation which crawls all objects and heal them.
func newBgHealSequence() *healSequence {
	reqInfo := &logger.ReqInfo{API: "BackgroundHeal"}
	ctx, cancelCtx := context.WithCancel(logger.SetReqInfo(GlobalContext, reqInfo))

	hs := madmin.HealOpts{
		// Remove objects that do not have read-quorum
		Remove:   true,
		ScanMode: madmin.HealNormalScan,
	}

	return &healSequence{
		sourceCh:    make(chan healSource),
		respCh:      make(chan healResult),
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

// getBackgroundHealStatus will return the
func getBackgroundHealStatus(ctx context.Context, o ObjectLayer) (madmin.BgHealState, bool) {
	if globalBackgroundHealState == nil {
		return madmin.BgHealState{}, false
	}

	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !ok {
		return madmin.BgHealState{}, false
	}

	var healDisksMap = map[string]struct{}{}
	for _, ep := range getLocalDisksToHeal() {
		healDisksMap[ep.String()] = struct{}{}
	}
	status := madmin.BgHealState{
		ScannedItemsCount: bgSeq.getScannedItemsCount(),
	}

	if o == nil {
		healing := globalBackgroundHealState.getLocalHealingDisks()
		for _, disk := range healing {
			status.HealDisks = append(status.HealDisks, disk.Endpoint)
		}

		return status, true
	}

	// ignores any errors here.
	si, _ := o.StorageInfo(ctx)

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
func (er *erasureObjects) healErasureSet(ctx context.Context, buckets []BucketInfo, tracker *healingTracker) error {
	bgSeq := mustGetHealSequence(ctx)
	buckets = append(buckets, BucketInfo{
		Name: pathJoin(minioMetaBucket, minioConfigPrefix),
	})

	// Try to pro-actively heal backend-encrypted file.
	if _, err := er.HealObject(ctx, minioMetaBucket, backendEncryptedFile, "", madmin.HealOpts{}); err != nil {
		if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			logger.LogIf(ctx, err)
		}
	}

	// Heal all buckets with all objects
	for _, bucket := range buckets {
		if tracker.isHealed(bucket.Name) {
			continue
		}
		var forwardTo string
		// If we resume to the same bucket, forward to last known item.
		if tracker.Bucket != "" {
			if tracker.Bucket == bucket.Name {
				forwardTo = tracker.Bucket
			} else {
				// Reset to where last bucket ended if resuming.
				tracker.resume()
			}
		}
		tracker.Object = ""
		tracker.Bucket = bucket.Name
		// Heal current bucket
		if _, err := er.HealBucket(ctx, bucket.Name, madmin.HealOpts{}); err != nil {
			if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
				logger.LogIf(ctx, err)
			}
		}

		if serverDebugLog {
			console.Debugf(color.Green("healDisk:")+" healing bucket %s content on erasure set %d\n", bucket.Name, tracker.SetIndex+1)
		}

		disks, _ := er.getOnlineDisksWithHealing()
		if len(disks) == 0 {
			return errors.New("healErasureSet: No non-healing disks found")
		}
		// Limit listing to 3 drives.
		if len(disks) > 3 {
			disks = disks[:3]
		}
		healEntry := func(entry metaCacheEntry) {
			if entry.isDir() {
				return
			}
			fivs, err := entry.fileInfoVersions(bucket.Name)
			if err != nil {
				logger.LogIf(ctx, err)
				return
			}
			waitForLowHTTPReq(globalHealConfig.IOCount, globalHealConfig.Sleep)
			for _, version := range fivs.Versions {
				if _, err := er.HealObject(ctx, bucket.Name, version.Name, version.VersionID, madmin.HealOpts{ScanMode: madmin.HealNormalScan, Remove: healDeleteDangling}); err != nil {
					if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
						// If not deleted, assume they failed.
						tracker.ObjectsFailed++
						tracker.BytesFailed += uint64(version.Size)
						logger.LogIf(ctx, err)
					}
				} else {
					tracker.ObjectsHealed++
					tracker.BytesDone += uint64(version.Size)
				}
				bgSeq.logHeal(madmin.HealItemObject)
			}
			tracker.Object = entry.name
			if time.Since(tracker.LastUpdate) > time.Minute {
				logger.LogIf(ctx, tracker.update(ctx))
			}
		}
		err := listPathRaw(ctx, listPathRawOptions{
			disks:          disks,
			bucket:         bucket.Name,
			recursive:      true,
			forwardTo:      forwardTo,
			minDisks:       1,
			reportNotFound: false,
			agreed:         healEntry,
			partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
				entry, _ := entries.firstFound()
				if entry != nil && !entry.isDir() {
					healEntry(*entry)
				}
			},
			finished: nil,
		})
		select {
		// If context is canceled don't mark as done...
		case <-ctx.Done():
			return ctx.Err()
		default:
			logger.LogIf(ctx, err)
			tracker.bucketDone(bucket.Name)
			logger.LogIf(ctx, tracker.update(ctx))
		}
	}
	tracker.Object = ""
	tracker.Bucket = ""

	return nil
}

// healObject heals given object path in deep to fix bitrot.
func healObject(bucket, object, versionID string, scan madmin.HealScanMode) {
	// Get background heal sequence to send elements to heal
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if ok {
		bgSeq.sourceCh <- healSource{
			bucket:    bucket,
			object:    object,
			versionID: versionID,
			opts: &madmin.HealOpts{
				Remove:   true, // if found dangling purge it.
				ScanMode: scan,
			},
		}
	}
}
