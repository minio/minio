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
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
)

const (
	bgHealingUUID = "0000-0000-0000-0000"
	leaderTick    = time.Hour
	healInterval  = 30 * 24 * time.Hour
)

var leaderLockTimeout = newDynamicTimeout(time.Minute, time.Minute)

// NewBgHealSequence creates a background healing sequence
// operation which crawls all objects and heal them.
func newBgHealSequence(numDisks int) *healSequence {

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
		settings:    hs,
		currentStatus: healSequenceStatus{
			Summary:      healNotStartedStatus,
			HealSettings: hs,
			NumDisks:     numDisks,
		},
		cancelCtx:          cancelCtx,
		ctx:                ctx,
		reportProgress:     false,
		scannedItemsMap:    make(map[madmin.HealItemType]int64),
		healedItemsMap:     make(map[madmin.HealItemType]int64),
		healFailedItemsMap: make(map[string]int64),
	}
}

func getLocalBackgroundHealStatus() madmin.BgHealState {
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !ok {
		return madmin.BgHealState{}
	}

	return madmin.BgHealState{
		ScannedItemsCount: bgSeq.getScannedItemsCount(),
		LastHealActivity:  bgSeq.lastHealActivity,
		NextHealRound:     UTCNow().Add(durationToNextHealRound(bgSeq.lastHealActivity)),
	}
}

// healErasureSet lists and heals all objects in a specific erasure set
func healErasureSet(ctx context.Context, setIndex int, xlObj *erasureObjects, drivesPerSet int) error {
	buckets, err := xlObj.ListBuckets(ctx)
	if err != nil {
		return err
	}

	// Get background heal sequence to send elements to heal
	var bgSeq *healSequence
	var ok bool
	for {
		bgSeq, ok = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			continue
		}
	}

	// Heal all buckets with all objects
	for _, bucket := range buckets {
		// Heal current bucket
		bgSeq.sourceCh <- healSource{
			bucket: bucket.Name,
		}

		var entryChs []FileInfoVersionsCh
		for _, disk := range xlObj.getLoadBalancedDisks() {
			if disk == nil {
				// Disk can be offline
				continue
			}

			entryCh, err := disk.WalkVersions(bucket.Name, "", "", true, ctx.Done())
			if err != nil {
				// Disk walk returned error, ignore it.
				continue
			}

			entryChs = append(entryChs, FileInfoVersionsCh{
				Ch: entryCh,
			})
		}

		entriesValid := make([]bool, len(entryChs))
		entries := make([]FileInfoVersions, len(entryChs))

		for {
			entry, quorumCount, ok := lexicallySortedEntryVersions(entryChs, entries, entriesValid)
			if !ok {
				break
			}

			if quorumCount == drivesPerSet {
				// Skip good entries.
				continue
			}

			for _, version := range entry.Versions {
				bgSeq.sourceCh <- healSource{
					bucket:    bucket.Name,
					object:    version.Name,
					versionID: version.VersionID,
				}
			}
		}
	}

	return nil
}

// deepHealObject heals given object path in deep to fix bitrot.
func deepHealObject(bucket, object, versionID string) {
	// Get background heal sequence to send elements to heal
	bgSeq, _ := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)

	bgSeq.sourceCh <- healSource{
		bucket:    bucket,
		object:    object,
		versionID: versionID,
		opts:      &madmin.HealOpts{ScanMode: madmin.HealDeepScan},
	}
}

// Returns the duration to the next background healing round
func durationToNextHealRound(lastHeal time.Time) time.Duration {
	if lastHeal.IsZero() {
		lastHeal = globalBootTime
	}

	d := lastHeal.Add(healInterval).Sub(UTCNow())
	if d < 0 {
		return time.Second
	}
	return d
}

// Healing leader will take the charge of healing all erasure sets
func execLeaderTasks(ctx context.Context, z *erasureZones) {
	// So that we don't heal immediately, but after one month.
	lastScanTime := UTCNow()
	// Get background heal sequence to send elements to heal
	var bgSeq *healSequence
	var ok bool
	for {
		bgSeq, ok = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			continue
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(durationToNextHealRound(lastScanTime)).C:
			bgSeq.resetHealStatusCounters()
			for _, zone := range z.zones {
				// Heal set by set
				for i, set := range zone.sets {
					if err := healErasureSet(ctx, i, set, zone.drivesPerSet); err != nil {
						logger.LogIf(ctx, err)
						continue
					}
				}
			}
			lastScanTime = UTCNow()
		}
	}
}

func startGlobalHeal(ctx context.Context, objAPI ObjectLayer) {
	zones, ok := objAPI.(*erasureZones)
	if !ok {
		return
	}

	execLeaderTasks(ctx, zones)
}

func initGlobalHeal(ctx context.Context, objAPI ObjectLayer) {
	go startGlobalHeal(ctx, objAPI)
}
