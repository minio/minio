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
	"sync"
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
	ctx := logger.SetReqInfo(GlobalContext, reqInfo)

	hs := madmin.HealOpts{
		// Remove objects that do not have read-quorum
		Remove:   true,
		ScanMode: madmin.HealNormalScan,
	}

	return &healSequence{
		sourceCh:    make(chan healSource),
		startTime:   UTCNow(),
		clientToken: bgHealingUUID,
		settings:    hs,
		currentStatus: healSequenceStatus{
			Summary:      healNotStartedStatus,
			HealSettings: hs,
			NumDisks:     numDisks,
			updateLock:   &sync.RWMutex{},
		},
		traverseAndHealDoneCh: make(chan error),
		stopSignalCh:          make(chan struct{}),
		ctx:                   ctx,
		reportProgress:        false,
		scannedItemsMap:       make(map[madmin.HealItemType]int64),
		healedItemsMap:        make(map[madmin.HealItemType]int64),
		healFailedItemsMap:    make(map[string]int64),
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
func healErasureSet(ctx context.Context, setIndex int, xlObj *xlObjects) error {
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
		time.Sleep(time.Second)
	}

	// Heal all buckets with all objects
	for _, bucket := range buckets {
		// Heal current bucket
		bgSeq.sourceCh <- healSource{
			path: bucket.Name,
		}

		// List all objects in the current bucket and heal them
		listDir := listDirFactory(ctx, xlObj.getLoadBalancedDisks()...)
		walkResultCh := startTreeWalk(ctx, bucket.Name, "", "", true, listDir, nil)
		for walkEntry := range walkResultCh {
			bgSeq.sourceCh <- healSource{
				path: pathJoin(bucket.Name, walkEntry.entry),
			}
		}
	}

	return nil
}

// deepHealObject heals given object path in deep to fix bitrot.
func deepHealObject(objectPath string) {
	// Get background heal sequence to send elements to heal
	bgSeq, _ := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)

	bgSeq.sourceCh <- healSource{
		path: objectPath,
		opts: &madmin.HealOpts{ScanMode: madmin.HealDeepScan},
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
func execLeaderTasks(ctx context.Context, z *xlZones) {
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
		time.Sleep(time.Second)
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
					if err := healErasureSet(ctx, i, set); err != nil {
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
	zones, ok := objAPI.(*xlZones)
	if !ok {
		return
	}

	execLeaderTasks(ctx, zones)
}

func initGlobalHeal(ctx context.Context, objAPI ObjectLayer) {
	go startGlobalHeal(ctx, objAPI)
}
