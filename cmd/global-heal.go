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
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
)

const (
	bgHealingUUID = "0000-0000-0000-0000"
	leaderTick    = time.Hour
	healTick      = time.Hour
	healInterval  = 30 * 24 * time.Hour
)

var leaderLockTimeout = newDynamicTimeout(time.Minute, time.Minute)

// NewBgHealSequence creates a background healing sequence
// operation which crawls all objects and heal them.
func newBgHealSequence(numDisks int) *healSequence {

	reqInfo := &logger.ReqInfo{API: "BackgroundHeal"}
	ctx := logger.SetReqInfo(context.Background(), reqInfo)

	hs := madmin.HealOpts{
		// Remove objects that do not have read-quorum
		Remove:   true,
		ScanMode: madmin.HealNormalScan,
	}

	return &healSequence{
		sourceCh:    make(chan string),
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
	}
}

func getLocalBackgroundHealStatus() madmin.BgHealState {
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if !ok {
		return madmin.BgHealState{}
	}

	return madmin.BgHealState{
		ScannedItemsCount: bgSeq.scannedItemsCount,
		LastHealActivity:  bgSeq.lastHealActivity,
	}
}

// healErasureSet lists and heals all objects in a specific erasure set
func healErasureSet(ctx context.Context, setIndex int, xlObj *xlObjects) error {
	// Hold a lock for healing the erasure set
	zeroDuration := time.Millisecond
	zeroDynamicTimeout := newDynamicTimeout(zeroDuration, zeroDuration)
	erasureSetHealLock := globalNSMutex.NewNSLock(ctx, "system", fmt.Sprintf("erasure-set-heal-%d", setIndex))
	if err := erasureSetHealLock.GetLock(zeroDynamicTimeout); err != nil {
		return err
	}
	defer erasureSetHealLock.Unlock()

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
		bgSeq.sourceCh <- bucket.Name

		// List all objects in the current bucket and heal them
		listDir := listDirFactory(ctx, xlObj.getLoadBalancedDisks()...)
		walkResultCh := startTreeWalk(ctx, bucket.Name, "", "", true, listDir, nil)
		for walkEntry := range walkResultCh {
			bgSeq.sourceCh <- pathJoin(bucket.Name, walkEntry.entry)
		}
	}

	return nil
}

// Healing leader will take the charge of healing all erasure sets
func execLeaderTasks(sets *xlSets) {
	ctx := context.Background()

	// Hold a lock so only one server performs auto-healing
	leaderLock := globalNSMutex.NewNSLock(ctx, minioMetaBucket, "leader")
	for {
		err := leaderLock.GetLock(leaderLockTimeout)
		if err == nil {
			break
		}
		time.Sleep(leaderTick)
	}

	lastScanTime := time.Now() // So that we don't heal immediately, but after one month.
	for {
		if time.Since(lastScanTime) < healInterval {
			time.Sleep(healTick)
			continue
		}
		// Heal set by set
		for i, set := range sets.sets {
			err := healErasureSet(ctx, i, set)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
		}
		lastScanTime = time.Now()
	}
}

func startGlobalHeal() {
	var objAPI ObjectLayer
	for {
		objAPI = newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	sets, ok := objAPI.(*xlSets)
	if !ok {
		return
	}

	execLeaderTasks(sets)
}

func initGlobalHeal() {
	go startGlobalHeal()
}
