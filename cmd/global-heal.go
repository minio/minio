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

func getLocalBackgroundHealStatus() (madmin.BgHealState, bool) {
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

	for _, ep := range globalBackgroundHealState.getHealLocalDisks() {
		if _, ok := healDisksMap[ep.String()]; !ok {
			healDisksMap[ep.String()] = struct{}{}
		}
	}

	var healDisks []string
	for disk := range healDisksMap {
		healDisks = append(healDisks, disk)
	}

	return madmin.BgHealState{
		ScannedItemsCount: bgSeq.getScannedItemsCount(),
		LastHealActivity:  bgSeq.lastHealActivity,
		HealDisks:         healDisks,
		NextHealRound:     UTCNow(),
	}, true
}

// healErasureSet lists and heals all objects in a specific erasure set
func healErasureSet(ctx context.Context, setIndex int, buckets []BucketInfo, disks []StorageAPI) error {
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

	buckets = append(buckets, BucketInfo{
		Name: pathJoin(minioMetaBucket, minioConfigPrefix),
	}, BucketInfo{
		Name: pathJoin(minioMetaBucket, bucketConfigPrefix),
	}) // add metadata .minio.sys/ bucket prefixes to heal

	// Heal all buckets with all objects
	for _, bucket := range buckets {
		// Heal current bucket
		bgSeq.sourceCh <- healSource{
			bucket: bucket.Name,
		}

		var entryChs []FileInfoVersionsCh
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, disk := range disks {
			disk := disk
			wg.Add(1)
			go func() {
				defer wg.Done()
				entryCh, err := disk.WalkVersions(ctx, bucket.Name, "", "", true, ctx.Done())
				if err != nil {
					// Disk walk returned error, ignore it.
					return
				}
				mu.Lock()
				entryChs = append(entryChs, FileInfoVersionsCh{
					Ch: entryCh,
				})
				mu.Unlock()
			}()
		}
		wg.Wait()

		entriesValid := make([]bool, len(entryChs))
		entries := make([]FileInfoVersions, len(entryChs))

		for {
			entry, _, ok := lexicallySortedEntryVersions(entryChs, entries, entriesValid)
			if !ok {
				break
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
	bgSeq, ok := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
	if ok {
		bgSeq.sourceCh <- healSource{
			bucket:    bucket,
			object:    object,
			versionID: versionID,
			opts:      &madmin.HealOpts{ScanMode: madmin.HealDeepScan},
		}
	}
}
