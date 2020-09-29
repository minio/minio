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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
)

const (
	defaultMonitorNewDiskInterval = time.Second * 10
	healingTrackerFilename        = ".healing.bin"
)

//go:generate msgp -file $GOFILE -unexported
type healingTracker struct {
	ID string

	// future add more tracking capabilities
}

func initAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	z, ok := objAPI.(*erasureZones)
	if !ok {
		return
	}

	initBackgroundHealing(ctx, objAPI) // start quick background healing

	var bgSeq *healSequence
	var found bool

	for {
		bgSeq, found = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if found {
			break
		}
		time.Sleep(time.Second)
	}

	globalBackgroundHealState.pushHealLocalDisks(getLocalDisksToHeal()...)

	if drivesToHeal := globalBackgroundHealState.healDriveCount(); drivesToHeal > 0 {
		logger.Info(fmt.Sprintf("Found drives to heal %d, waiting until %s to heal the content...",
			drivesToHeal, defaultMonitorNewDiskInterval))

		// Heal any disk format and metadata early, if possible.
		if err := bgSeq.healDiskMeta(); err != nil {
			if newObjectLayerFn() != nil {
				// log only in situations, when object layer
				// has fully initialized.
				logger.LogIf(bgSeq.ctx, err)
			}
		}
	}

	go monitorLocalDisksAndHeal(ctx, z, bgSeq)
}

func getLocalDisksToHeal() (disksToHeal Endpoints) {
	for _, ep := range globalEndpoints {
		for _, endpoint := range ep.Endpoints {
			if !endpoint.IsLocal {
				continue
			}
			// Try to connect to the current endpoint
			// and reformat if the current disk is not formatted
			disk, _, err := connectEndpoint(endpoint)
			if errors.Is(err, errUnformattedDisk) {
				disksToHeal = append(disksToHeal, endpoint)
			} else if err == nil && disk != nil && disk.Healing() {
				disksToHeal = append(disksToHeal, disk.Endpoint())
			}
		}
	}
	return disksToHeal

}

func initBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	// Run the background healer
	globalBackgroundHealRoutine = newHealRoutine()
	go globalBackgroundHealRoutine.run(ctx, objAPI)

	globalBackgroundHealState.LaunchNewHealSequence(newBgHealSequence())
}

// monitorLocalDisksAndHeal - ensures that detected new disks are healed
//  1. Only the concerned erasure set will be listed and healed
//  2. Only the node hosting the disk is responsible to perform the heal
func monitorLocalDisksAndHeal(ctx context.Context, z *erasureZones, bgSeq *healSequence) {
	// Perform automatic disk healing when a disk is replaced locally.
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(defaultMonitorNewDiskInterval):
			waitForLowHTTPReq(int32(globalEndpoints.NEndpoints()), time.Second)

			var erasureSetInZoneDisksToHeal []map[int][]StorageAPI

			healDisks := globalBackgroundHealState.getHealLocalDisks()
			if len(healDisks) > 0 {
				// Reformat disks
				bgSeq.sourceCh <- healSource{bucket: SlashSeparator}

				// Ensure that reformatting disks is finished
				bgSeq.sourceCh <- healSource{bucket: nopHeal}

				logger.Info(fmt.Sprintf("Found drives to heal %d, proceeding to heal content...",
					len(healDisks)))

				erasureSetInZoneDisksToHeal = make([]map[int][]StorageAPI, len(z.zones))
				for i := range z.zones {
					erasureSetInZoneDisksToHeal[i] = map[int][]StorageAPI{}
				}
			}

			// heal only if new disks found.
			for _, endpoint := range healDisks {
				disk, format, err := connectEndpoint(endpoint)
				if err != nil {
					printEndpointError(endpoint, err, true)
					continue
				}

				zoneIdx := globalEndpoints.GetLocalZoneIdx(disk.Endpoint())
				if zoneIdx < 0 {
					continue
				}

				// Calculate the set index where the current endpoint belongs
				setIndex, _, err := findDiskIndex(z.zones[zoneIdx].format, format)
				if err != nil {
					printEndpointError(endpoint, err, false)
					continue
				}

				erasureSetInZoneDisksToHeal[zoneIdx][setIndex] = append(erasureSetInZoneDisksToHeal[zoneIdx][setIndex], disk)
			}

			buckets, _ := z.ListBucketsHeal(ctx)
			for i, setMap := range erasureSetInZoneDisksToHeal {
				for setIndex, disks := range setMap {
					for _, disk := range disks {
						logger.Info("Healing disk '%s' on %s zone", disk, humanize.Ordinal(i+1))

						lbDisks := z.zones[i].sets[setIndex].getLoadBalancedNDisks(z.zones[i].listTolerancePerSet)
						if err := healErasureSet(ctx, setIndex, buckets, lbDisks); err != nil {
							logger.LogIf(ctx, err)
							continue
						}

						logger.Info("Healing disk '%s' on %s zone complete", disk, humanize.Ordinal(i+1))

						if err := disk.DeleteFile(ctx, pathJoin(minioMetaBucket, bucketMetaPrefix),
							healingTrackerFilename); err != nil {
							logger.LogIf(ctx, err)
							continue
						}

						// Only upon success pop the healed disk.
						globalBackgroundHealState.popHealLocalDisks(disk.Endpoint())
					}
				}
			}
		}
	}
}
