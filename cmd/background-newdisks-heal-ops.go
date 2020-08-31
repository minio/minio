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

const defaultMonitorNewDiskInterval = time.Minute * 3

func initAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	z, ok := objAPI.(*erasureZones)
	if !ok {
		return
	}

	initBackgroundHealing(ctx, objAPI) // start quick background healing

	localDisksInZoneHeal := getLocalDisksToHeal(objAPI)
	globalBackgroundHealState.updateHealLocalDisks(localDisksInZoneHeal)

	drivesToHeal := getDrivesToHealCount(localDisksInZoneHeal)
	if drivesToHeal != 0 {
		logger.Info(fmt.Sprintf("Found drives to heal %d, waiting until %s to heal the content...",
			drivesToHeal, defaultMonitorNewDiskInterval))
	}

	var bgSeq *healSequence
	var found bool

	for {
		bgSeq, found = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if found {
			break
		}
		time.Sleep(time.Second)
	}

	if drivesToHeal != 0 {
		// Heal any disk format and metadata early, if possible.
		if err := bgSeq.healDiskMeta(); err != nil {
			if newObjectLayerFn() != nil {
				// log only in situations, when object layer
				// has fully initialized.
				logger.LogIf(bgSeq.ctx, err)
			}
		}
	}

	go monitorLocalDisksAndHeal(ctx, z, drivesToHeal, localDisksInZoneHeal, bgSeq)
}

func getLocalDisksToHeal(objAPI ObjectLayer) []Endpoints {
	z, ok := objAPI.(*erasureZones)
	if !ok {
		return nil
	}

	// Attempt a heal as the server starts-up first.
	localDisksInZoneHeal := make([]Endpoints, len(z.zones))
	for i, ep := range globalEndpoints {
		localDisksToHeal := Endpoints{}
		for _, endpoint := range ep.Endpoints {
			if !endpoint.IsLocal {
				continue
			}
			// Try to connect to the current endpoint
			// and reformat if the current disk is not formatted
			_, _, err := connectEndpoint(endpoint)
			if errors.Is(err, errUnformattedDisk) {
				localDisksToHeal = append(localDisksToHeal, endpoint)
			}
		}
		if len(localDisksToHeal) == 0 {
			continue
		}
		localDisksInZoneHeal[i] = localDisksToHeal
	}
	return localDisksInZoneHeal

}

func getDrivesToHealCount(localDisksInZoneHeal []Endpoints) int {
	var drivesToHeal int
	for _, eps := range localDisksInZoneHeal {
		for range eps {
			drivesToHeal++
		}
	}
	return drivesToHeal
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
func monitorLocalDisksAndHeal(ctx context.Context, z *erasureZones, drivesToHeal int, localDisksInZoneHeal []Endpoints, bgSeq *healSequence) {
	// Perform automatic disk healing when a disk is replaced locally.
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(defaultMonitorNewDiskInterval):
			// heal only if new disks found.
			if drivesToHeal == 0 {
				localDisksInZoneHeal = getLocalDisksToHeal(z)
				drivesToHeal = getDrivesToHealCount(localDisksInZoneHeal)
				if drivesToHeal == 0 {
					// No drives to heal.
					globalBackgroundHealState.updateHealLocalDisks(nil)
					continue
				}
				globalBackgroundHealState.updateHealLocalDisks(localDisksInZoneHeal)

				logger.Info(fmt.Sprintf("Found drives to heal %d, proceeding to heal content...",
					drivesToHeal))

				// Reformat disks
				bgSeq.sourceCh <- healSource{bucket: SlashSeparator}

				// Ensure that reformatting disks is finished
				bgSeq.sourceCh <- healSource{bucket: nopHeal}
			}

			var erasureSetInZoneToHeal = make([][]int, len(localDisksInZoneHeal))
			// Compute the list of erasure set to heal
			for i, localDisksToHeal := range localDisksInZoneHeal {
				var erasureSetToHeal []int
				for _, endpoint := range localDisksToHeal {
					// Load the new format of this passed endpoint
					_, format, err := connectEndpoint(endpoint)
					if err != nil {
						printEndpointError(endpoint, err, true)
						continue
					}

					// Calculate the set index where the current endpoint belongs
					setIndex, _, err := findDiskIndex(z.zones[i].format, format)
					if err != nil {
						printEndpointError(endpoint, err, false)
						continue
					}

					erasureSetToHeal = append(erasureSetToHeal, setIndex)
				}
				erasureSetInZoneToHeal[i] = erasureSetToHeal
			}

			logger.Info("New unformatted drives detected attempting to heal the content...")
			for i, disks := range localDisksInZoneHeal {
				for _, disk := range disks {
					logger.Info("Healing disk '%s' on %s zone", disk, humanize.Ordinal(i+1))
				}
			}

			// Heal all erasure sets that need
			for i, erasureSetToHeal := range erasureSetInZoneToHeal {
				for _, setIndex := range erasureSetToHeal {
					err := healErasureSet(ctx, setIndex, z.zones[i].sets[setIndex], z.zones[i].setDriveCount)
					if err != nil {
						logger.LogIf(ctx, err)
					}

					// Only upon success reduce the counter
					if err == nil {
						drivesToHeal--
					}
				}
			}
		}
	}
}
