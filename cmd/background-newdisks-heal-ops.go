/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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

const defaultMonitorNewDiskInterval = time.Minute * 10

func initDisksAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	go monitorDisksAndHeal(ctx, objAPI)
}

func runStartupHeal(ctx context.Context, objAPI ObjectLayer, healCh chan healSource) {
	// Reformat disks and wait until it finishes
	healCh <- healSource{path: SlashSeparator}
	healCh <- healSource{path: nopHeal}

	settings := madmin.HealOpts{Recursive: true, ScanMode: madmin.HealDeepScan}

	// Heal all objects under .minio.sys/config/
	objAPI.HealObjects(ctx, minioMetaBucket, minioConfigPrefix, settings, func(bucket string, object string) error {
		healCh <- healSource{path: pathJoin(bucket, object)}
		return nil
	})

	// Heal all objects under .minio.sys/buckets/
	objAPI.HealObjects(ctx, minioMetaBucket, bucketConfigPrefix, settings, func(bucket string, object string) error {
		healCh <- healSource{path: pathJoin(bucket, object)}
		return nil
	})

	// Recreate buckets
	buckets, err := objAPI.ListBucketsHeal(ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}

	for _, bucket := range buckets {
		_ = objAPI.MakeBucketWithLocation(ctx, bucket.Name, "", false)
	}
}

func computeUnformattedDisks(endpoints EndpointZones) (disksInZoneHeal []Endpoints, unformattedDiskFound bool) {
	disksInZoneHeal = make([]Endpoints, len(endpoints))
	for i, ep := range endpoints {
		disksToHeal := Endpoints{}
		for _, endpoint := range ep.Endpoints {
			// Try to connect to the current endpoint
			// to check if it is unformatted or not
			_, _, err := connectEndpoint(endpoint)
			if err == errUnformattedDisk {
				disksToHeal = append(disksToHeal, endpoint)
			}
		}
		if len(disksToHeal) == 0 {
			continue
		}
		disksInZoneHeal[i] = disksToHeal
		unformattedDiskFound = true
	}

	return
}

// monitorDisksAndHeal - ensures that detected new disks are healed,
// only the concerned erasure set will be listed and healed
func monitorDisksAndHeal(ctx context.Context, objAPI ObjectLayer) {

	z, ok := objAPI.(*xlZones)
	if !ok {
		return
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

	// Loop until all nodes are up
	for {
		info := z.StorageInfo(ctx, false)
		for _, online := range info.Backend.OnlineDisks {
			if online == 0 {
				time.Sleep(5 * time.Second)
				continue
			}
		}
		break
	}

	// Save unformatted disks to heal them later, this is executed before
	// runStartupHeal because this latter will format disks.
	disksInZoneHeal, unformattedDiskFound := computeUnformattedDisks(globalEndpoints)

	// Run the heal procedure of the startup mode
	runStartupHeal(ctx, z, bgSeq.sourceCh)

	// Perform automatic disk healing when a disk is replaced locally.
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(defaultMonitorNewDiskInterval):
			if len(disksInZoneHeal) == 0 {
				disksInZoneHeal, unformattedDiskFound = computeUnformattedDisks(globalEndpoints)
			}

			if !unformattedDiskFound {
				break
			}

			// Reformat disks and wait until it finishes
			bgSeq.sourceCh <- healSource{path: SlashSeparator}
			bgSeq.sourceCh <- healSource{path: nopHeal}

			var erasureSetInZoneToHeal = make([][]int, len(disksInZoneHeal))
			// Compute the list of erasure set to heal
			for i, disksToHeal := range disksInZoneHeal {
				var erasureSetToHeal []int
				for _, endpoint := range disksToHeal {
					// Load the new format of this passed endpoint
					_, format, err := connectEndpoint(endpoint)
					if err != nil {
						logger.LogIf(ctx, err)
						continue
					}
					// Calculate the set index where the current endpoint belongs
					setIndex, _, err := findDiskIndex(z.zones[i].format, format)
					if err != nil {
						logger.LogIf(ctx, err)
						continue
					}

					erasureSetToHeal = append(erasureSetToHeal, setIndex)
				}
				erasureSetInZoneToHeal[i] = erasureSetToHeal
			}

			// Heal all needed erasure sets
			for i, erasureSetToHeal := range erasureSetInZoneToHeal {
				for _, setIndex := range erasureSetToHeal {
					err := healErasureSet(ctx, setIndex, z.zones[i].sets[setIndex])
					if err != nil {
						logger.LogIf(ctx, err)
					}
				}
			}
		}

		disksInZoneHeal, unformattedDiskFound = nil, false
	}
}
