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
)

const defaultMonitorNewDiskInterval = time.Minute * 10

// healErasureSet lists and heals all objects in a specific erasure set
func healErasureSet(ctx context.Context, xlObj *xlObjects) error {
	resp := make(chan healResult)

	buckets, err := xlObj.ListBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		// Heal current bucket
		globalBackgroundHealing.queueHealTask(healTask{path: bucket.Name, responseCh: resp})
		bucketHRes := <-resp
		if bucketHRes.err != nil {
			logger.LogIf(ctx, bucketHRes.err)
			continue
		}

		listDir := listDirFactory(ctx, xlObj.getLoadBalancedDisks()...)
		walkResultCh := startTreeWalk(ctx, bucket.Name, "", "", true, listDir, nil)
		for walkEntry := range walkResultCh {
			globalBackgroundHealing.queueHealTask(healTask{path: pathJoin(bucket.Name, walkEntry.entry), responseCh: resp})
			objectHRes := <-resp
			if objectHRes.err != nil {
				logger.LogIf(ctx, objectHRes.err)
				continue
			}
		}
	}

	return nil
}

func initLocalDisksAutoHeal() {
	go monitorLocalDisksAndHeal()
}

// monitorLocalDisksAndHeal - ensures that detected new disks are healed
//  1. Only the concerned erasure set will be listed and healed
//  2. Only the node hosting the disk is responsible to perform the heal
func monitorLocalDisksAndHeal() {
	// Wait until the object layer is ready
	var objAPI ObjectLayer
	for {
		objAPI = newObjectLayerFn()
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

	ctx := context.Background()

	// Perform automatic disk healing when a new one is inserted
	for {
		time.Sleep(defaultMonitorNewDiskInterval)
		localDisksToHeal := []Endpoint{}
		for _, endpoint := range globalEndpoints {
			if !endpoint.IsLocal {
				continue
			}
			// Try to connect to the current endpoint
			// and reformat if the current disk is not formatted
			_, _, err := connectEndpoint(endpoint)
			if err == errUnformattedDisk {
				localDisksToHeal = append(localDisksToHeal, endpoint)
			}
		}

		if len(localDisksToHeal) == 0 {
			continue
		}

		// Reformat disks
		resp := make(chan healResult)
		globalBackgroundHealing.queueHealTask(healTask{path: "/", responseCh: resp})
		formatHRes := <-resp
		if formatHRes.err != nil {
			logger.LogIf(ctx, formatHRes.err)
			continue
		}

		for _, endpoint := range localDisksToHeal {
			// Load the new format of this passed endpoint
			_, format, err := connectEndpoint(endpoint)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}

			// Calculate the set index where the current endpoint belongs
			setIndex, _, err := findDiskIndex(sets.format, format)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}

			// List all buckets & objects in the current erasure set
			xlObj := sets.sets[setIndex]
			err = healErasureSet(ctx, xlObj)
			if err != nil {
				logger.LogIf(ctx, err)
			}
		}
	}
}
