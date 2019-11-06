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
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
)

const (
	dataUsageObjName       = "data-usage"
	dataUsageCrawlInterval = 12 * time.Hour
)

func initDataUsageStats() {
	go runDataUsageInfoUpdateRoutine()
}

func runDataUsageInfoUpdateRoutine() {
	// Wait until the object layer is ready
	var objAPI ObjectLayer
	for {
		objAPI = newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	ctx := context.Background()

	switch v := objAPI.(type) {
	case *xlZones:
		runDataUsageInfoForXLZones(ctx, v)
	case *FSObjects:
		runDataUsageInfoForFS(ctx, v)
	default:
		return
	}
}

func runDataUsageInfoForFS(ctx context.Context, fsObj *FSObjects) {
	for {
		// Get data usage info of the FS Object
		usageInfo := fsObj.crawlAndGetDataUsageInfo(ctx)
		// Save the data usage in the disk
		err := storeDataUsageInBackend(ctx, fsObj, usageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
		}
		// Wait until the next crawl interval
		time.Sleep(dataUsageCrawlInterval)
	}
}

func runDataUsageInfoForXLZones(ctx context.Context, zones *xlZones) {
	for {
		locker := zones.NewNSLock(ctx, minioMetaBucket, "leader-data-usage-info")
		err := locker.GetLock(newDynamicTimeout(time.Millisecond, time.Millisecond))
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}
		// Break without locking
		break
	}

	for {
		usageInfo := zonesCrawlAndGetDataUsage(ctx, zones)
		err := storeDataUsageInBackend(ctx, zones, usageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
		}

		time.Sleep(dataUsageCrawlInterval)
	}

}

func zonesCrawlAndGetDataUsage(ctx context.Context, zones *xlZones) DataUsageInfo {
	// Calculate the aggregated data usage, meaning
	// accumulate data usage of all zones.
	var aggregatedDataUsageInfo = DataUsageInfo{
		ObjectsSizesHistogram: make(map[string]uint64),
		BucketsSizes:          make(map[string]uint64),
	}

	addUpUsageInfo := func(dataUsageInfo DataUsageInfo) {
		aggregatedDataUsageInfo.ObjectsCount += dataUsageInfo.ObjectsCount
		aggregatedDataUsageInfo.ObjectsTotalSize += dataUsageInfo.ObjectsTotalSize
		if aggregatedDataUsageInfo.BucketsCount < dataUsageInfo.BucketsCount {
			aggregatedDataUsageInfo.BucketsCount = dataUsageInfo.BucketsCount
		}
		for k, v := range dataUsageInfo.ObjectsSizesHistogram {
			aggregatedDataUsageInfo.ObjectsSizesHistogram[k] += v
		}
		for k, v := range dataUsageInfo.BucketsSizes {
			aggregatedDataUsageInfo.BucketsSizes[k] += v
		}
	}

	for _, z := range zones.zones {
		for _, xl := range z.sets {
			var randomDisks []StorageAPI
			for _, d := range xl.getLoadBalancedDisks() {
				if d == nil || !d.IsOnline() {
					continue
				}
				if len(randomDisks) > 3 {
					break
				}
				randomDisks = append(randomDisks, d)
			}

			var dataUsageResult = make([]DataUsageInfo, len(randomDisks))

			var wg sync.WaitGroup
			for i := 0; i < len(randomDisks); i++ {
				wg.Add(1)
				go func(index int, disk StorageAPI) {
					defer wg.Done()
					var err error
					dataUsageResult[index], err = disk.CrawlAndGetDataUsage()
					if err != nil {
						logger.LogIf(ctx, err)
					}
				}(i, randomDisks[i])
			}
			wg.Wait()

			var reliableDataUsageResult DataUsageInfo
			for i := 0; i < len(dataUsageResult); i++ {
				if dataUsageResult[i].ObjectsCount > reliableDataUsageResult.ObjectsCount {
					reliableDataUsageResult = dataUsageResult[i]
				}
			}

			addUpUsageInfo(reliableDataUsageResult)
		}
	}

	aggregatedDataUsageInfo.LastUpdate = UTCNow()
	return aggregatedDataUsageInfo
}

func storeDataUsageInBackend(ctx context.Context, objAPI ObjectLayer, dataUsageInfo DataUsageInfo) error {
	dataUsageJSON, err := json.Marshal(dataUsageInfo)
	if err != nil {
		return err
	}

	size := int64(len(dataUsageJSON))
	r, err := hash.NewReader(bytes.NewReader(dataUsageJSON), size, "", "", size, false)
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(ctx, minioMetaBackgroundOpsBucket, dataUsageObjName, NewPutObjReader(r, nil, nil), ObjectOptions{})
	return err
}

func loadDataUsageFromBackend(ctx context.Context, objAPI ObjectLayer) (DataUsageInfo, error) {
	var dataUsageInfoJSON bytes.Buffer

	err := objAPI.GetObject(ctx, minioMetaBackgroundOpsBucket, dataUsageObjName, 0, -1, &dataUsageInfoJSON, "", ObjectOptions{})
	if err != nil {
		return DataUsageInfo{}, nil
	}

	var dataUsageInfo DataUsageInfo
	err = json.Unmarshal(dataUsageInfoJSON.Bytes(), &dataUsageInfo)
	if err != nil {
		return DataUsageInfo{}, err
	}

	return dataUsageInfo, nil
}
