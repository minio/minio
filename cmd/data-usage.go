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
		runDataUsageInfoForXLZones(ctx, v, GlobalServiceDoneCh)
	case *FSObjects:
		runDataUsageInfoForFS(ctx, v, GlobalServiceDoneCh)
	default:
		return
	}
}

func runDataUsageInfoForFS(ctx context.Context, fsObj *FSObjects, endCh <-chan struct{}) {
	t := time.NewTicker(dataUsageCrawlInterval)
	defer t.Stop()
	for {
		// Get data usage info of the FS Object
		usageInfo := fsObj.crawlAndGetDataUsageInfo(ctx, endCh)
		// Save the data usage in the disk
		err := storeDataUsageInBackend(ctx, fsObj, usageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
		}
		select {
		case <-endCh:
			return
		// Wait until the next crawl interval
		case <-t.C:
		}
	}
}

func runDataUsageInfoForXLZones(ctx context.Context, z *xlZones, endCh <-chan struct{}) {
	locker := z.NewNSLock(ctx, minioMetaBucket, "leader-data-usage-info")
	for {
		err := locker.GetLock(newDynamicTimeout(time.Millisecond, time.Millisecond))
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}
		// Break without locking
		break
	}

	t := time.NewTicker(dataUsageCrawlInterval)
	defer t.Stop()
	for {
		usageInfo := z.crawlAndGetDataUsage(ctx, endCh)
		err := storeDataUsageInBackend(ctx, z, usageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
		}
		select {
		case <-endCh:
			locker.Unlock()
			return
		case <-t.C:
		}
	}
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
