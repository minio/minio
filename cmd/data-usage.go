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
	"os"
	"path/filepath"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
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

	runDataUsageInfo(context.Background(), objAPI, GlobalServiceDoneCh)
}

// timeToNextCrawl returns the duration until next crawl should occur
// this is validated by verifying the LastUpdate time.
func timeToCrawl(ctx context.Context, objAPI ObjectLayer) time.Duration {
	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		// Upon an error wait for like 10
		// seconds to start the crawler.
		return 10 * time.Second
	}
	// File indeed doesn't exist when LastUpdate is zero
	// so we have never crawled, start crawl right away.
	if dataUsageInfo.LastUpdate.IsZero() {
		return 1 * time.Second
	}
	waitDuration := dataUsageInfo.LastUpdate.Sub(UTCNow())
	if waitDuration > dataUsageCrawlInterval {
		// Waited long enough start crawl in a 1 second
		return 1 * time.Second
	}
	// No crawling needed, ask the routine to wait until
	// the daily interval 12hrs - delta between last update
	// with current time.
	return dataUsageCrawlInterval - waitDuration
}

func runDataUsageInfo(ctx context.Context, objAPI ObjectLayer, endCh <-chan struct{}) {
	locker := objAPI.NewNSLock(ctx, minioMetaBucket, "leader-data-usage-info")
	for {
		err := locker.GetLock(newDynamicTimeout(time.Millisecond, time.Millisecond))
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}
		// Break without unlocking, this node will acquire
		// data usage calculator role for its lifetime.
		break
	}

	for {
		wait := timeToCrawl(ctx, objAPI)
		select {
		case <-endCh:
			locker.Unlock()
			return
		case <-time.NewTimer(wait).C:
			// Crawl only when no previous crawl has occurred,
			// or its been too long since last crawl.
			err := storeDataUsageInBackend(ctx, objAPI, objAPI.CrawlAndGetDataUsage(ctx, endCh))
			logger.LogIf(ctx, err)
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
		if isErrObjectNotFound(err) {
			return DataUsageInfo{}, nil
		}
		return DataUsageInfo{}, toObjectErr(err, minioMetaBackgroundOpsBucket, dataUsageObjName)
	}

	var dataUsageInfo DataUsageInfo
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal(dataUsageInfoJSON.Bytes(), &dataUsageInfo)
	if err != nil {
		return DataUsageInfo{}, err
	}

	return dataUsageInfo, nil
}

// Item represents each file while walking.
type Item struct {
	Path string
	Typ  os.FileMode
}

type getSizeFn func(item Item) (int64, error)

func updateUsage(basePath string, doneCh <-chan struct{}, waitForLowActiveIO func(), getSize getSizeFn) DataUsageInfo {
	var dataUsageInfo = DataUsageInfo{
		BucketsSizes:          make(map[string]uint64),
		ObjectsSizesHistogram: make(map[string]uint64),
	}

	numWorkers := 4
	var mutex sync.Mutex // Mutex to update dataUsageInfo

	fastWalk(basePath, numWorkers, doneCh, func(path string, typ os.FileMode) error {
		// Wait for I/O to go down.
		waitForLowActiveIO()

		bucket, entry := path2BucketObjectWithBasePath(basePath, path)
		if bucket == "" {
			return nil
		}

		if isReservedOrInvalidBucket(bucket, false) {
			return filepath.SkipDir
		}

		if entry == "" && typ&os.ModeDir != 0 {
			mutex.Lock()
			dataUsageInfo.BucketsCount++
			dataUsageInfo.BucketsSizes[bucket] = 0
			mutex.Unlock()
			return nil
		}

		mutex.Lock()
		defer mutex.Unlock()

		if typ&os.ModeDir != 0 {
			return nil
		}

		t := time.Now()
		size, err := getSize(Item{path, typ})
		// Use the response time of the getSize call to guess system load.
		// Sleep equivalent time.
		if d := time.Since(t); d > 100*time.Microsecond {
			time.Sleep(d)
		}
		if err != nil {
			return errSkipFile
		}

		dataUsageInfo.ObjectsCount++
		dataUsageInfo.ObjectsTotalSize += uint64(size)
		dataUsageInfo.BucketsSizes[bucket] += uint64(size)
		dataUsageInfo.ObjectsSizesHistogram[objSizeToHistoInterval(uint64(size))]++
		return nil
	})

	return dataUsageInfo
}
