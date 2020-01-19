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
type activeIOFn func() error

func updateUsage(basePath string, endCh <-chan struct{}, waitForLowActiveIO activeIOFn, getSize getSizeFn) DataUsageInfo {
	var dataUsageInfo = DataUsageInfo{
		BucketsSizes:          make(map[string]uint64),
		ObjectsSizesHistogram: make(map[string]uint64),
	}

	itemCh := make(chan Item)
	skipCh := make(chan error)
	defer close(skipCh)

	go func() {
		defer close(itemCh)
		fastWalk(basePath, func(path string, typ os.FileMode) error {
			if err := waitForLowActiveIO(); err != nil {
				return filepath.SkipDir
			}

			select {
			case <-endCh:
				return filepath.SkipDir
			case itemCh <- Item{path, typ}:
			}
			return <-skipCh
		})
	}()

	for {
		select {
		case <-endCh:
			return dataUsageInfo
		case item, ok := <-itemCh:
			if !ok {
				return dataUsageInfo
			}

			bucket, entry := path2BucketObjectWithBasePath(basePath, item.Path)
			if bucket == "" {
				skipCh <- nil
				continue
			}

			if isReservedOrInvalidBucket(bucket, false) {
				skipCh <- filepath.SkipDir
				continue
			}

			if entry == "" && item.Typ&os.ModeDir != 0 {
				dataUsageInfo.BucketsCount++
				dataUsageInfo.BucketsSizes[bucket] = 0
				skipCh <- nil
				continue
			}

			if item.Typ&os.ModeDir != 0 {
				skipCh <- nil
				continue
			}

			size, err := getSize(item)
			if err != nil {
				skipCh <- errSkipFile
				continue
			}

			dataUsageInfo.ObjectsCount++
			dataUsageInfo.ObjectsTotalSize += uint64(size)
			dataUsageInfo.BucketsSizes[bucket] += uint64(size)
			dataUsageInfo.ObjectsSizesHistogram[objSizeToHistoInterval(uint64(size))]++
			skipCh <- nil
		}
	}
}
