/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
)

// BucketQuotaSys - map of bucket and quota configuration.
type BucketQuotaSys struct {
	sync.RWMutex
	quotaMap map[string]madmin.BucketQuota
}

// Set - set quota configuration.
func (sys *BucketQuotaSys) Set(bucketName string, q madmin.BucketQuota) {
	sys.Lock()
	sys.quotaMap[bucketName] = q
	sys.Unlock()
}

// Get - Get quota configuration.
func (sys *BucketQuotaSys) Get(bucketName string) (q madmin.BucketQuota, ok bool) {
	sys.RLock()
	defer sys.RUnlock()
	q, ok = sys.quotaMap[bucketName]
	return
}

// Remove - removes quota configuration.
func (sys *BucketQuotaSys) Remove(bucketName string) {
	sys.Lock()
	delete(sys.quotaMap, bucketName)
	sys.Unlock()
}

// Buckets - list of buckets with quota configuration
func (sys *BucketQuotaSys) Buckets() []string {
	sys.RLock()
	defer sys.RUnlock()
	var buckets []string
	for k := range sys.quotaMap {
		buckets = append(buckets, k)
	}
	return buckets
}

// Init initialize bucket quota sys configuration with all buckets.
func (sys *BucketQuotaSys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, we do not support bucket quota.
	// So, this is a no-op for gateway servers.
	if globalIsGateway {
		return nil
	}

	return sys.load(buckets, objAPI)
}

func (sys *BucketQuotaSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		ctx := logger.SetReqInfo(GlobalContext, &logger.ReqInfo{BucketName: bucket.Name})
		configFile := path.Join(bucketConfigPrefix, bucket.Name, bucketQuotaConfigFile)
		configData, err := readConfig(ctx, objAPI, configFile)
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				continue
			}
			return err
		}
		quotaCfg, err := parseBucketQuota(configData)
		if err != nil {
			return err
		}
		globalBucketQuotaSys.Set(bucket.Name, quotaCfg)
	}
	return nil
}

// NewBucketQuotaSys returns initialized BucketQuotaSys
func NewBucketQuotaSys() *BucketQuotaSys {
	return &BucketQuotaSys{quotaMap: map[string]madmin.BucketQuota{}}
}

// parseBucketQuota parses BucketQuota from json
func parseBucketQuota(data []byte) (quotaCfg madmin.BucketQuota, err error) {
	err = json.Unmarshal(data, &quotaCfg)
	if err != nil {
		return
	}
	if !quotaCfg.Type.IsValid() {
		return quotaCfg, fmt.Errorf("Invalid quota type %s", quotaCfg.Type)
	}
	return
}

type bucketStorageCache struct {
	bucketsSizes map[string]uint64
	lastUpdate   time.Time
	mu           sync.Mutex
}

func (b *bucketStorageCache) check(ctx context.Context, q madmin.BucketQuota, bucket string, size int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if time.Since(b.lastUpdate) > 10*time.Second {
		dui, err := loadDataUsageFromBackend(ctx, newObjectLayerWithoutSafeModeFn())
		if err != nil {
			return err
		}
		b.lastUpdate = time.Now()
		b.bucketsSizes = dui.BucketsSizes
	}
	currUsage := b.bucketsSizes[bucket]
	if (currUsage + uint64(size)) > q.Quota {
		return BucketQuotaExceeded{Bucket: bucket}
	}
	return nil
}
func enforceBucketQuota(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}
	q, ok := globalBucketQuotaSys.Get(bucket)
	if !ok {
		return nil
	}

	if q.Type == madmin.FIFOQuota {
		return nil
	}
	return globalBucketStorageCache.check(ctx, q, bucket, size)
}

const (
	bgQuotaInterval = 1 * time.Hour
)

// initQuotaEnforcement starts the routine that deletes objects in bucket
// that exceeds the FIFO quota
func initQuotaEnforcement(ctx context.Context, objAPI ObjectLayer) {
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOn {
		go startBucketQuotaEnforcement(ctx, objAPI)
	}
}

func startBucketQuotaEnforcement(ctx context.Context, objAPI ObjectLayer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(bgQuotaInterval).C:
			logger.LogIf(ctx, enforceFIFOQuota(ctx, objAPI))
		}

	}
}

// enforceFIFOQuota deletes objects in FIFO order until sufficient objects
// have been deleted so as to bring bucket usage within quota
func enforceFIFOQuota(ctx context.Context, objectAPI ObjectLayer) error {
	// Turn off quota enforcement if data usage info is unavailable.
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		return nil
	}
	for _, bucket := range globalBucketQuotaSys.Buckets() {
		// Check if the current bucket has quota restrictions, if not skip it
		cfg, ok := globalBucketQuotaSys.Get(bucket)
		if !ok {
			continue
		}
		if cfg.Type != madmin.FIFOQuota {
			continue
		}
		_, bucketHasLockConfig := globalBucketObjectLockSys.Get(bucket)

		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
		if err != nil {
			return err
		}
		var toFree uint64
		if dataUsageInfo.BucketsSizes[bucket] > cfg.Quota {
			toFree = dataUsageInfo.BucketsSizes[bucket] - cfg.Quota
		}
		if toFree == 0 {
			continue
		}
		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan ObjectInfo)

		// Walk through all objects
		if err := objectAPI.Walk(ctx, bucket, "", objInfoCh); err != nil {
			return err
		}
		// reuse the fileScorer used by disk cache to score entries by
		// ModTime to find the oldest objects in bucket to delete. In
		// the context of bucket quota enforcement - number of hits are
		// irrelevant.
		scorer, err := newFileScorer(toFree, time.Now().Unix(), 1)
		if err != nil {
			return err
		}

		for obj := range objInfoCh {
			// skip objects currently under retention
			if bucketHasLockConfig && enforceRetentionForDeletion(ctx, obj) {
				continue
			}
			scorer.addFile(obj.Name, obj.ModTime, obj.Size, 1)
		}
		var objects []string
		numKeys := len(scorer.fileNames())
		for i, key := range scorer.fileNames() {
			objects = append(objects, key)
			if len(objects) < maxObjectList && (i < numKeys-1) {
				// skip deletion until maxObjectList or end of slice
				continue
			}

			if len(objects) == 0 {
				break
			}
			// Deletes a list of objects.
			deleteErrs, err := objectAPI.DeleteObjects(ctx, bucket, objects)
			if err != nil {
				logger.LogIf(ctx, err)
			} else {
				for i := range deleteErrs {
					if deleteErrs[i] != nil {
						logger.LogIf(ctx, deleteErrs[i])
						continue
					}
					// Notify object deleted event.
					sendEvent(eventArgs{
						EventName:  event.ObjectRemovedDelete,
						BucketName: bucket,
						Object: ObjectInfo{
							Name: objects[i],
						},
						Host: "Internal: [FIFO-QUOTA-EXPIRY]",
					})
				}
				objects = nil
			}
		}
	}
	return nil
}
