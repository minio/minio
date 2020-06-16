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
	"fmt"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
)

// BucketQuotaSys - map of bucket and quota configuration.
type BucketQuotaSys struct {
	bucketStorageCache timedValue
}

// Get - Get quota configuration.
func (sys *BucketQuotaSys) Get(bucketName string) (*madmin.BucketQuota, error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		return &madmin.BucketQuota{}, nil
	}

	return globalBucketMetadataSys.GetQuotaConfig(bucketName)
}

// NewBucketQuotaSys returns initialized BucketQuotaSys
func NewBucketQuotaSys() *BucketQuotaSys {
	return &BucketQuotaSys{}
}

// parseBucketQuota parses BucketQuota from json
func parseBucketQuota(bucket string, data []byte) (quotaCfg *madmin.BucketQuota, err error) {
	quotaCfg = &madmin.BucketQuota{}
	if err = json.Unmarshal(data, quotaCfg); err != nil {
		return quotaCfg, err
	}
	if !quotaCfg.IsValid() {
		return quotaCfg, fmt.Errorf("Invalid quota config %#v", quotaCfg)
	}
	return
}

func (sys *BucketQuotaSys) check(ctx context.Context, bucket string, size int64) error {
	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	q, err := sys.Get(bucket)
	if err != nil {
		return nil
	}

	if q.Type == madmin.FIFOQuota {
		return nil
	}

	if q.Quota == 0 {
		// No quota set return quickly.
		return nil
	}

	sys.bucketStorageCache.Once.Do(func() {
		sys.bucketStorageCache.TTL = 10 * time.Second
		sys.bucketStorageCache.Update = func() (interface{}, error) {
			return loadDataUsageFromBackend(ctx, objAPI)
		}
	})

	v, err := sys.bucketStorageCache.Get()
	if err != nil {
		return err
	}

	dui := v.(DataUsageInfo)

	bui, ok := dui.BucketsUsage[bucket]
	if !ok {
		// bucket not found, cannot enforce quota
		// call will fail anyways later.
		return nil
	}

	if (bui.Size + uint64(size)) > q.Quota {
		return BucketQuotaExceeded{Bucket: bucket}
	}

	return nil
}

func enforceBucketQuota(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}

	return globalBucketQuotaSys.check(ctx, bucket, size)
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
			enforceFIFOQuota(ctx, objAPI)
		}

	}
}

// enforceFIFOQuota deletes objects in FIFO order until sufficient objects
// have been deleted so as to bring bucket usage within quota
func enforceFIFOQuota(ctx context.Context, objectAPI ObjectLayer) {
	// Turn off quota enforcement if data usage info is unavailable.
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		return
	}

	buckets, err := objectAPI.ListBuckets(ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}

	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}

	for _, binfo := range buckets {
		bucket := binfo.Name

		bui, ok := dataUsageInfo.BucketsUsage[bucket]
		if !ok {
			// bucket doesn't exist anymore, or we
			// do not have any information to proceed.
			continue
		}

		// Check if the current bucket has quota restrictions, if not skip it
		cfg, err := globalBucketQuotaSys.Get(bucket)
		if err != nil {
			continue
		}

		if cfg.Type != madmin.FIFOQuota {
			continue
		}

		var toFree uint64
		if bui.Size > cfg.Quota && cfg.Quota > 0 {
			toFree = bui.Size - cfg.Quota
		}

		if toFree == 0 {
			continue
		}

		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan ObjectInfo)

		// Walk through all objects
		if err := objectAPI.Walk(ctx, bucket, "", objInfoCh); err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		// reuse the fileScorer used by disk cache to score entries by
		// ModTime to find the oldest objects in bucket to delete. In
		// the context of bucket quota enforcement - number of hits are
		// irrelevant.
		scorer, err := newFileScorer(toFree, time.Now().Unix(), 1)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		rcfg, _ := globalBucketObjectLockSys.Get(bucket)
		for obj := range objInfoCh {
			if obj.DeleteMarker {
				// Delete markers are automatically added for FIFO purge.
				scorer.addFileWithObjInfo(obj, 1)
				continue
			}
			// skip objects currently under retention
			if rcfg.LockEnabled && enforceRetentionForDeletion(ctx, obj) {
				continue
			}
			scorer.addFileWithObjInfo(obj, 1)
		}

		versioned := globalBucketVersioningSys.Enabled(bucket)

		var objects []ObjectToDelete
		numKeys := len(scorer.fileObjInfos())
		for i, obj := range scorer.fileObjInfos() {
			objects = append(objects, ObjectToDelete{
				ObjectName: obj.Name,
				VersionID:  obj.VersionID,
			})
			if len(objects) < maxDeleteList && (i < numKeys-1) {
				// skip deletion until maxDeleteList or end of slice
				continue
			}

			if len(objects) == 0 {
				break
			}

			// Deletes a list of objects.
			_, deleteErrs := objectAPI.DeleteObjects(ctx, bucket, objects, ObjectOptions{
				Versioned: versioned,
			})
			for i := range deleteErrs {
				if deleteErrs[i] != nil {
					logger.LogIf(ctx, deleteErrs[i])
					continue
				}

				// Notify object deleted event.
				sendEvent(eventArgs{
					EventName:  event.ObjectRemovedDelete,
					BucketName: bucket,
					Object:     obj,
					Host:       "Internal: [FIFO-QUOTA-EXPIRY]",
				})
			}
			objects = nil
		}
	}
}
