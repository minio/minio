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
	"math"
	"sync"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
)

//go:generate msgp -file $GOFILE

const (
	fifoDeletionFile = "fifodelete.json"
	// % below configured quota at which to start collecting FIFO usage.
	quotaThresholdPct = 20
)

// FIFOCallbackFn defines a callback that accepts a bucket
// and creates a list of oldest objects in the bucket
type FIFOCallbackFn func(bucket string) error

type fifoCallbackState struct {
	fn FIFOCallbackFn
	ch chan ObjectInfo
}

// bucketStorageCache caches bucket usage
type bucketStorageCache struct {
	bucketSizes map[string]uint64
	lastUpdate  time.Time
	mu          sync.RWMutex
}

func (bc *bucketStorageCache) getUsage(bucket string) (uint64, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	sz, ok := bc.bucketSizes[bucket]
	if ok {
		return sz, nil
	}
	return 0, fmt.Errorf("bucket quota not available")
}

// BucketQuotaSys - map of bucket and quota configuration.
type BucketQuotaSys struct {
	cache     *bucketStorageCache
	fifoState map[string]fifoCallbackState
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
	c := bucketStorageCache{
		bucketSizes: make(map[string]uint64),
		mu:          sync.RWMutex{},
	}
	return &BucketQuotaSys{cache: &c, fifoState: make(map[string]fifoCallbackState)}
}

// Init initializes the bucket storage cache.
func (sys *BucketQuotaSys) Init(ctx context.Context, objAPI ObjectLayer) error {
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		return nil
	}
	c := sys.cache
	dui, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		return err
	}
	buckets, err := objAPI.ListBuckets(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, binfo := range buckets {
		bucket := binfo.Name
		currUsage, ok := dui.BucketSizes[bucket]
		if !ok {
			// bucket doesn't exist anymore, or we
			// do not have any information to proceed.
			continue
		}

		// Check if the current bucket has quota restrictions, if not skip it
		if _, err := globalBucketQuotaSys.Get(bucket); err != nil {
			continue
		}
		c.bucketSizes[bucket] = currUsage
	}
	c.lastUpdate = time.Now()
	return nil
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

	currUsage, err := sys.cache.getUsage(bucket)
	if err != nil {
		// bucket doesn't exist anymore, or we
		// do not have any information to proceed.
		return nil
	}

	if (currUsage + uint64(size)) > q.Quota {
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
			logger.LogIf(ctx, enforceFIFOQuota(ctx, objAPI))
		}
	}
}

// enforceFIFOQuota deletes objects in FIFO order until sufficient objects
// have been deleted so as to bring bucket usage within quota
func enforceFIFOQuota(ctx context.Context, objectAPI ObjectLayer) error {
	for bucket, currUsage := range globalBucketQuotaSys.cache.bucketSizes {
		// Check if the current bucket has quota restrictions, if not skip it
		cfg, err := globalBucketQuotaSys.Get(bucket)
		if err != nil {
			continue
		}
		if cfg.Type != madmin.FIFOQuota {
			continue
		}
		toFree := int64(currUsage) - int64(cfg.Quota)
		if toFree <= 0 {
			continue
		}
		// fetch list of oldest objects in bucket eligible for deletion
		quotaDelFile := pathJoin(bucketMetaPrefix, bucket, fifoDeletionFile)
		data, err := readConfig(GlobalContext, objectAPI, quotaDelFile)
		if err != nil {
			return err
		}
		toDel := &DeleteFIFOList{Objects: make([]string, 1)}
		// OK, parse data.
		if _, err = toDel.UnmarshalMsg(data); err != nil {
			continue
		}
		rcfg, _ := globalBucketObjectLockSys.Get(bucket)
		var objects []string
		numKeys := len(toDel.Objects)
		remaining := int64(toFree)
		for i, key := range toDel.Objects {
			if remaining <= 0 { // break if deleted enough
				break
			}
			oi, err := objectAPI.GetObjectInfo(ctx, bucket, key, ObjectOptions{})
			if err != nil {
				continue
			}

			// skip objects currently under retention
			if rcfg.LockEnabled && enforceRetentionForDeletion(ctx, oi) {
				continue
			}
			objects = append(objects, key)
			remaining -= oi.Size
			if len(objects) < maxDeleteList && (i < numKeys-1) && remaining > 0 {
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

func (sys *BucketQuotaSys) startCollectingFIFOUsage(ctx context.Context) error {
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}
	// Turn off quota enforcement if data usage info is unavailable.
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOff {
		return nil
	}

	buckets, err := objectAPI.ListBuckets(ctx)
	if err != nil {
		return err
	}

	dui, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err != nil {
		return err
	}
	for _, binfo := range buckets {
		bucket := binfo.Name
		currUsage, ok := dui.BucketSizes[bucket]
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
		var toFree, remUsage int64
		if cfg.Quota > 0 {
			remUsage = -int64(currUsage) + int64(cfg.Quota)
			toFree = int64(float64(cfg.Quota) * float64(quotaThresholdPct) / 100.0)
			if remUsage < 0 {
				toFree = int64(math.Max(math.Abs(float64(remUsage)), float64(toFree)))
			}
			if remUsage <= toFree {
				globalBucketQuotaSys.buildFIFODeleteList(ctx, objectAPI, bucket, toFree)
			}
		}
	}
	return nil
}
func (sys *BucketQuotaSys) endCollectingFIFOUsage(ctx context.Context, objAPI ObjectLayer) {
	for _, cb := range sys.fifoState {
		close(cb.ch)
	}
	sys.Init(ctx, objAPI)
	sys.fifoState = make(map[string]fifoCallbackState)
}

// DeleteFIFOList holds sorted list of objects in FIFO order
type DeleteFIFOList struct {
	Objects []string
}

func (sys *BucketQuotaSys) buildFIFODeleteList(ctx context.Context, objLayer ObjectLayer, bucket string, size int64) {
	objInfoCh := make(chan ObjectInfo)
	quotaCallbackFn := func(bucket string) error {
		if size < 0 {
			return nil
		}
		// reuse the fileScorer used by disk cache to score entries by
		// ModTime to find the oldest objects in bucket to delete. In
		// the context of bucket quota enforcement - number of hits and size are
		// irrelevant.
		scorer, err := newFileScorer(uint64(size), time.Now().Unix(), 1)
		if err != nil {
			return err
		}
		scorer.skipSizeScore()
		for oi := range objInfoCh {
			scorer.addFile(oi.Name, oi.ModTime, oi.Size, 1)
		}
		dl := DeleteFIFOList{Objects: scorer.fileNames()}
		data := make([]byte, 0, dl.Msgsize())
		data, err = dl.MarshalMsg(data)
		if err != nil {
			return err
		}
		return saveConfig(ctx, objLayer, pathJoin(bucketMetaPrefix, bucket, fifoDeletionFile), data)
	}
	cb := fifoCallbackState{
		fn: quotaCallbackFn,
		ch: objInfoCh,
	}
	sys.fifoState[bucket] = cb
	go cb.fn(bucket)
}

// SendUsage sends object usage to BucketQuotaSys for buckets with FIFO quota restriction
func (sys *BucketQuotaSys) sendUsage(bucket, object string, size int64, modTime time.Time) {
	if _, ok := sys.fifoState[bucket]; ok {
		cb := sys.fifoState[bucket]
		cb.ch <- ObjectInfo{Bucket: bucket, Name: object, Size: size, ModTime: modTime}
	}
}
