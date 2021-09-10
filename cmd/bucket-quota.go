// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
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
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	sys.bucketStorageCache.Once.Do(func() {
		sys.bucketStorageCache.TTL = 1 * time.Second
		sys.bucketStorageCache.Update = func() (interface{}, error) {
			ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			return loadDataUsageFromBackend(ctx, objAPI)
		}
	})

	q, err := sys.Get(bucket)
	if err != nil {
		return err
	}

	if q != nil && q.Type == madmin.HardQuota && q.Quota > 0 {
		v, err := sys.bucketStorageCache.Get()
		if err != nil {
			return err
		}

		dui := v.(madmin.DataUsageInfo)

		bui, ok := dui.BucketsUsage[bucket]
		if !ok {
			// bucket not found, cannot enforce quota
			// call will fail anyways later.
			return nil
		}

		if (bui.Size + uint64(size)) >= q.Quota {
			return BucketQuotaExceeded{Bucket: bucket}
		}
	}

	return nil
}

func enforceBucketQuota(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}

	return globalBucketQuotaSys.check(ctx, bucket, size)
}

// enforceFIFOQuota deletes objects in FIFO order until sufficient objects
// have been deleted so as to bring bucket usage within quota.
func enforceFIFOQuotaBucket(ctx context.Context, objectAPI ObjectLayer, bucket string, bui madmin.BucketUsageInfo) {
	// Check if the current bucket has quota restrictions, if not skip it
	cfg, err := globalBucketQuotaSys.Get(bucket)
	if err != nil {
		return
	}

	if cfg.Type != madmin.FIFOQuota {
		return
	}

	var toFree uint64
	if bui.Size > cfg.Quota && cfg.Quota > 0 {
		toFree = bui.Size - cfg.Quota
	}

	if toFree <= 0 {
		return
	}

	// Allocate new results channel to receive ObjectInfo.
	objInfoCh := make(chan ObjectInfo)

	versioned := globalBucketVersioningSys.Enabled(bucket)

	// Walk through all objects
	if err := objectAPI.Walk(ctx, bucket, "", objInfoCh, ObjectOptions{WalkVersions: versioned}); err != nil {
		logger.LogIf(ctx, err)
		return
	}

	// reuse the fileScorer used by disk cache to score entries by
	// ModTime to find the oldest objects in bucket to delete. In
	// the context of bucket quota enforcement - number of hits are
	// irrelevant.
	scorer, err := newFileScorer(toFree, time.Now().Unix(), 1)
	if err != nil {
		logger.LogIf(ctx, err)
		return
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

	// If we saw less than quota we are good.
	if scorer.seenBytes <= cfg.Quota {
		return
	}
	// Calculate how much we want to delete now.
	toFreeNow := scorer.seenBytes - cfg.Quota
	// We were less over quota than we thought. Adjust so we delete less.
	// If we are more over, leave it for the next run to pick up.
	if toFreeNow < toFree {
		if !scorer.adjustSaveBytes(int64(toFreeNow) - int64(toFree)) {
			// We got below or at quota.
			return
		}
	}

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
