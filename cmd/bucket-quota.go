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
	"errors"
	"fmt"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

// BucketQuotaSys - map of bucket and quota configuration.
type BucketQuotaSys struct {
	bucketStorageCache timedValue
}

// Get - Get quota configuration.
func (sys *BucketQuotaSys) Get(ctx context.Context, bucketName string) (*madmin.BucketQuota, error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		return &madmin.BucketQuota{}, nil
	}
	qCfg, _, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucketName)
	return qCfg, err
}

// NewBucketQuotaSys returns initialized BucketQuotaSys
func NewBucketQuotaSys() *BucketQuotaSys {
	return &BucketQuotaSys{}
}

// Init initialize bucket quota.
func (sys *BucketQuotaSys) Init(objAPI ObjectLayer) {
	sys.bucketStorageCache.Once.Do(func() {
		sys.bucketStorageCache.TTL = 1 * time.Second
		sys.bucketStorageCache.Update = func() (interface{}, error) {
			ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()

			return loadDataUsageFromBackend(ctx, objAPI)
		}
	})
}

// GetBucketUsageInfo return bucket usage info for a given bucket
func (sys *BucketQuotaSys) GetBucketUsageInfo(bucket string) (BucketUsageInfo, error) {
	v, err := sys.bucketStorageCache.Get()
	if err != nil {
		return BucketUsageInfo{}, err
	}

	dui, ok := v.(DataUsageInfo)
	if !ok {
		return BucketUsageInfo{}, fmt.Errorf("internal error: Unexpected DUI data type: %T", v)
	}

	bui := dui.BucketsUsage[bucket]
	return bui, nil
}

// parseBucketQuota parses BucketQuota from json
func parseBucketQuota(bucket string, data []byte) (quotaCfg *madmin.BucketQuota, err error) {
	quotaCfg = &madmin.BucketQuota{}
	if err = json.Unmarshal(data, quotaCfg); err != nil {
		return quotaCfg, err
	}
	if !quotaCfg.IsValid() {
		if quotaCfg.Type == "fifo" {
			logger.LogIf(GlobalContext, errors.New("Detected older 'fifo' quota config, 'fifo' feature is removed and not supported anymore. Please clear your quota configs using 'mc admin bucket quota alias/bucket --clear' and use 'mc ilm add' for expiration of objects"))
			return quotaCfg, nil
		}
		return quotaCfg, fmt.Errorf("Invalid quota config %#v", quotaCfg)
	}
	return
}

func (sys *BucketQuotaSys) enforceQuotaHard(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}

	q, err := sys.Get(ctx, bucket)
	if err != nil {
		return err
	}

	if q != nil && q.Type == madmin.HardQuota && q.Quota > 0 {
		bui, err := sys.GetBucketUsageInfo(bucket)
		if err != nil {
			return err
		}

		if bui.Size > 0 && ((bui.Size + uint64(size)) >= q.Quota) {
			return BucketQuotaExceeded{Bucket: bucket}
		}
	}

	return nil
}

func enforceBucketQuotaHard(ctx context.Context, bucket string, size int64) error {
	if globalBucketQuotaSys == nil {
		return nil
	}
	return globalBucketQuotaSys.enforceQuotaHard(ctx, bucket, size)
}
