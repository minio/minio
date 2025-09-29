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

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/logger"
)

// BucketQuotaSys - map of bucket and quota configuration.
type BucketQuotaSys struct{}

// Get - Get quota configuration.
func (sys *BucketQuotaSys) Get(ctx context.Context, bucketName string) (*madmin.BucketQuota, error) {
	cfg, _, err := globalBucketMetadataSys.GetQuotaConfig(ctx, bucketName)
	return cfg, err
}

// NewBucketQuotaSys returns initialized BucketQuotaSys
func NewBucketQuotaSys() *BucketQuotaSys {
	return &BucketQuotaSys{}
}

var bucketStorageCache = cachevalue.New[DataUsageInfo]()

// Init initialize bucket quota.
func (sys *BucketQuotaSys) Init(objAPI ObjectLayer) {
	bucketStorageCache.InitOnce(10*time.Second,
		cachevalue.Opts{ReturnLastGood: true, NoWait: true},
		func(ctx context.Context) (DataUsageInfo, error) {
			if objAPI == nil {
				return DataUsageInfo{}, errServerNotInitialized
			}
			ctx, done := context.WithTimeout(ctx, 2*time.Second)
			defer done()

			return loadDataUsageFromBackend(ctx, objAPI)
		},
	)
}

// GetBucketUsageInfo return bucket usage info for a given bucket
func (sys *BucketQuotaSys) GetBucketUsageInfo(ctx context.Context, bucket string) BucketUsageInfo {
	sys.Init(newObjectLayerFn())

	dui, err := bucketStorageCache.GetWithCtx(ctx)
	timedout := OperationTimedOut{}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.As(err, &timedout) {
		if len(dui.BucketsUsage) > 0 {
			internalLogOnceIf(GlobalContext, fmt.Errorf("unable to retrieve usage information for bucket: %s, relying on older value cached in-memory: err(%v)", bucket, err), "bucket-usage-cache-"+bucket)
		} else {
			internalLogOnceIf(GlobalContext, errors.New("unable to retrieve usage information for bucket: %s, no reliable usage value available - quota will not be enforced"), "bucket-usage-empty-"+bucket)
		}
	}

	if len(dui.BucketsUsage) > 0 {
		bui, ok := dui.BucketsUsage[bucket]
		if ok {
			return bui
		}
	}
	return BucketUsageInfo{}
}

// parseBucketQuota parses BucketQuota from json
func parseBucketQuota(bucket string, data []byte) (quotaCfg *madmin.BucketQuota, err error) {
	quotaCfg = &madmin.BucketQuota{}
	if err = json.Unmarshal(data, quotaCfg); err != nil {
		return quotaCfg, err
	}
	if !quotaCfg.IsValid() {
		if quotaCfg.Type == "fifo" {
			internalLogIf(GlobalContext, errors.New("Detected older 'fifo' quota config, 'fifo' feature is removed and not supported anymore. Please clear your quota configs using 'mc quota clear alias/bucket' and use 'mc ilm add' for expiration of objects"), logger.WarningKind)
			return quotaCfg, fmt.Errorf("invalid quota type 'fifo'")
		}
		return quotaCfg, fmt.Errorf("Invalid quota config %#v", quotaCfg)
	}
	return quotaCfg, err
}

func (sys *BucketQuotaSys) enforceQuotaHard(ctx context.Context, bucket string, size int64) error {
	if size < 0 {
		return nil
	}

	q, err := sys.Get(ctx, bucket)
	if err != nil {
		return err
	}

	var quotaSize uint64
	if q != nil && q.Type == madmin.HardQuota {
		if q.Size > 0 {
			quotaSize = q.Size
		} else if q.Quota > 0 {
			quotaSize = q.Quota
		}
	}
	if quotaSize > 0 {
		if uint64(size) >= quotaSize { // check if file size already exceeds the quota
			return BucketQuotaExceeded{Bucket: bucket}
		}

		bui := sys.GetBucketUsageInfo(ctx, bucket)
		if bui.Size > 0 && ((bui.Size + uint64(size)) >= quotaSize) {
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
