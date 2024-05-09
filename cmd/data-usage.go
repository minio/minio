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
	"errors"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/cachevalue"
)

const (
	dataUsageRoot   = SlashSeparator
	dataUsageBucket = minioMetaBucket + SlashSeparator + bucketMetaPrefix

	dataUsageObjName       = ".usage.json"
	dataUsageObjNamePath   = bucketMetaPrefix + SlashSeparator + dataUsageObjName
	dataUsageBloomName     = ".bloomcycle.bin"
	dataUsageBloomNamePath = bucketMetaPrefix + SlashSeparator + dataUsageBloomName

	backgroundHealInfoPath = bucketMetaPrefix + SlashSeparator + ".background-heal.json"

	dataUsageCacheName = ".usage-cache.bin"
)

// storeDataUsageInBackend will store all objects sent on the dui channel until closed.
func storeDataUsageInBackend(ctx context.Context, objAPI ObjectLayer, dui <-chan DataUsageInfo) {
	attempts := 1
	for dataUsageInfo := range dui {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		dataUsageJSON, err := json.Marshal(dataUsageInfo)
		if err != nil {
			scannerLogIf(ctx, err)
			continue
		}
		if attempts > 10 {
			saveConfig(ctx, objAPI, dataUsageObjNamePath+".bkp", dataUsageJSON) // Save a backup every 10th update.
			attempts = 1
		}
		if err = saveConfig(ctx, objAPI, dataUsageObjNamePath, dataUsageJSON); err != nil {
			scannerLogOnceIf(ctx, err, dataUsageObjNamePath)
		}
		attempts++
	}
}

var prefixUsageCache = cachevalue.New[map[string]uint64]()

// loadPrefixUsageFromBackend returns prefix usages found in passed buckets
//
//	e.g.:  /testbucket/prefix => 355601334
func loadPrefixUsageFromBackend(ctx context.Context, objAPI ObjectLayer, bucket string) (map[string]uint64, error) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		// Prefix usage is empty
		return map[string]uint64{}, nil
	}

	cache := dataUsageCache{}

	prefixUsageCache.InitOnce(30*time.Second,
		// No need to fail upon Update() error, fallback to old value.
		cachevalue.Opts{ReturnLastGood: true, NoWait: true},
		func(ctx context.Context) (map[string]uint64, error) {
			m := make(map[string]uint64)
			for _, pool := range z.serverPools {
				for _, er := range pool.sets {
					// Load bucket usage prefixes
					ctx, done := context.WithTimeout(ctx, 2*time.Second)
					ok := cache.load(ctx, er, bucket+slashSeparator+dataUsageCacheName) == nil
					done()
					if ok {
						root := cache.find(bucket)
						if root == nil {
							// We dont have usage information for this bucket in this
							// set, go to the next set
							continue
						}

						for id, usageInfo := range cache.flattenChildrens(*root) {
							prefix := decodeDirObject(strings.TrimPrefix(id, bucket+slashSeparator))
							// decodeDirObject to avoid any __XLDIR__ objects
							m[prefix] += uint64(usageInfo.Size)
						}
					}
				}
			}
			return m, nil
		},
	)

	return prefixUsageCache.GetWithCtx(ctx)
}

func loadDataUsageFromBackend(ctx context.Context, objAPI ObjectLayer) (DataUsageInfo, error) {
	buf, err := readConfig(ctx, objAPI, dataUsageObjNamePath)
	if err != nil {
		buf, err = readConfig(ctx, objAPI, dataUsageObjNamePath+".bkp")
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				return DataUsageInfo{}, nil
			}
			return DataUsageInfo{}, toObjectErr(err, minioMetaBucket, dataUsageObjNamePath)
		}
	}

	var dataUsageInfo DataUsageInfo
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(buf, &dataUsageInfo); err != nil {
		return DataUsageInfo{}, err
	}
	// For forward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketsUsage) == 0 {
		dataUsageInfo.BucketsUsage = make(map[string]BucketUsageInfo, len(dataUsageInfo.BucketSizes))
		for bucket, size := range dataUsageInfo.BucketSizes {
			dataUsageInfo.BucketsUsage[bucket] = BucketUsageInfo{Size: size}
		}
	}

	// For backward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketSizes) == 0 {
		dataUsageInfo.BucketSizes = make(map[string]uint64, len(dataUsageInfo.BucketsUsage))
		for bucket, bui := range dataUsageInfo.BucketsUsage {
			dataUsageInfo.BucketSizes[bucket] = bui.Size
		}
	}
	// For forward compatibility reasons, we need to add this code.
	for bucket, bui := range dataUsageInfo.BucketsUsage {
		if bui.ReplicatedSizeV1 > 0 || bui.ReplicationFailedCountV1 > 0 ||
			bui.ReplicationFailedSizeV1 > 0 || bui.ReplicationPendingCountV1 > 0 {
			cfg, _ := getReplicationConfig(GlobalContext, bucket)
			if cfg != nil && cfg.RoleArn != "" {
				if dataUsageInfo.ReplicationInfo == nil {
					dataUsageInfo.ReplicationInfo = make(map[string]BucketTargetUsageInfo)
				}
				dataUsageInfo.ReplicationInfo[cfg.RoleArn] = BucketTargetUsageInfo{
					ReplicationFailedSize:   bui.ReplicationFailedSizeV1,
					ReplicationFailedCount:  bui.ReplicationFailedCountV1,
					ReplicatedSize:          bui.ReplicatedSizeV1,
					ReplicationPendingCount: bui.ReplicationPendingCountV1,
					ReplicationPendingSize:  bui.ReplicationPendingSizeV1,
				}
			}
		}
	}
	return dataUsageInfo, nil
}
