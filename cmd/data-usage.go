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
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
)

const (
	dataUsageRoot   = SlashSeparator
	dataUsageBucket = minioMetaBucket + SlashSeparator + bucketMetaPrefix

	dataUsageObjName   = ".usage.json"
	dataUsageCacheName = ".usage-cache.bin"
	dataUsageBloomName = ".bloomcycle.bin"
)

// storeDataUsageInBackend will store all objects sent on the gui channel until closed.
func storeDataUsageInBackend(ctx context.Context, objAPI ObjectLayer, dui <-chan madmin.DataUsageInfo) {
	for dataUsageInfo := range dui {
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		dataUsageJSON, err := json.Marshal(dataUsageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		size := int64(len(dataUsageJSON))
		r, err := hash.NewReader(bytes.NewReader(dataUsageJSON), size, "", "", size)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageObjName, NewPutObjReader(r), ObjectOptions{})
		if !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	}
}

// loadPrefixUsageFromBackend returns prefix usages found in passed buckets
//   e.g.:  /testbucket/prefix => 355601334
func loadPrefixUsageFromBackend(ctx context.Context, objAPI ObjectLayer, bucket string) (map[string]uint64, error) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		return nil, errors.New("prefix usage is not supported")
	}

	cache := dataUsageCache{}

	m := make(map[string]uint64)
	for _, pool := range z.serverPools {
		for _, er := range pool.sets {
			// Load bucket usage prefixes
			if err := cache.load(ctx, er, bucket+slashSeparator+dataUsageCacheName); err == nil {
				root := cache.find(bucket)
				if root == nil {
					// We dont have usage information for this bucket in this
					// set, go to the next set
					continue
				}

				for id, usageInfo := range cache.flattenChildrens(*root) {
					prefix := strings.TrimPrefix(id, bucket+slashSeparator)
					m[prefix] += uint64(usageInfo.Size)
				}
			}
		}
	}

	return m, nil
}

func loadDataUsageFromBackend(ctx context.Context, objAPI ObjectLayer) (madmin.DataUsageInfo, error) {
	r, err := objAPI.GetObjectNInfo(ctx, dataUsageBucket, dataUsageObjName, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		if isErrObjectNotFound(err) || isErrBucketNotFound(err) {
			return madmin.DataUsageInfo{}, nil
		}
		return madmin.DataUsageInfo{}, toObjectErr(err, dataUsageBucket, dataUsageObjName)
	}
	defer r.Close()

	var dataUsageInfo madmin.DataUsageInfo
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.NewDecoder(r).Decode(&dataUsageInfo); err != nil {
		return madmin.DataUsageInfo{}, err
	}

	// For forward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketsUsage) == 0 {
		dataUsageInfo.BucketsUsage = make(map[string]madmin.BucketUsageInfo, len(dataUsageInfo.BucketSizes))
		for bucket, size := range dataUsageInfo.BucketSizes {
			dataUsageInfo.BucketsUsage[bucket] = madmin.BucketUsageInfo{Size: size}
		}
	}

	// For backward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketSizes) == 0 {
		dataUsageInfo.BucketSizes = make(map[string]uint64, len(dataUsageInfo.BucketsUsage))
		for bucket, bui := range dataUsageInfo.BucketsUsage {
			dataUsageInfo.BucketSizes[bucket] = bui.Size
		}
	}

	return dataUsageInfo, nil
}
