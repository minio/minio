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
	"bytes"
	"errors"
	"io"

	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
)

// BucketSSEConfigSys - in-memory cache of bucket encryption config
type BucketSSEConfigSys struct {
	bucketSSEConfigMap map[string]*bucketsse.BucketSSEConfig
}

// NewBucketSSEConfigSys - Creates an empty in-memory bucket encryption configuration cache
func NewBucketSSEConfigSys() *BucketSSEConfigSys {
	return &BucketSSEConfigSys{
		bucketSSEConfigMap: make(map[string]*bucketsse.BucketSSEConfig),
	}
}

// load - Loads the bucket encryption configuration for the given list of buckets
func (sys *BucketSSEConfigSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		configData, err := globalBucketMetadataSys.GetConfig(bucket.Name, bucketSSEConfig)
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				continue
			}
			return err
		}

		config, err := bucketsse.ParseBucketSSEConfig(bytes.NewReader(configData))
		if err != nil {
			return err
		}

		sys.bucketSSEConfigMap[bucket.Name] = config
	}

	return nil
}

// Init - Initializes in-memory bucket encryption config cache for the given list of buckets
func (sys *BucketSSEConfigSys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// We don't cache bucket encryption config in gateway mode, nothing to do.
	if globalIsGateway {
		return nil
	}

	// Load bucket encryption config cache once during boot.
	return sys.load(buckets, objAPI)
}

// Get - gets bucket encryption config for the given bucket.
func (sys *BucketSSEConfigSys) Get(bucket string) (config *bucketsse.BucketSSEConfig, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketSSEConfigNotFound{Bucket: bucket}
	}

	config, ok := sys.bucketSSEConfigMap[bucket]
	if !ok {
		configData, err := globalBucketMetadataSys.GetConfig(bucket, bucketSSEConfig)
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				return nil, BucketSSEConfigNotFound{Bucket: bucket}
			}
			return nil, err
		}
		return bucketsse.ParseBucketSSEConfig(bytes.NewReader(configData))
	}
	return config, nil
}

// validateBucketSSEConfig parses bucket encryption configuration and validates if it is supported by MinIO.
func validateBucketSSEConfig(r io.Reader) (*bucketsse.BucketSSEConfig, error) {
	encConfig, err := bucketsse.ParseBucketSSEConfig(r)
	if err != nil {
		return nil, err
	}

	if len(encConfig.Rules) == 1 && encConfig.Rules[0].DefaultEncryptionAction.Algorithm == bucketsse.AES256 {
		return encConfig, nil
	}
	return nil, errors.New("Unsupported bucket encryption configuration")
}
