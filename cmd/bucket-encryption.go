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
	"context"
	"encoding/xml"
	"errors"
	"io"
	"path"
	"sync"

	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
)

// BucketSSEConfigSys - in-memory cache of bucket encryption config
type BucketSSEConfigSys struct {
	sync.RWMutex
	bucketSSEConfigMap map[string]bucketsse.BucketSSEConfig
}

// NewBucketSSEConfigSys - Creates an empty in-memory bucket encryption configuration cache
func NewBucketSSEConfigSys() *BucketSSEConfigSys {
	return &BucketSSEConfigSys{
		bucketSSEConfigMap: make(map[string]bucketsse.BucketSSEConfig),
	}
}

// load - Loads the bucket encryption configuration for the given list of buckets
func (sys *BucketSSEConfigSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketSSEConfig(GlobalContext, bucket.Name)
		if err != nil {
			if _, ok := err.(BucketSSEConfigNotFound); ok {
				sys.Remove(bucket.Name)
			}
			continue
		}
		sys.Set(bucket.Name, *config)
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
func (sys *BucketSSEConfigSys) Get(bucket string) (config bucketsse.BucketSSEConfig, ok bool) {
	// We don't cache bucket encryption config in gateway mode.
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return
		}

		cfg, err := objAPI.GetBucketSSEConfig(GlobalContext, bucket)
		if err != nil {
			return
		}
		return *cfg, true
	}

	sys.Lock()
	defer sys.Unlock()
	config, ok = sys.bucketSSEConfigMap[bucket]
	return
}

// Set - sets bucket encryption config to given bucket name.
func (sys *BucketSSEConfigSys) Set(bucket string, config bucketsse.BucketSSEConfig) {
	// We don't cache bucket encryption config in gateway mode.
	if globalIsGateway {
		return
	}

	sys.Lock()
	defer sys.Unlock()
	sys.bucketSSEConfigMap[bucket] = config
}

// Remove - removes bucket encryption config for given bucket.
func (sys *BucketSSEConfigSys) Remove(bucket string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketSSEConfigMap, bucket)
}

// saveBucketSSEConfig - save bucket encryption config for given bucket.
func saveBucketSSEConfig(ctx context.Context, objAPI ObjectLayer, bucket string, config *bucketsse.BucketSSEConfig) error {
	data, err := xml.Marshal(config)
	if err != nil {
		return err
	}

	// Path to store bucket encryption config for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucket, bucketSSEConfig)
	return saveConfig(ctx, objAPI, configFile, data)
}

// getBucketSSEConfig - get bucket encryption config for given bucket.
func getBucketSSEConfig(objAPI ObjectLayer, bucket string) (*bucketsse.BucketSSEConfig, error) {
	// Path to bucket-encryption.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucket, bucketSSEConfig)
	configData, err := readConfig(GlobalContext, objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = BucketSSEConfigNotFound{Bucket: bucket}
		}
		return nil, err
	}

	return bucketsse.ParseBucketSSEConfig(bytes.NewReader(configData))
}

// removeBucketSSEConfig - removes bucket encryption config for given bucket.
func removeBucketSSEConfig(ctx context.Context, objAPI ObjectLayer, bucket string) error {
	// Path to bucket-encryption.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucket, bucketSSEConfig)

	if err := objAPI.DeleteObject(ctx, minioMetaBucket, configFile); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketSSEConfigNotFound{Bucket: bucket}
		}
		return err
	}
	return nil
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
