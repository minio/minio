/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
)

const (

	// Disabled means the lifecycle rule is inactive
	Disabled = "Disabled"
)

// LifecycleSys - Bucket lifecycle subsystem.
type LifecycleSys struct {
	bucketLifecycleMap map[string]*lifecycle.Lifecycle
}

// Get - gets lifecycle config associated to a given bucket name.
func (sys *LifecycleSys) Get(bucketName string) (lc *lifecycle.Lifecycle, err error) {
	if globalIsGateway {
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		return nil, BucketLifecycleNotFound{Bucket: bucketName}
	}

	lc, ok := sys.bucketLifecycleMap[bucketName]
	if !ok {
		configData, err := globalBucketMetadataSys.GetConfig(bucketName, bucketLifecycleConfig)
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				return nil, BucketLifecycleNotFound{Bucket: bucketName}
			}
			return nil, err
		}
		return lifecycle.ParseLifecycleConfig(bytes.NewReader(configData))
	}
	return lc, nil
}

// NewLifecycleSys - creates new lifecycle system.
func NewLifecycleSys() *LifecycleSys {
	return &LifecycleSys{
		bucketLifecycleMap: make(map[string]*lifecycle.Lifecycle),
	}
}

// Init - initializes lifecycle system from lifecycle.xml of all buckets.
func (sys *LifecycleSys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, we always fetch the bucket lifecycle configuration from the gateway backend.
	// So, this is a no-op for gateway servers.
	if globalIsGateway {
		return nil
	}

	// Load LifecycleSys once during boot.
	return sys.load(buckets, objAPI)
}

// Loads lifecycle policies for all buckets into LifecycleSys.
func (sys *LifecycleSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		configData, err := globalBucketMetadataSys.GetConfig(bucket.Name, bucketLifecycleConfig)
		if err != nil {
			if errors.Is(err, errConfigNotFound) {
				continue
			}
			return err
		}

		config, err := lifecycle.ParseLifecycleConfig(bytes.NewReader(configData))
		if err != nil {
			logger.LogIf(GlobalContext, err)
			continue
		}

		sys.bucketLifecycleMap[bucket.Name] = config
	}
	return nil
}
