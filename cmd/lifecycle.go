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
	"context"
	"encoding/xml"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lifecycle"
)

const (

	// Disabled means the lifecycle rule is inactive
	Disabled = "Disabled"
)

// LifecycleSys - Bucket lifecycle subsystem.
type LifecycleSys struct {
	sync.RWMutex
	bucketLifecycleMap map[string]lifecycle.Lifecycle
}

// Set - sets lifecycle config to given bucket name.
func (sys *LifecycleSys) Set(bucketName string, lifecycle lifecycle.Lifecycle) {
	sys.Lock()
	defer sys.Unlock()

	sys.bucketLifecycleMap[bucketName] = lifecycle
}

func saveLifecycleConfig(ctx context.Context, objAPI ObjectLayer, bucketName string, bucketLifecycle *lifecycle.Lifecycle) error {
	data, err := xml.Marshal(bucketLifecycle)
	if err != nil {
		return err
	}

	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifecycleConfig)
	return saveConfig(ctx, objAPI, configFile, data)
}

// getLifecycleConfig - get lifecycle config for given bucket name.
func getLifecycleConfig(objAPI ObjectLayer, bucketName string) (*lifecycle.Lifecycle, error) {
	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifecycleConfig)
	configData, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = BucketLifecycleNotFound{Bucket: bucketName}
		}
		return nil, err
	}

	return lifecycle.ParseLifecycleConfig(bytes.NewReader(configData))
}

func removeLifecycleConfig(ctx context.Context, objAPI ObjectLayer, bucketName string) error {
	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifecycleConfig)

	if err := objAPI.DeleteObject(ctx, minioMetaBucket, configFile); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketLifecycleNotFound{Bucket: bucketName}
		}
		return err
	}
	return nil
}

// NewLifecycleSys - creates new lifecycle system.
func NewLifecycleSys() *LifecycleSys {
	return &LifecycleSys{
		bucketLifecycleMap: make(map[string]lifecycle.Lifecycle),
	}
}

// Init - initializes lifecycle system from lifecycle.xml of all buckets.
func (sys *LifecycleSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	defer func() {
		// Refresh LifecycleSys in background.
		go func() {
			ticker := time.NewTicker(globalRefreshBucketLifecycleInterval)
			defer ticker.Stop()
			for {
				select {
				case <-GlobalServiceDoneCh:
					return
				case <-ticker.C:
					sys.refresh(objAPI)
				}
			}
		}()
	}()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing lifecycle needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	for range newRetryTimerSimple(doneCh) {
		// Load LifecycleSys once during boot.
		if err := sys.refresh(objAPI); err != nil {
			if err == errDiskNotFound ||
				strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
				strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
				logger.Info("Waiting for lifecycle subsystem to be initialized..")
				continue
			}
			return err
		}
		break
	}
	return nil
}

// Refresh LifecycleSys.
func (sys *LifecycleSys) refresh(objAPI ObjectLayer) error {
	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	sys.removeDeletedBuckets(buckets)
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketLifecycle(context.Background(), bucket.Name)
		if err != nil {
			if _, ok := err.(BucketLifecycleNotFound); ok {
				sys.Remove(bucket.Name)
			}
			continue
		}

		sys.Set(bucket.Name, *config)
	}

	return nil
}

// removeDeletedBuckets - to handle a corner case where we have cached the lifecycle for a deleted
// bucket. i.e if we miss a delete-bucket notification we should delete the corresponding
// bucket policy during sys.refresh()
func (sys *LifecycleSys) removeDeletedBuckets(bucketInfos []BucketInfo) {
	buckets := set.NewStringSet()
	for _, info := range bucketInfos {
		buckets.Add(info.Name)
	}
	sys.Lock()
	defer sys.Unlock()

	for bucket := range sys.bucketLifecycleMap {
		if !buckets.Contains(bucket) {
			delete(sys.bucketLifecycleMap, bucket)
		}
	}
}

// Remove - removes policy for given bucket name.
func (sys *LifecycleSys) Remove(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketLifecycleMap, bucketName)
}
