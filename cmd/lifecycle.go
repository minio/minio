/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

	// Deny - deny effect.
	Enabled = "Enabled"
)

// LifeCycleSys - lifecycle subsystems.
type LifeCycleSys struct {
	sync.RWMutex
	bucketLifeCycleMap map[string]lifecycle.LifeCycle
}

// Set - sets lifecycle to given bucket name.  If lifecycle is empty, existing lifecycle is removed.
func (sys *LifeCycleSys) Set(bucketName string, lifecycle lifecycle.LifeCycle) {
	sys.Lock()
	defer sys.Unlock()

	if lifecycle.IsEmpty() {
		delete(sys.bucketLifeCycleMap, bucketName)
	} else {
		sys.bucketLifeCycleMap[bucketName] = lifecycle
	}
}

func saveLifeCycleConfig(ctx context.Context, objAPI ObjectLayer, bucketName string, bucketLifeCycle *lifecycle.LifeCycle) error {
	data, err := xml.Marshal(bucketLifeCycle)

	if err != nil {
		return err
	}

	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifeCycleConfig)
	return saveConfig(ctx, objAPI, configFile, data)
}

// getLifeCycleConfig - get lifecycle config for given bucket name.
func getLifeCycleConfig(objAPI ObjectLayer, bucketName string) (*lifecycle.LifeCycle, error) {
	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifeCycleConfig)
	configData, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = BucketLifeCycleNotFound{Bucket: bucketName}
		}
		return nil, err
	}

	return lifecycle.ParseConfig(bytes.NewReader(configData))
}

func removeLifeCycleConfig(ctx context.Context, objAPI ObjectLayer, bucketName string) error {
	// Construct path to lifecycle.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketLifeCycleConfig)

	if err := objAPI.DeleteObject(ctx, minioMetaBucket, configFile); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketLifeCycleNotFound{Bucket: bucketName}
		}
		return err
	}
	return nil
}

// NewLifeCycleSys - creates new lifecycle system.
func NewLifeCycleSys() *LifeCycleSys {
	return &LifeCycleSys{
		bucketLifeCycleMap: make(map[string]lifecycle.LifeCycle),
	}
}

// Init - initializes lifecycle system from lifecycle.xml of all buckets.
func (sys *LifeCycleSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	defer func() {
		// Refresh LifeCycleSys in background.
		go func() {
			ticker := time.NewTicker(globalRefreshBucketLifeCycleInterval)
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
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			// Load LifeCycleSys once during boot.
			if err := sys.refresh(objAPI); err != nil {
				if err == errDiskNotFound ||
					strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
					strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
					logger.Info("Waiting for policy subsystem to be initialized..")
					continue
				}
				return err
			}
			return nil
		}
	}
}

// Refresh LifeCycleSys.
func (sys *LifeCycleSys) refresh(objAPI ObjectLayer) error {
	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	sys.removeDeletedBuckets(buckets)
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketLifeCycle(context.Background(), bucket.Name)
		if err != nil {
			if _, ok := err.(BucketLifeCycleNotFound); ok {
				sys.Remove(bucket.Name)
			}
			continue
		}

		if err = saveLifeCycleConfig(context.Background(), objAPI, bucket.Name, config); err != nil {
			logger.LogIf(context.Background(), err)
			return err
		}

		sys.Set(bucket.Name, *config)
	}

	return nil
}

// removeDeletedBuckets - to handle a corner case where we have cached the lifecycle for a deleted
// bucket. i.e if we miss a delete-bucket notification we should delete the corresponding
// bucket policy during sys.refresh()
func (sys *LifeCycleSys) removeDeletedBuckets(bucketInfos []BucketInfo) {
	buckets := set.NewStringSet()
	for _, info := range bucketInfos {
		buckets.Add(info.Name)
	}
	sys.Lock()
	defer sys.Unlock()

	for bucket := range sys.bucketLifeCycleMap {
		if !buckets.Contains(bucket) {
			delete(sys.bucketLifeCycleMap, bucket)
		}
	}
}

// Remove - removes policy for given bucket name.
func (sys *LifeCycleSys) Remove(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketLifeCycleMap, bucketName)
}

// Executes the lifecycle of bucket
func lifeCycleService(ctx context.Context, objAPI ObjectLayer, presentLifeCycle map[string]lifecycle.LifeCycle, now time.Time) {
	for bucketName, bucketlifecycle := range presentLifeCycle {
		for _, Rule := range bucketlifecycle.Rules {
			if Rule.Status == Enabled {
				prefix := ""
				if Rule.Filter.Prefix != "" {
					prefix = Rule.Filter.Prefix
				}
				objectslist, err := objAPI.ListObjects(ctx, bucketName, prefix, "", "", 1000)
				if err != nil {
					break
				}
				for _, object := range objectslist.Objects {
					//Check for Expiration in Rule of LifeCycle
					if Rule.Expiration != (lifecycle.Expiration{}) {
						if Rule.Expiration.Date != "" {
							expiry, err := time.Parse(time.RFC1123, Rule.Expiration.Date)
							if err != nil {
								logger.LogIf(ctx, err)
							}
							if object.ModTime.Sub(expiry) < 0 {
								// Delete Object
								objAPI.DeleteObject(ctx, bucketName, object.Name)
							}
						} else {
							if now.Sub(object.ModTime) > time.Hour*24*time.Duration(Rule.Expiration.Days) {
								// Delete Object
								objAPI.DeleteObject(ctx, bucketName, object.Name)
							}
						}

					}

					// Check for Transition Action is Present
					// Transition Action functionality yet to be implemented
					if Rule.Transition != (lifecycle.Transition{}) {
					}
				}
			}
		}
	}
}
