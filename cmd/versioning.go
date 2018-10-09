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
	"context"
	"encoding/xml"
	"io/ioutil"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
)

// VersioningSys - versioning subsystem.
type VersioningSys struct {
	sync.RWMutex
	bucketVersioningMap map[string]VersioningConfiguration
}

// removeDeletedBuckets - to handle a corner case where we have cached the versioning config for a deleted
// bucket. i.e if we miss a delete-bucket notification we should delete the corresponding
// bucket versioning during sys.refresh()
func (sys *VersioningSys) removeDeletedBuckets(bucketInfos []BucketInfo) {
	buckets := set.NewStringSet()
	for _, info := range bucketInfos {
		buckets.Add(info.Name)
	}
	sys.Lock()
	defer sys.Unlock()

	for bucket := range sys.bucketVersioningMap {
		if !buckets.Contains(bucket) {
			delete(sys.bucketVersioningMap, bucket)
		}
	}
}

// Set - sets versioning to given bucket name.
func (sys *VersioningSys) Set(bucketName string, versioning VersioningConfiguration) {
	sys.Lock()
	defer sys.Unlock()

	sys.bucketVersioningMap[bucketName] = versioning
}

// Remove - removes versioning configuration for given bucket name.
func (sys *VersioningSys) Remove(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketVersioningMap, bucketName)
}

// IsEnabled - checks given bucket has versioning enabled
func (sys *VersioningSys) IsEnabled(bucketName string) bool {
	sys.RLock()
	defer sys.RUnlock()

	// If policy is available for given bucket, check the policy.
	if p, found := sys.bucketVersioningMap[bucketName]; found {
		return strings.ToLower(p.Status) == "enabled"
	}

	return false
}

// Refresh VersioningSys.
func (sys *VersioningSys) refresh(objAPI ObjectLayer) error {

	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	sys.removeDeletedBuckets(buckets)
	for _, bucket := range buckets {
		config, err := objAPI.GetBucketVersioning(context.Background(), bucket.Name)
		if err != nil {
			if _, ok := err.(BucketVersioningNotFound); ok {
				sys.Remove(bucket.Name)
			}
			continue
		}
		sys.Set(bucket.Name, *config)
	}
	return nil
}

// Init - initializes versioning system from versioning.json for all buckets.
func (sys *VersioningSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	// Load VersioningSys once during boot.
	if err := sys.refresh(objAPI); err != nil {
		return err
	}

	// Refresh VersioningSys in background.
	go func() {
		ticker := time.NewTicker(globalRefreshBucketVersioningInterval)
		defer ticker.Stop()
		for {
			select {
			case <-globalServiceDoneCh:
				return
			case <-ticker.C:
				sys.refresh(objAPI)
			}
		}
	}()
	return nil
}

// NewVersioningSys - creates new versioning system.
func NewVersioningSys() *VersioningSys {
	return &VersioningSys{
		bucketVersioningMap: make(map[string]VersioningConfiguration),
	}
}

// getVersioningConfig - get versioning config for given bucket name.
func getVersioningConfig(objAPI ObjectLayer, bucketName string) (*VersioningConfiguration, error) {
	// Construct path to versioning.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketVersioningConfig)

	reader, err := readConfig(context.Background(), objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			return &VersioningConfiguration{
				XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
			}, BucketVersioningNotFound{Bucket: bucketName}
		}
		return nil, err
	}

	var versioningConfig VersioningConfiguration
	if versioningConfigBytes, err := ioutil.ReadAll(reader); err != nil {
		return nil, err
	} else if err = xml.Unmarshal(versioningConfigBytes, &versioningConfig); err != nil {
		return nil, err
	}
	return &versioningConfig, nil
}

func saveVersioningConfig(objAPI ObjectLayer, bucketName string, versioning VersioningConfiguration) error {
	data, err := xml.Marshal(versioning)
	if err != nil {
		return err
	}

	// Construct path to versioning.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketVersioningConfig)

	return saveConfig(objAPI, configFile, data)
}

func removeVersioningConfig(ctx context.Context, objAPI ObjectLayer, bucketName string) error {
	// Construct path to versioning.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketVersioningConfig)

	if _, err := objAPI.DeleteObject(ctx, minioMetaBucket, configFile); err != nil {
		if _, ok := err.(ObjectNotFound); ok {
			return BucketVersioningNotFound{Bucket: bucketName}
		}
		return err
	}

	return nil
}
