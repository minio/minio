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
	"fmt"
	"sync"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/bucket/policy"
)

// BucketMetadataSys captures all bucket metadata for a given cluster.
type BucketMetadataSys struct {
	sync.RWMutex
	metadataMap map[string]BucketMetadata
}

// Remove bucket metadata from memory.
func (sys *BucketMetadataSys) Remove(bucket string) {
	if globalIsGateway {
		return
	}
	sys.Lock()
	delete(sys.metadataMap, bucket)
	globalBucketMonitor.DeleteBucket(bucket)
	sys.Unlock()
}

// Set - sets a new metadata in-memory.
// Only a shallow copy is saved and fields with references
// cannot be modified without causing a race condition,
// so they should be replaced atomically and not appended to, etc.
// Data is not persisted to disk.
func (sys *BucketMetadataSys) Set(bucket string, meta BucketMetadata) {
	if globalIsGateway {
		return
	}

	if bucket != minioMetaBucket {
		sys.Lock()
		sys.metadataMap[bucket] = meta
		sys.Unlock()
	}
}

// Update update bucket metadata for the specified config file.
// The configData data should not be modified after being sent here.
func (sys *BucketMetadataSys) Update(bucket string, configFile string, configData []byte) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	if globalIsGateway {
		// This code is needed only for gateway implementations.
		switch configFile {
		case bucketSSEConfig:
			if globalGatewayName == NASBackendGateway {
				meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
				if err != nil {
					return err
				}
				meta.EncryptionConfigXML = configData
				return meta.Save(GlobalContext, objAPI)
			}
		case bucketLifecycleConfig:
			if globalGatewayName == NASBackendGateway {
				meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
				if err != nil {
					return err
				}
				meta.LifecycleConfigXML = configData
				return meta.Save(GlobalContext, objAPI)
			}
		case bucketTaggingConfig:
			if globalGatewayName == NASBackendGateway {
				meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
				if err != nil {
					return err
				}
				meta.TaggingConfigXML = configData
				return meta.Save(GlobalContext, objAPI)
			}
		case bucketNotificationConfig:
			if globalGatewayName == NASBackendGateway {
				meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
				if err != nil {
					return err
				}
				meta.NotificationConfigXML = configData
				return meta.Save(GlobalContext, objAPI)
			}
		case bucketPolicyConfig:
			if configData == nil {
				return objAPI.DeleteBucketPolicy(GlobalContext, bucket)
			}
			config, err := policy.ParseConfig(bytes.NewReader(configData), bucket)
			if err != nil {
				return err
			}
			return objAPI.SetBucketPolicy(GlobalContext, bucket, config)
		}
		return NotImplemented{}
	}

	if bucket == minioMetaBucket {
		return errInvalidArgument
	}

	meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
	if err != nil {
		return err
	}

	switch configFile {
	case bucketPolicyConfig:
		meta.PolicyConfigJSON = configData
	case bucketNotificationConfig:
		meta.NotificationConfigXML = configData
	case bucketLifecycleConfig:
		meta.LifecycleConfigXML = configData
	case bucketSSEConfig:
		meta.EncryptionConfigXML = configData
	case bucketTaggingConfig:
		meta.TaggingConfigXML = configData
	case bucketQuotaConfigFile:
		meta.QuotaConfigJSON = configData
	case objectLockConfig:
		if !globalIsErasure && !globalIsDistErasure {
			return NotImplemented{}
		}
		meta.ObjectLockConfigXML = configData
	case bucketVersioningConfig:
		if !globalIsErasure && !globalIsDistErasure {
			return NotImplemented{}
		}
		meta.VersioningConfigXML = configData
	case bucketReplicationConfig:
		if !globalIsErasure && !globalIsDistErasure {
			return NotImplemented{}
		}
		meta.ReplicationConfigXML = configData
	case bucketTargetsFile:
		meta.BucketTargetsConfigJSON, meta.BucketTargetsConfigMetaJSON, err = encryptBucketMetadata(meta.Name, configData, kms.Context{
			bucket:            meta.Name,
			bucketTargetsFile: bucketTargetsFile,
		})
		if err != nil {
			return fmt.Errorf("Error encrypting bucket target metadata %w", err)
		}
	default:
		return fmt.Errorf("Unknown bucket %s metadata update requested %s", bucket, configFile)
	}

	if err := meta.Save(GlobalContext, objAPI); err != nil {
		return err
	}

	sys.Set(bucket, meta)
	globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

	return nil
}

// Get metadata for a bucket.
// If no metadata exists errConfigNotFound is returned and a new metadata is returned.
// Only a shallow copy is returned, so referenced data should not be modified,
// but can be replaced atomically.
//
// This function should only be used with
// - GetBucketInfo
// - ListBuckets
// For all other bucket specific metadata, use the relevant
// calls implemented specifically for each of those features.
func (sys *BucketMetadataSys) Get(bucket string) (BucketMetadata, error) {
	if globalIsGateway || bucket == minioMetaBucket {
		return newBucketMetadata(bucket), errConfigNotFound
	}

	sys.RLock()
	defer sys.RUnlock()

	meta, ok := sys.metadataMap[bucket]
	if !ok {
		return newBucketMetadata(bucket), errConfigNotFound
	}

	return meta, nil
}

// GetVersioningConfig returns configured versioning config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetVersioningConfig(bucket string) (*versioning.Versioning, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		return nil, err
	}
	return meta.versioningConfig, nil
}

// GetTaggingConfig returns configured tagging config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetTaggingConfig(bucket string) (*tags.Tags, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketTaggingNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.taggingConfig == nil {
		return nil, BucketTaggingNotFound{Bucket: bucket}
	}
	return meta.taggingConfig, nil
}

// GetObjectLockConfig returns configured object lock config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetObjectLockConfig(bucket string) (*objectlock.Config, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketObjectLockConfigNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.objectLockConfig == nil {
		return nil, BucketObjectLockConfigNotFound{Bucket: bucket}
	}
	return meta.objectLockConfig, nil
}

// GetLifecycleConfig returns configured lifecycle config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetLifecycleConfig(bucket string) (*lifecycle.Lifecycle, error) {
	if globalIsGateway && globalGatewayName == NASBackendGateway {
		// Only needed in case of NAS gateway.
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
		if err != nil {
			return nil, err
		}
		if meta.lifecycleConfig == nil {
			return nil, BucketLifecycleNotFound{Bucket: bucket}
		}
		return meta.lifecycleConfig, nil
	}

	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketLifecycleNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.lifecycleConfig == nil {
		return nil, BucketLifecycleNotFound{Bucket: bucket}
	}
	return meta.lifecycleConfig, nil
}

// GetNotificationConfig returns configured notification config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetNotificationConfig(bucket string) (*event.Config, error) {
	if globalIsGateway && globalGatewayName == NASBackendGateway {
		// Only needed in case of NAS gateway.
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
		if err != nil {
			return nil, err
		}
		return meta.notificationConfig, nil
	}

	meta, err := sys.GetConfig(bucket)
	if err != nil {
		return nil, err
	}
	return meta.notificationConfig, nil
}

// GetSSEConfig returns configured SSE config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetSSEConfig(bucket string) (*bucketsse.BucketSSEConfig, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketSSEConfigNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.sseConfig == nil {
		return nil, BucketSSEConfigNotFound{Bucket: bucket}
	}
	return meta.sseConfig, nil
}

// GetPolicyConfig returns configured bucket policy
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetPolicyConfig(bucket string) (*policy.Policy, error) {
	if globalIsGateway {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}
		return objAPI.GetBucketPolicy(GlobalContext, bucket)
	}

	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.policyConfig == nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, nil
}

// GetQuotaConfig returns configured bucket quota
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetQuotaConfig(bucket string) (*madmin.BucketQuota, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		return nil, err
	}
	return meta.quotaConfig, nil
}

// GetReplicationConfig returns configured bucket replication config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetReplicationConfig(ctx context.Context, bucket string) (*replication.Config, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketReplicationConfigNotFound{Bucket: bucket}
		}
		return nil, err
	}

	if meta.replicationConfig == nil {
		return nil, BucketReplicationConfigNotFound{Bucket: bucket}
	}
	return meta.replicationConfig, nil
}

// GetBucketTargetsConfig returns configured bucket targets for this bucket
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetBucketTargetsConfig(bucket string) (*madmin.BucketTargets, error) {
	meta, err := sys.GetConfig(bucket)
	if err != nil {
		return nil, err
	}
	if meta.bucketTargetConfig == nil {
		return nil, BucketRemoteTargetNotFound{Bucket: bucket}
	}
	return meta.bucketTargetConfig, nil
}

// GetBucketTarget returns the target for the bucket and arn.
func (sys *BucketMetadataSys) GetBucketTarget(bucket string, arn string) (madmin.BucketTarget, error) {
	targets, err := sys.GetBucketTargetsConfig(bucket)
	if err != nil {
		return madmin.BucketTarget{}, err
	}
	for _, t := range targets.Targets {
		if t.Arn == arn {
			return t, nil
		}
	}
	return madmin.BucketTarget{}, errConfigNotFound
}

// GetConfig returns a specific configuration from the bucket metadata.
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetConfig(bucket string) (BucketMetadata, error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return newBucketMetadata(bucket), errServerNotInitialized
	}

	if globalIsGateway {
		return newBucketMetadata(bucket), NotImplemented{}
	}

	if bucket == minioMetaBucket {
		return newBucketMetadata(bucket), errInvalidArgument
	}

	sys.RLock()
	meta, ok := sys.metadataMap[bucket]
	sys.RUnlock()
	if ok {
		return meta, nil
	}
	meta, err := loadBucketMetadata(GlobalContext, objAPI, bucket)
	if err != nil {
		return meta, err
	}
	sys.Lock()
	sys.metadataMap[bucket] = meta
	sys.Unlock()

	return meta, nil
}

// Init - initializes bucket metadata system for all buckets.
func (sys *BucketMetadataSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, we don't need to load the policies
	// from the backend.
	if globalIsGateway {
		return nil
	}

	// Load bucket metadata sys in background
	go sys.load(ctx, buckets, objAPI)
	return nil
}

// concurrently load bucket metadata to speed up loading bucket metadata.
func (sys *BucketMetadataSys) concurrentLoad(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	g := errgroup.WithNErrs(len(buckets))
	for index := range buckets {
		index := index
		g.Go(func() error {
			_, _ = objAPI.HealBucket(ctx, buckets[index].Name, madmin.HealOpts{
				// Ensure heal opts for bucket metadata be deep healed all the time.
				ScanMode: madmin.HealDeepScan,
			})
			meta, err := loadBucketMetadata(ctx, objAPI, buckets[index].Name)
			if err != nil {
				return err
			}
			sys.Lock()
			sys.metadataMap[buckets[index].Name] = meta
			sys.Unlock()
			return nil
		}, index)
	}
	for _, err := range g.Wait() {
		if err != nil {
			logger.LogIf(ctx, err)
		}
	}
}

// Loads bucket metadata for all buckets into BucketMetadataSys.
func (sys *BucketMetadataSys) load(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) {
	count := 100 // load 100 bucket metadata at a time.
	for {
		if len(buckets) < count {
			sys.concurrentLoad(ctx, buckets, objAPI)
			return
		}
		sys.concurrentLoad(ctx, buckets[:count], objAPI)
		buckets = buckets[count:]
	}
}

// Reset the state of the BucketMetadataSys.
func (sys *BucketMetadataSys) Reset() {
	sys.Lock()
	for k := range sys.metadataMap {
		delete(sys.metadataMap, k)
	}
	sys.Unlock()
}

// NewBucketMetadataSys - creates new policy system.
func NewBucketMetadataSys() *BucketMetadataSys {
	return &BucketMetadataSys{
		metadataMap: make(map[string]BucketMetadata),
	}
}
