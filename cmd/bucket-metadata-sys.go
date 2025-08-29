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
	"encoding/xml"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/pkg/v3/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

// BucketMetadataSys captures all bucket metadata for a given cluster.
type BucketMetadataSys struct {
	objAPI ObjectLayer

	sync.RWMutex
	initialized bool
	group       *singleflight.Group
	metadataMap map[string]BucketMetadata
}

// Count returns number of bucket metadata map entries.
func (sys *BucketMetadataSys) Count() int {
	sys.RLock()
	defer sys.RUnlock()

	return len(sys.metadataMap)
}

// Remove bucket metadata from memory.
func (sys *BucketMetadataSys) Remove(buckets ...string) {
	sys.Lock()
	for _, bucket := range buckets {
		sys.group.Forget(bucket)
		delete(sys.metadataMap, bucket)
		globalBucketMonitor.DeleteBucket(bucket)
	}
	sys.Unlock()
}

// RemoveStaleBuckets removes all stale buckets in memory that are not on disk.
func (sys *BucketMetadataSys) RemoveStaleBuckets(diskBuckets set.StringSet) {
	sys.Lock()
	defer sys.Unlock()

	for bucket := range sys.metadataMap {
		if diskBuckets.Contains(bucket) {
			continue
		} // doesn't exist on disk remove from memory.
		delete(sys.metadataMap, bucket)
		globalBucketMonitor.DeleteBucket(bucket)
	}
}

// Set - sets a new metadata in-memory.
// Only a shallow copy is saved and fields with references
// cannot be modified without causing a race condition,
// so they should be replaced atomically and not appended to, etc.
// Data is not persisted to disk.
func (sys *BucketMetadataSys) Set(bucket string, meta BucketMetadata) {
	if !isMinioMetaBucketName(bucket) {
		sys.Lock()
		sys.metadataMap[bucket] = meta
		sys.Unlock()
	}
}

func (sys *BucketMetadataSys) updateAndParse(ctx context.Context, bucket string, configFile string, configData []byte, parse bool) (updatedAt time.Time, err error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return updatedAt, errServerNotInitialized
	}

	if isMinioMetaBucketName(bucket) {
		return updatedAt, errInvalidArgument
	}

	meta, err := loadBucketMetadataParse(ctx, objAPI, bucket, parse)
	if err != nil {
		if !globalIsErasure && !globalIsDistErasure && errors.Is(err, errVolumeNotFound) {
			// Only single drive mode needs this fallback.
			meta = newBucketMetadata(bucket)
		} else {
			return updatedAt, err
		}
	}
	updatedAt = UTCNow()
	switch configFile {
	case bucketPolicyConfig:
		meta.PolicyConfigJSON = configData
		meta.PolicyConfigUpdatedAt = updatedAt
	case bucketNotificationConfig:
		meta.NotificationConfigXML = configData
		meta.NotificationConfigUpdatedAt = updatedAt
	case bucketLifecycleConfig:
		meta.LifecycleConfigXML = configData
		meta.LifecycleConfigUpdatedAt = updatedAt
	case bucketSSEConfig:
		meta.EncryptionConfigXML = configData
		meta.EncryptionConfigUpdatedAt = updatedAt
	case bucketTaggingConfig:
		meta.TaggingConfigXML = configData
		meta.TaggingConfigUpdatedAt = updatedAt
	case bucketQuotaConfigFile:
		meta.QuotaConfigJSON = configData
		meta.QuotaConfigUpdatedAt = updatedAt
	case objectLockConfig:
		meta.ObjectLockConfigXML = configData
		meta.ObjectLockConfigUpdatedAt = updatedAt
	case bucketVersioningConfig:
		meta.VersioningConfigXML = configData
		meta.VersioningConfigUpdatedAt = updatedAt
	case bucketReplicationConfig:
		meta.ReplicationConfigXML = configData
		meta.ReplicationConfigUpdatedAt = updatedAt
	case bucketTargetsFile:
		meta.BucketTargetsConfigJSON, meta.BucketTargetsConfigMetaJSON, err = encryptBucketMetadata(ctx, meta.Name, configData, kms.Context{
			bucket:            meta.Name,
			bucketTargetsFile: bucketTargetsFile,
		})
		if err != nil {
			return updatedAt, fmt.Errorf("Error encrypting bucket target metadata %w", err)
		}
		meta.BucketTargetsConfigUpdatedAt = updatedAt
		meta.BucketTargetsConfigMetaUpdatedAt = updatedAt
	default:
		return updatedAt, fmt.Errorf("Unknown bucket %s metadata update requested %s", bucket, configFile)
	}

	return updatedAt, sys.save(ctx, meta)
}

func (sys *BucketMetadataSys) save(ctx context.Context, meta BucketMetadata) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	if isMinioMetaBucketName(meta.Name) {
		return errInvalidArgument
	}

	if err := meta.Save(ctx, objAPI); err != nil {
		return err
	}

	sys.Set(meta.Name, meta)
	globalNotificationSys.LoadBucketMetadata(bgContext(ctx), meta.Name) // Do not use caller context here
	return nil
}

// Delete delete the bucket metadata for the specified bucket.
// must be used by all callers instead of using Update() with nil configData.
func (sys *BucketMetadataSys) Delete(ctx context.Context, bucket string, configFile string) (updatedAt time.Time, err error) {
	if configFile == bucketLifecycleConfig {
		// Get bucket config from current site
		meta, e := globalBucketMetadataSys.GetConfigFromDisk(ctx, bucket)
		if e != nil && !errors.Is(e, errConfigNotFound) {
			return updatedAt, e
		}
		var expiryRuleRemoved bool
		if len(meta.LifecycleConfigXML) > 0 {
			var lcCfg lifecycle.Lifecycle
			if err := xml.Unmarshal(meta.LifecycleConfigXML, &lcCfg); err != nil {
				return updatedAt, err
			}
			// find a single expiry rule set the flag
			for _, rl := range lcCfg.Rules {
				if !rl.Expiration.IsNull() || !rl.NoncurrentVersionExpiration.IsNull() {
					expiryRuleRemoved = true
					break
				}
			}
		}

		// Form empty ILM details with `ExpiryUpdatedAt` field and save
		var cfgData []byte
		if expiryRuleRemoved {
			var lcCfg lifecycle.Lifecycle
			currtime := time.Now()
			lcCfg.ExpiryUpdatedAt = &currtime
			cfgData, err = xml.Marshal(lcCfg)
			if err != nil {
				return updatedAt, err
			}
		}
		return sys.updateAndParse(ctx, bucket, configFile, cfgData, false)
	}
	return sys.updateAndParse(ctx, bucket, configFile, nil, false)
}

// Update update bucket metadata for the specified bucket.
// The configData data should not be modified after being sent here.
func (sys *BucketMetadataSys) Update(ctx context.Context, bucket string, configFile string, configData []byte) (updatedAt time.Time, err error) {
	return sys.updateAndParse(ctx, bucket, configFile, configData, true)
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
	if isMinioMetaBucketName(bucket) {
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
func (sys *BucketMetadataSys) GetVersioningConfig(bucket string) (*versioning.Versioning, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return &versioning.Versioning{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}, meta.Created, nil
		}
		return &versioning.Versioning{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}, time.Time{}, err
	}
	return meta.versioningConfig, meta.VersioningConfigUpdatedAt, nil
}

// GetBucketPolicy returns configured bucket policy
func (sys *BucketMetadataSys) GetBucketPolicy(bucket string) (*policy.BucketPolicy, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	if meta.policyConfig == nil {
		return nil, time.Time{}, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, meta.PolicyConfigUpdatedAt, nil
}

// GetTaggingConfig returns configured tagging config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetTaggingConfig(bucket string) (*tags.Tags, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketTaggingNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	if meta.taggingConfig == nil {
		return nil, time.Time{}, BucketTaggingNotFound{Bucket: bucket}
	}
	return meta.taggingConfig, meta.TaggingConfigUpdatedAt, nil
}

// GetObjectLockConfig returns configured object lock config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetObjectLockConfig(bucket string) (*objectlock.Config, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketObjectLockConfigNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	if meta.objectLockConfig == nil {
		return nil, time.Time{}, BucketObjectLockConfigNotFound{Bucket: bucket}
	}
	return meta.objectLockConfig, meta.ObjectLockConfigUpdatedAt, nil
}

// GetLifecycleConfig returns configured lifecycle config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetLifecycleConfig(bucket string) (*lifecycle.Lifecycle, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketLifecycleNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	// there could be just `ExpiryUpdatedAt` field populated as part
	// of last delete all. Treat this situation as not lifecycle configuration
	// available
	if meta.lifecycleConfig == nil || len(meta.lifecycleConfig.Rules) == 0 {
		return nil, time.Time{}, BucketLifecycleNotFound{Bucket: bucket}
	}
	return meta.lifecycleConfig, meta.LifecycleConfigUpdatedAt, nil
}

// GetNotificationConfig returns configured notification config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetNotificationConfig(bucket string) (*event.Config, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		return nil, err
	}
	return meta.notificationConfig, nil
}

// GetSSEConfig returns configured SSE config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetSSEConfig(bucket string) (*bucketsse.BucketSSEConfig, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketSSEConfigNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	if meta.sseConfig == nil {
		return nil, time.Time{}, BucketSSEConfigNotFound{Bucket: bucket}
	}
	return meta.sseConfig, meta.EncryptionConfigUpdatedAt, nil
}

// CreatedAt returns the time of creation of bucket
func (sys *BucketMetadataSys) CreatedAt(bucket string) (time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		return time.Time{}, err
	}
	return meta.Created.UTC(), nil
}

// GetPolicyConfig returns configured bucket policy
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetPolicyConfig(bucket string) (*policy.BucketPolicy, time.Time, error) {
	meta, _, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketPolicyNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	if meta.policyConfig == nil {
		return nil, time.Time{}, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, meta.PolicyConfigUpdatedAt, nil
}

// GetQuotaConfig returns configured bucket quota
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetQuotaConfig(ctx context.Context, bucket string) (*madmin.BucketQuota, time.Time, error) {
	meta, _, err := sys.GetConfig(ctx, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketQuotaConfigNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}
	return meta.quotaConfig, meta.QuotaConfigUpdatedAt, nil
}

// GetReplicationConfig returns configured bucket replication config
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetReplicationConfig(ctx context.Context, bucket string) (*replication.Config, time.Time, error) {
	meta, reloaded, err := sys.GetConfig(ctx, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, time.Time{}, BucketReplicationConfigNotFound{Bucket: bucket}
		}
		return nil, time.Time{}, err
	}

	if meta.replicationConfig == nil {
		return nil, time.Time{}, BucketReplicationConfigNotFound{Bucket: bucket}
	}
	if reloaded {
		globalBucketTargetSys.set(bucket, meta)
	}
	return meta.replicationConfig, meta.ReplicationConfigUpdatedAt, nil
}

// GetBucketTargetsConfig returns configured bucket targets for this bucket
// The returned object may not be modified.
func (sys *BucketMetadataSys) GetBucketTargetsConfig(bucket string) (*madmin.BucketTargets, error) {
	meta, reloaded, err := sys.GetConfig(GlobalContext, bucket)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, BucketRemoteTargetNotFound{Bucket: bucket}
		}
		return nil, err
	}
	if meta.bucketTargetConfig == nil {
		return nil, BucketRemoteTargetNotFound{Bucket: bucket}
	}
	if reloaded {
		globalBucketTargetSys.set(bucket, meta)
	}
	return meta.bucketTargetConfig, nil
}

// GetConfigFromDisk read bucket metadata config from disk.
func (sys *BucketMetadataSys) GetConfigFromDisk(ctx context.Context, bucket string) (BucketMetadata, error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return newBucketMetadata(bucket), errServerNotInitialized
	}

	if isMinioMetaBucketName(bucket) {
		return newBucketMetadata(bucket), errInvalidArgument
	}

	return loadBucketMetadata(ctx, objAPI, bucket)
}

var errBucketMetadataNotInitialized = errors.New("bucket metadata not initialized yet")

// GetConfig returns a specific configuration from the bucket metadata.
// The returned object may not be modified.
// reloaded will be true if metadata refreshed from disk
func (sys *BucketMetadataSys) GetConfig(ctx context.Context, bucket string) (meta BucketMetadata, reloaded bool, err error) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return newBucketMetadata(bucket), reloaded, errServerNotInitialized
	}

	if isMinioMetaBucketName(bucket) {
		return newBucketMetadata(bucket), reloaded, errInvalidArgument
	}

	sys.RLock()
	meta, ok := sys.metadataMap[bucket]
	sys.RUnlock()
	if ok {
		return meta, reloaded, nil
	}

	val, err, _ := sys.group.Do(bucket, func() (val any, err error) {
		meta, err = loadBucketMetadata(ctx, objAPI, bucket)
		if err != nil {
			if !sys.Initialized() {
				// bucket metadata not yet initialized
				return newBucketMetadata(bucket), errBucketMetadataNotInitialized
			}
		}
		return meta, err
	})
	meta, _ = val.(BucketMetadata)
	if err != nil {
		return meta, false, err
	}
	sys.Lock()
	sys.metadataMap[bucket] = meta
	sys.Unlock()

	return meta, true, nil
}

// Init - initializes bucket metadata system for all buckets.
func (sys *BucketMetadataSys) Init(ctx context.Context, buckets []string, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	sys.objAPI = objAPI

	// Load bucket metadata sys.
	sys.init(ctx, buckets)
	return nil
}

// concurrently load bucket metadata to speed up loading bucket metadata.
func (sys *BucketMetadataSys) concurrentLoad(ctx context.Context, buckets []string) {
	g := errgroup.WithNErrs(len(buckets))
	bucketMetas := make([]BucketMetadata, len(buckets))
	for index := range buckets {
		g.Go(func() error {
			// Sleep and stagger to avoid blocked CPU and thundering
			// herd upon start up sequence.
			time.Sleep(25*time.Millisecond + time.Duration(rand.Int63n(int64(100*time.Millisecond))))

			_, _ = sys.objAPI.HealBucket(ctx, buckets[index], madmin.HealOpts{Recreate: true})
			meta, err := loadBucketMetadata(ctx, sys.objAPI, buckets[index])
			if err != nil {
				return err
			}
			bucketMetas[index] = meta
			return nil
		}, index)
	}

	errs := g.Wait()
	for index, err := range errs {
		if err != nil {
			internalLogOnceIf(ctx, fmt.Errorf("Unable to load bucket metadata, will be retried: %w", err),
				"load-bucket-metadata-"+buckets[index], logger.WarningKind)
		}
	}

	// Hold lock here to update in-memory map at once,
	// instead of serializing the Go routines.
	sys.Lock()
	for i, meta := range bucketMetas {
		if errs[i] != nil {
			continue
		}
		sys.metadataMap[buckets[i]] = meta
	}
	sys.Unlock()

	for i, meta := range bucketMetas {
		if errs[i] != nil {
			continue
		}
		globalEventNotifier.set(buckets[i], meta)   // set notification targets
		globalBucketTargetSys.set(buckets[i], meta) // set remote replication targets
	}
}

func (sys *BucketMetadataSys) refreshBucketsMetadataLoop(ctx context.Context) {
	const bucketMetadataRefresh = 15 * time.Minute

	sleeper := newDynamicSleeper(2, 150*time.Millisecond, false)

	t := time.NewTimer(bucketMetadataRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			buckets, err := sys.objAPI.ListBuckets(ctx, BucketOptions{NoMetadata: true})
			if err != nil {
				internalLogIf(ctx, err, logger.WarningKind)
				break
			}

			// Handle if we have some buckets in-memory those are stale.
			// first delete them and then replace the newer state()
			// from disk.
			diskBuckets := set.CreateStringSet()
			for _, bucket := range buckets {
				diskBuckets.Add(bucket.Name)
			}
			sys.RemoveStaleBuckets(diskBuckets)

			for i := range buckets {
				wait := sleeper.Timer(ctx)

				bucket := buckets[i].Name
				updated := false

				meta, err := loadBucketMetadata(ctx, sys.objAPI, bucket)
				if err != nil {
					internalLogIf(ctx, err, logger.WarningKind)
					wait() // wait to proceed to next entry.
					continue
				}

				sys.Lock()
				// Update if the bucket metadata in the memory is older than on-disk one
				if lu := sys.metadataMap[bucket].lastUpdate(); lu.Before(meta.lastUpdate()) {
					updated = true
					sys.metadataMap[bucket] = meta
				}
				sys.Unlock()

				if updated {
					globalEventNotifier.set(bucket, meta)
					globalBucketTargetSys.set(bucket, meta)
				}

				wait() // wait to proceed to next entry.
			}
		}
		t.Reset(bucketMetadataRefresh)
	}
}

// Initialized indicates if bucket metadata sys is initialized atleast once.
func (sys *BucketMetadataSys) Initialized() bool {
	sys.RLock()
	defer sys.RUnlock()

	return sys.initialized
}

// Loads bucket metadata for all buckets into BucketMetadataSys.
func (sys *BucketMetadataSys) init(ctx context.Context, buckets []string) {
	count := globalEndpoints.ESCount() * 10
	for {
		if len(buckets) < count {
			sys.concurrentLoad(ctx, buckets)
			break
		}
		sys.concurrentLoad(ctx, buckets[:count])
		buckets = buckets[count:]
	}

	sys.Lock()
	sys.initialized = true
	sys.Unlock()

	if globalIsDistErasure {
		go sys.refreshBucketsMetadataLoop(ctx)
	}
}

// Reset the state of the BucketMetadataSys.
func (sys *BucketMetadataSys) Reset() {
	sys.Lock()
	clear(sys.metadataMap)
	sys.Unlock()
}

// NewBucketMetadataSys - creates new policy system.
func NewBucketMetadataSys() *BucketMetadataSys {
	return &BucketMetadataSys{
		metadataMap: make(map[string]BucketMetadata),
		group:       &singleflight.Group{},
	}
}
