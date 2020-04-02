/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"sort"

	"github.com/minio/minio-go/v6/pkg/s3utils"
	"github.com/minio/minio/cmd/logger"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/policy"

	"github.com/minio/minio/pkg/sync/errgroup"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// Verify if bucket is valid.
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	storageDisks := xl.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				if err := storageDisks[index].MakeVol(bucket); err != nil {
					if err != errVolumeExists {
						logger.LogIf(ctx, err)
					}
					return err
				}
				return nil
			}
			return errDiskNotFound
		}, index)
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	err := reduceWriteQuorumErrs(ctx, g.Wait(), bucketOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(storageDisks, bucket)
	}
	return toObjectErr(err, bucket)
}

func undoDeleteBucket(storageDisks []StorageAPI, bucket string) {
	g := errgroup.WithNErrs(len(storageDisks))
	// Undo previous make bucket entry on all underlying storage disks.
	for index := range storageDisks {
		if storageDisks[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].MakeVol(bucket)
			return nil
		}, index)
	}

	// Wait for all make vol to finish.
	g.Wait()
}

// undo make bucket operation upon quorum failure.
func undoMakeBucket(storageDisks []StorageAPI, bucket string) {
	g := errgroup.WithNErrs(len(storageDisks))
	// Undo previous make bucket entry on all underlying storage disks.
	for index := range storageDisks {
		if storageDisks[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].DeleteVol(bucket, false)
			return nil
		}, index)
	}

	// Wait for all make vol to finish.
	g.Wait()
}

// getBucketInfo - returns the BucketInfo from one of the load balanced disks.
func (xl xlObjects) getBucketInfo(ctx context.Context, bucketName string) (bucketInfo BucketInfo, err error) {
	var bucketErrs []error
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			bucketErrs = append(bucketErrs, errDiskNotFound)
			continue
		}
		volInfo, serr := disk.StatVol(bucketName)
		if serr == nil {
			return BucketInfo(volInfo), nil
		}
		err = serr
		// For any reason disk went offline continue and pick the next one.
		if IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			bucketErrs = append(bucketErrs, err)
			continue
		}
		// Any error which cannot be ignored, we return quickly.
		return BucketInfo{}, err
	}
	// If all our errors were ignored, then we try to
	// reduce to one error based on read quorum.
	// `nil` is deliberately passed for ignoredErrs
	// because these errors were already ignored.
	readQuorum := getReadQuorum(len(xl.getDisks()))
	return BucketInfo{}, reduceReadQuorumErrs(ctx, bucketErrs, nil, readQuorum)
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (xl xlObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	bucketInfo, err := xl.getBucketInfo(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}
	return bucketInfo, nil
}

// listBuckets - returns list of all buckets from a disk picked at random.
func (xl xlObjects) listBuckets(ctx context.Context) (bucketsInfo []BucketInfo, err error) {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		var volsInfo []VolInfo
		volsInfo, err = disk.ListVols()
		if err == nil {
			// NOTE: The assumption here is that volumes across all disks in
			// readQuorum have consistent view i.e they all have same number
			// of buckets. This is essentially not verified since healing
			// should take care of this.
			var bucketsInfo []BucketInfo
			for _, volInfo := range volsInfo {
				if isReservedOrInvalidBucket(volInfo.Name, true) {
					continue
				}
				bucketsInfo = append(bucketsInfo, BucketInfo(volInfo))
			}
			// For buckets info empty, loop once again to check
			// if we have, can happen if disks were down.
			if len(bucketsInfo) == 0 {
				continue
			}
			return bucketsInfo, nil
		}
		logger.LogIf(ctx, err)
		// Ignore any disks not found.
		if IsErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return nil, err
}

// ListBuckets - lists all the buckets, sorted by its name.
func (xl xlObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	bucketInfos, err := xl.listBuckets(ctx)
	if err != nil {
		return nil, toObjectErr(err)
	}
	// Sort by bucket name before returning.
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// Dangling buckets should be handled appropriately, in this following situation
// we actually have quorum error to be `nil` but we have some disks where
// the bucket delete returned `errVolumeNotEmpty` but this is not correct
// can only happen if there are dangling objects in a bucket. Under such
// a situation we simply attempt a full delete of the bucket including
// the dangling objects. All of this happens under a lock and there
// is no way a user can create buckets and sneak in objects into namespace,
// so it is safer to do.
func deleteDanglingBucket(ctx context.Context, storageDisks []StorageAPI, dErrs []error, bucket string) {
	for index, err := range dErrs {
		if err == errVolumeNotEmpty {
			// Attempt to delete bucket again.
			if derr := storageDisks[index].DeleteVol(bucket, false); derr == errVolumeNotEmpty {
				_ = cleanupDir(ctx, storageDisks[index], bucket, "")

				_ = storageDisks[index].DeleteVol(bucket, false)

				// Cleanup all the previously incomplete multiparts.
				_ = cleanupDir(ctx, storageDisks[index], minioMetaMultipartBucket, bucket)
			}
		}
	}
}

// DeleteBucket - deletes a bucket.
func (xl xlObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	// Collect if all disks report volume not found.
	storageDisks := xl.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] != nil {
				if err := storageDisks[index].DeleteVol(bucket, forceDelete); err != nil {
					return err
				}
				err := cleanupDir(ctx, storageDisks[index], minioMetaMultipartBucket, bucket)
				if err != nil && err != errVolumeNotFound {
					return err
				}
				return nil
			}
			return errDiskNotFound
		}, index)
	}

	// Wait for all the delete vols to finish.
	dErrs := g.Wait()

	if forceDelete {
		for _, err := range dErrs {
			if err != nil {
				undoDeleteBucket(storageDisks, bucket)
				return toObjectErr(err, bucket)
			}
		}

		return nil
	}

	writeQuorum := getWriteQuorum(len(storageDisks))
	err := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		undoDeleteBucket(storageDisks, bucket)
	}
	if err != nil {
		return toObjectErr(err, bucket)
	}

	// If we reduce quorum to nil, means we have deleted buckets properly
	// on some servers in quorum, we should look for volumeNotEmpty errors
	// and delete those buckets as well.
	deleteDanglingBucket(ctx, storageDisks, dErrs, bucket)

	return nil
}

// SetBucketPolicy sets policy on bucket
func (xl xlObjects) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return savePolicyConfig(ctx, xl, bucket, policy)
}

// GetBucketPolicy will get policy on bucket
func (xl xlObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return getPolicyConfig(xl, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (xl xlObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, xl, bucket)
}

// SetBucketLifecycle sets lifecycle on bucket
func (xl xlObjects) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	return saveLifecycleConfig(ctx, xl, bucket, lifecycle)
}

// GetBucketLifecycle will get lifecycle on bucket
func (xl xlObjects) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return getLifecycleConfig(xl, bucket)
}

// DeleteBucketLifecycle deletes all lifecycle on bucket
func (xl xlObjects) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return removeLifecycleConfig(ctx, xl, bucket)
}

// GetBucketSSEConfig returns bucket encryption config on given bucket
func (xl xlObjects) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return getBucketSSEConfig(xl, bucket)
}

// SetBucketSSEConfig sets bucket encryption config on given bucket
func (xl xlObjects) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return saveBucketSSEConfig(ctx, xl, bucket, config)
}

// DeleteBucketSSEConfig deletes bucket encryption config on given bucket
func (xl xlObjects) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return removeBucketSSEConfig(ctx, xl, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (xl xlObjects) IsNotificationSupported() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (xl xlObjects) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (xl xlObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (xl xlObjects) IsCompressionSupported() bool {
	return true
}
