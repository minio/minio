/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/policy"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		logger.LogIf(ctx, BucketNameInvalid{Bucket: bucket})
		return BucketNameInvalid{Bucket: bucket}
	}

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.getDisks()))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range xl.getDisks() {
		if disk == nil {
			dErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.MakeVol(bucket)
			if err != nil {
				if err != errVolumeExists {
					logger.LogIf(ctx, err)
				}
				dErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	writeQuorum := len(xl.getDisks())/2 + 1
	err := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(xl.getDisks(), bucket)
	}
	return toObjectErr(err, bucket)
}

func (xl xlObjects) undoDeleteBucket(bucket string) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}
	// Undo previous make bucket entry on all underlying storage disks.
	for index, disk := range xl.getDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		// Delete a bucket inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_ = disk.MakeVol(bucket)
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()
}

// undo make bucket operation upon quorum failure.
func undoMakeBucket(storageDisks []StorageAPI, bucket string) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}
	// Undo previous make bucket entry on all underlying storage disks.
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		// Delete a bucket inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_ = disk.DeleteVol(bucket)
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()
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
			bucketInfo = BucketInfo{
				Name:    volInfo.Name,
				Created: volInfo.Created,
			}
			return bucketInfo, nil
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
	readQuorum := len(xl.getDisks()) / 2
	return BucketInfo{}, reduceReadQuorumErrs(ctx, bucketErrs, nil, readQuorum)
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (xl xlObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	bucketLock := xl.nsMutex.NewNSLock(bucket, "")
	if e := bucketLock.GetRLock(globalObjectTimeout); e != nil {
		return bi, e
	}
	defer bucketLock.RUnlock()
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
				if isReservedOrInvalidBucket(volInfo.Name) {
					continue
				}
				bucketsInfo = append(bucketsInfo, BucketInfo{
					Name:    volInfo.Name,
					Created: volInfo.Created,
				})
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

// DeleteBucket - deletes a bucket.
func (xl xlObjects) DeleteBucket(ctx context.Context, bucket string) error {
	bucketLock := xl.nsMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer bucketLock.Unlock()

	// Collect if all disks report volume not found.
	var wg = &sync.WaitGroup{}
	var dErrs = make([]error, len(xl.getDisks()))

	// Remove a volume entry on all underlying storage disks.
	for index, disk := range xl.getDisks() {
		if disk == nil {
			dErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Delete volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Attempt to delete bucket.
			err := disk.DeleteVol(bucket)

			if err != nil {
				dErrs[index] = err
				return
			}
			// Cleanup all the previously incomplete multiparts.
			err = cleanupDir(ctx, disk, minioMetaMultipartBucket, bucket)

			if err != nil {
				if err == errVolumeNotFound {
					return
				}
				dErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all the delete vols to finish.
	wg.Wait()

	writeQuorum := len(xl.getDisks())/2 + 1
	err := reduceWriteQuorumErrs(ctx, dErrs, bucketOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		xl.undoDeleteBucket(bucket)
	}
	if err != nil {
		return toObjectErr(err, bucket)
	}

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

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (xl xlObjects) IsNotificationSupported() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (xl xlObjects) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (xl xlObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (xl xlObjects) IsCompressionSupported() bool {
	return true
}
