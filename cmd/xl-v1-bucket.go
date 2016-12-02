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
	"sort"
	"sync"
)

// list all errors that can be ignore in a bucket operation.
var bucketOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// list all errors that can be ignored in a bucket metadata operation.
var bucketMetadataOpIgnoredErrs = append(bucketOpIgnoredErrs, errVolumeNotFound)

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}

	bucketLock := nsMutex.NewNSLock(bucket, "")
	bucketLock.Lock()
	defer bucketLock.Unlock()

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			dErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.MakeVol(bucket)
			if err != nil {
				dErrs[index] = traceError(err)
			}
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	// Do we have write quorum?.
	if !isDiskQuorum(dErrs, xl.writeQuorum) {
		// Purge successfully created buckets if we don't have writeQuorum.
		undoMakeBucket(xl.storageDisks, bucket)
		return toObjectErr(traceError(errXLWriteQuorum), bucket)
	}

	// Verify we have any other errors which should undo make bucket.
	if reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, xl.writeQuorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket)
	}
	return nil
}

func (xl xlObjects) undoDeleteBucket(bucket string) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}
	// Undo previous make bucket entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
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
func (xl xlObjects) getBucketInfo(bucketName string) (bucketInfo BucketInfo, err error) {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		var volInfo VolInfo
		volInfo, err = disk.StatVol(bucketName)
		if err == nil {
			bucketInfo = BucketInfo{
				Name:    volInfo.Name,
				Created: volInfo.Created,
			}
			return bucketInfo, nil
		}
		err = traceError(err)
		// For any reason disk went offline continue and pick the next one.
		if isErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return BucketInfo{}, err
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (xl xlObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}

	bucketLock := nsMutex.NewNSLock(bucket, "")
	bucketLock.RLock()
	defer bucketLock.RUnlock()

	bucketInfo, err := xl.getBucketInfo(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}
	return bucketInfo, nil
}

// listBuckets - returns list of all buckets from a disk picked at random.
func (xl xlObjects) listBuckets() (bucketsInfo []BucketInfo, err error) {
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
				// StorageAPI can send volume names which are incompatible
				// with buckets, handle it and skip them.
				if !IsValidBucketName(volInfo.Name) {
					continue
				}
				// Ignore the volume special bucket.
				if volInfo.Name == minioMetaBucket {
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
		// Ignore any disks not found.
		if isErrIgnored(err, bucketMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return nil, err
}

// ListBuckets - lists all the buckets, sorted by its name.
func (xl xlObjects) ListBuckets() ([]BucketInfo, error) {
	bucketInfos, err := xl.listBuckets()
	if err != nil {
		return nil, toObjectErr(err)
	}
	// Sort by bucket name before returning.
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// DeleteBucket - deletes a bucket.
func (xl xlObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	bucketLock := nsMutex.NewNSLock(bucket, "")
	bucketLock.Lock()
	defer bucketLock.Unlock()

	// Collect if all disks report volume not found.
	var wg = &sync.WaitGroup{}
	var dErrs = make([]error, len(xl.storageDisks))

	// Remove a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			dErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Delete volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Attempt to delete bucket.
			err := disk.DeleteVol(bucket)
			if err != nil {
				dErrs[index] = traceError(err)
				return
			}
			// Cleanup all the previously incomplete multiparts.
			err = cleanupDir(disk, minioMetaMultipartBucket, bucket)
			if err != nil {
				if errorCause(err) == errVolumeNotFound {
					return
				}
				dErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all the delete vols to finish.
	wg.Wait()

	if !isDiskQuorum(dErrs, xl.writeQuorum) {
		xl.undoDeleteBucket(bucket)
		return toObjectErr(traceError(errXLWriteQuorum), bucket)
	}

	if reducedErr := reduceWriteQuorumErrs(dErrs, bucketOpIgnoredErrs, xl.writeQuorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket)
	}

	// Success.
	return nil
}
