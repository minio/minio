/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/probe"
)

/// Bucket Operations

// DeleteBucket - delete a bucket.
func (fs Filesystem) DeleteBucket(bucket string) *probe.Error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = fs.denormalizeBucket(bucket)
	bucketDir := filepath.Join(fs.path, bucket)
	if e := os.Remove(bucketDir); e != nil {
		// Error if there was no bucket in the first place.
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		// On windows the string is slightly different, handle it here.
		if strings.Contains(e.Error(), "directory is not empty") {
			return probe.NewError(BucketNotEmpty{Bucket: bucket})
		}
		// Hopefully for all other operating systems, this is
		// assumed to be consistent.
		if strings.Contains(e.Error(), "directory not empty") {
			return probe.NewError(BucketNotEmpty{Bucket: bucket})
		}
		return probe.NewError(e)
	}

	// Critical region hold write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	delete(fs.buckets.Metadata, bucket)
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}

// ListBuckets - Get service.
func (fs Filesystem) ListBuckets() ([]BucketMetadata, *probe.Error) {
	files, e := ioutil.ReadDir(fs.path)
	if e != nil {
		return []BucketMetadata{}, probe.NewError(e)
	}
	var metadataList []BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			// If not directory, ignore all file types.
			continue
		}
		// If directories are found with odd names, skip them.
		dirName := strings.ToLower(file.Name())
		if !IsValidBucketName(dirName) {
			continue
		}
		metadata := BucketMetadata{
			Name:    dirName,
			Created: file.ModTime(),
		}
		metadataList = append(metadataList, metadata)
	}
	// Remove duplicated entries.
	metadataList = removeDuplicateBuckets(metadataList)
	return metadataList, nil
}

// removeDuplicateBuckets - remove duplicate buckets.
func removeDuplicateBuckets(buckets []BucketMetadata) []BucketMetadata {
	length := len(buckets) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if buckets[i].Name == buckets[j].Name {
				if buckets[i].Created.Sub(buckets[j].Created) > 0 {
					buckets[i] = buckets[length]
				} else {
					buckets[j] = buckets[length]
				}
				buckets = buckets[0:length]
				length--
				j--
			}
		}
	}
	return buckets
}

// MakeBucket - PUT Bucket.
func (fs Filesystem) MakeBucket(bucket, acl string) *probe.Error {
	di, err := disk.GetInfo(fs.path)
	if err != nil {
		return probe.NewError(err)
	}

	// Remove 5% from total space for cumulative disk space used for
	// journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return probe.NewError(RootPathFull{Path: fs.path})
	}

	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify if bucket acl is valid.
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}

	// Get bucket path.
	bucket = fs.denormalizeBucket(bucket)
	bucketDir := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketDir); e == nil {
		return probe.NewError(BucketExists{Bucket: bucket})
	}

	// Make bucket.
	if e := os.Mkdir(bucketDir, 0700); e != nil {
		return probe.NewError(err)
	}

	fi, e := os.Stat(bucketDir)
	// Check if bucket exists.
	if e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	if acl == "" {
		acl = "private"
	}

	// Get a new bucket name metadata.
	bucketMetadata := &BucketMetadata{}
	bucketMetadata.Name = fi.Name()
	bucketMetadata.Created = fi.ModTime()
	bucketMetadata.ACL = BucketACL(acl)

	// Critical region hold a write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	fs.buckets.Metadata[bucket] = bucketMetadata
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}

// denormalizeBucket - will convert incoming bucket names to
// corresponding valid bucketnames on the backend in a platform
// compatible way for all operating systems.
func (fs Filesystem) denormalizeBucket(bucket string) string {
	buckets, e := ioutil.ReadDir(fs.path)
	if e != nil {
		return bucket
	}
	for _, b := range buckets {
		// Verify if lowercase version of the bucket is equal to the
		// incoming bucket, then use the proper name.
		if strings.ToLower(b.Name()) == bucket {
			return b.Name()
		}
	}
	return bucket
}

// GetBucketMetadata - get bucket metadata.
func (fs Filesystem) GetBucketMetadata(bucket string) (BucketMetadata, *probe.Error) {
	if !IsValidBucketName(bucket) {
		return BucketMetadata{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	// Get bucket path.
	bucketDir := filepath.Join(fs.path, bucket)
	fi, e := os.Stat(bucketDir)
	if e != nil {
		// Check if bucket exists.
		if os.IsNotExist(e) {
			return BucketMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return BucketMetadata{}, probe.NewError(e)
	}
	fs.rwLock.RLock()
	bucketMetadata, ok := fs.buckets.Metadata[bucket]
	fs.rwLock.RUnlock()
	// If metadata value is not found, get it from disk.
	if !ok {
		bucketMetadata = &BucketMetadata{}
		bucketMetadata.Name = fi.Name()
		bucketMetadata.Created = fi.ModTime()
		bucketMetadata.ACL = BucketACL("private")
	}
	return *bucketMetadata, nil
}

// SetBucketMetadata - set bucket metadata.
func (fs Filesystem) SetBucketMetadata(bucket string, metadata map[string]string) *probe.Error {
	bucketMetadata, err := fs.GetBucketMetadata(bucket)
	if err != nil {
		return err
	}

	// Save the acl.
	acl := metadata["acl"]
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	} else if acl == "" {
		acl = "private"
	}
	bucketMetadata.ACL = BucketACL(acl)

	bucket = fs.denormalizeBucket(bucket)

	// Critical region handle write lock.
	fs.rwLock.Lock()
	defer fs.rwLock.Unlock()

	fs.buckets.Metadata[bucket] = &bucketMetadata
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}
