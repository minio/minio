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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/ioutils"
)

/// Bucket Operations

// DeleteBucket - delete bucket
func (fs Filesystem) DeleteBucket(bucket string) *probe.Error {
	// verify bucket path legal
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = fs.denormalizeBucket(bucket)
	bucketDir := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, e := os.Stat(bucketDir); e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	if e := os.Remove(bucketDir); e != nil {
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
	fs.rwLock.Lock()
	delete(fs.buckets.Metadata, bucket)
	fs.rwLock.Unlock()
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}

// ListBuckets - Get service.
func (fs Filesystem) ListBuckets() ([]BucketMetadata, *probe.Error) {
	files, err := ioutils.ReadDirN(fs.path, fs.maxBuckets)
	if err != nil && err != io.EOF {
		return []BucketMetadata{}, probe.NewError(err)
	}
	if err == io.EOF {
		// This message is printed if there are more than 1000 buckets.
		fmt.Printf("More buckets found, truncating the bucket list to %d entries only.", fs.maxBuckets)
	}
	var metadataList []BucketMetadata
	for _, file := range files {
		if !file.IsDir() {
			// if files found ignore them
			continue
		}
		dirName := strings.ToLower(file.Name())
		if file.IsDir() {
			// If directories found with odd names, skip them.
			if !IsValidBucketName(dirName) {
				continue
			}
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
func removeDuplicateBuckets(elements []BucketMetadata) (result []BucketMetadata) {
	// Use map to record duplicates as we find them.
	duplicates := make(map[string]struct{})
	for _, element := range elements {
		if _, ok := duplicates[element.Name]; !ok {
			duplicates[element.Name] = struct{}{}
			result = append(result, element)
		}
	}
	return result
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

	// Verify if bucket path legal.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify if bucket acl is legal.
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}

	bucket = fs.denormalizeBucket(bucket)

	// Get bucket path.
	bucketDir := filepath.Join(fs.path, bucket)
	if _, e := os.Stat(bucketDir); e == nil {
		return probe.NewError(BucketExists{Bucket: bucket})
	}

	// Make bucket.
	if e := os.Mkdir(bucketDir, 0700); e != nil {
		return probe.NewError(err)
	}

	bucketMetadata := &BucketMetadata{}
	fi, e := os.Stat(bucketDir)
	// Check if bucket exists.
	if e != nil {
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	bucketMetadata.Name = fi.Name()
	bucketMetadata.Created = fi.ModTime()
	bucketMetadata.ACL = BucketACL(acl)
	fs.rwLock.Lock()
	fs.buckets.Metadata[bucket] = bucketMetadata
	fs.rwLock.Unlock()
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}

// denormalizeBucket - will convert incoming bucket names to
// corresponding valid bucketnames on the backend in a platform
// compatible way for all operating systems.
func (fs Filesystem) denormalizeBucket(bucket string) string {
	buckets, e := ioutils.ReadDirNamesN(fs.path, fs.maxBuckets)
	if e != nil {
		return bucket
	}
	for _, b := range buckets {
		// Verify if lowercase version of the bucket is equal to the
		// incoming bucket, then use the proper name.
		if strings.ToLower(b) == bucket {
			return b
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
	// Input validation.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// Save the acl.
	acl := metadata["acl"]
	if !IsValidBucketACL(acl) {
		return probe.NewError(InvalidACL{ACL: acl})
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	bucket = fs.denormalizeBucket(bucket)
	bucketDir := filepath.Join(fs.path, bucket)
	fi, e := os.Stat(bucketDir)
	if e != nil {
		// Check if bucket exists.
		if os.IsNotExist(e) {
			return probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return probe.NewError(e)
	}
	fs.rwLock.RLock()
	bucketMetadata, ok := fs.buckets.Metadata[bucket]
	fs.rwLock.RUnlock()
	if !ok {
		bucketMetadata = &BucketMetadata{}
		bucketMetadata.Name = fi.Name()
		bucketMetadata.Created = fi.ModTime()
	}
	bucketMetadata.ACL = BucketACL(acl)
	fs.rwLock.Lock()
	fs.buckets.Metadata[bucket] = bucketMetadata
	fs.rwLock.Unlock()
	if err := saveBucketsMetadata(*fs.buckets); err != nil {
		return err.Trace(bucket)
	}
	return nil
}
