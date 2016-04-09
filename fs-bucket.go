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

package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/probe"
)

/// Bucket Operations

// DeleteBucket - delete a bucket.
func (fs Filesystem) DeleteBucket(bucket string) *probe.Error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = getActualBucketname(fs.diskPath, bucket)
	bucketDir := filepath.Join(fs.diskPath, bucket)
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
	return nil
}

// ListBuckets - Get service.
func (fs Filesystem) ListBuckets() ([]BucketInfo, *probe.Error) {
	files, e := ioutil.ReadDir(fs.diskPath)
	if e != nil {
		return []BucketInfo{}, probe.NewError(e)
	}
	var buckets []BucketInfo
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
		bucket := BucketInfo{
			Name:    dirName,
			Created: file.ModTime(),
		}
		buckets = append(buckets, bucket)
	}
	// Remove duplicated entries.
	buckets = removeDuplicateBuckets(buckets)
	return buckets, nil
}

// removeDuplicateBuckets - remove duplicate buckets.
func removeDuplicateBuckets(buckets []BucketInfo) []BucketInfo {
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

// MakeBucket - PUT Bucket
func (fs Filesystem) MakeBucket(bucket string) *probe.Error {
	if _, e := fs.checkBucketArg(bucket); e == nil {
		return probe.NewError(BucketExists{Bucket: bucket})
	} else if _, ok := e.(BucketNameInvalid); ok {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucketDir := filepath.Join(fs.diskPath, bucket)

	// Make bucket.
	if e := os.Mkdir(bucketDir, 0700); e != nil {
		return probe.NewError(e)
	}
	return nil
}

// getActualBucketname - will convert incoming bucket names to
// corresponding actual bucketnames on the backend in a platform
// compatible way for all operating systems.
func getActualBucketname(fsPath, bucket string) string {
	fd, e := os.Open(fsPath)
	if e != nil {
		return bucket
	}
	buckets, e := fd.Readdirnames(-1)
	if e != nil {
		return bucket
	}
	for _, b := range buckets {
		// Verify if lowercase version of the bucket is equal
		// to the incoming bucket, then use the proper name.
		if strings.ToLower(b) == bucket {
			return b
		}
	}
	return bucket
}

// GetBucketInfo - get bucket metadata.
func (fs Filesystem) GetBucketInfo(bucket string) (BucketInfo, *probe.Error) {
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	bucket = getActualBucketname(fs.diskPath, bucket)
	// Get bucket path.
	bucketDir := filepath.Join(fs.diskPath, bucket)
	fi, e := os.Stat(bucketDir)
	if e != nil {
		// Check if bucket exists.
		if os.IsNotExist(e) {
			return BucketInfo{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return BucketInfo{}, probe.NewError(e)
	}
	bucketMetadata := BucketInfo{}
	bucketMetadata.Name = fi.Name()
	bucketMetadata.Created = fi.ModTime()
	return bucketMetadata, nil
}
