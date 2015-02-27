/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package storage

import (
	"io"
	"regexp"
	"time"
)

type Storage interface {
	// Bucket Operations
	ListBuckets() ([]BucketMetadata, error)
	StoreBucket(bucket string) error
	StoreBucketPolicy(bucket string, policy interface{}) error
	GetBucketPolicy(bucket string) (interface{}, error)

	// Object Operations
	CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error)
	GetObjectMetadata(bucket string, object string) (ObjectMetadata, error)
	ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, error)
	StoreObject(bucket string, key string, contentType string, data io.Reader) error
}

type BucketMetadata struct {
	Name    string
	Created time.Time
}

type ObjectMetadata struct {
	Bucket    string
	Key       string
	Maxkeys   int
	Prefix    string
	Marker    string
	Delimiter string

	ContentType string
	Created     time.Time
	ETag        string
	Size        int64
}

// Various types of bucket resources
type BucketResourcesMetadata struct {
	Prefix         string
	Marker         string
	Maxkeys        int
	Delimiter      string
	IsTruncated    bool
	CommonPrefixes []string

	Policy bool
	// TODO
	Logging      string
	Notification string
}

func IsValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", bucket); match == true {
		return false
	}
	// We don't support buckets with '.' in them
	match, _ := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9\\-]+[a-zA-Z0-9]$", bucket)
	return match
}

func IsValidObject(object string) bool {
	return true
}
