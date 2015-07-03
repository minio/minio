/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import "time"

// ObjectMetadata container for object on donut system
type ObjectMetadata struct {
	// version
	Version string `json:"version"`

	// object metadata
	Created time.Time `json:"created"`
	Bucket  string    `json:"bucket"`
	Object  string    `json:"object"`
	Size    int64     `json:"size"`

	// erasure
	DataDisks        uint8  `json:"sys.erasureK"`
	ParityDisks      uint8  `json:"sys.erasureM"`
	ErasureTechnique string `json:"sys.erasureTechnique"`
	BlockSize        int    `json:"sys.blockSize"`
	ChunkCount       int    `json:"sys.chunkCount"`

	// checksums
	MD5Sum    string `json:"sys.md5sum"`
	SHA512Sum string `json:"sys.sha512sum"`

	// metadata
	Metadata map[string]string `json:"metadata"`
}

// Metadata container for donut metadata
type Metadata struct {
	Version string `json:"version"`
}

// AllBuckets container for all buckets
type AllBuckets struct {
	Version string                    `json:"version"`
	Buckets map[string]BucketMetadata `json:"buckets"`
}

// BucketMetadata container for bucket level metadata
type BucketMetadata struct {
	Version       string                 `json:"version"`
	Name          string                 `json:"name"`
	ACL           BucketACL              `json:"acl"`
	Created       time.Time              `json:"created"`
	Metadata      map[string]string      `json:"metadata"`
	BucketObjects map[string]interface{} `json:"objects"`
}

// ListObjectsResults container for list objects response
type ListObjectsResults struct {
	Objects        map[string]ObjectMetadata `json:"objects"`
	CommonPrefixes []string                  `json:"commonPrefixes"`
	IsTruncated    bool                      `json:"isTruncated"`
}
