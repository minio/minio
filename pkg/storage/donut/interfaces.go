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

import "io"

// Collection of Donut specification interfaces

// Interface is a collection of object storage and management interface
type Interface interface {
	ObjectStorage
	Management
}

// ObjectStorage is a donut object storage interface
type ObjectStorage interface {
	// Storage service operations
	GetBucketMetadata(bucket string) (BucketMetadata, error)
	SetBucketMetadata(bucket string, metadata map[string]string) error
	ListBuckets() ([]BucketMetadata, error)
	MakeBucket(bucket string, ACL string) error

	// Bucket operations
	ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, error)

	// Object operations
	GetObject(w io.Writer, bucket, object string) (int64, error)
	GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error)
	GetObjectMetadata(bucket, object string) (ObjectMetadata, error)
	CreateObject(bucket, object, expectedMD5Sum string, size int64, reader io.Reader, metadata map[string]string) (ObjectMetadata, error)

	Multipart
}

// Multipart API
type Multipart interface {
	NewMultipartUpload(bucket, key, contentType string) (string, error)
	AbortMultipartUpload(bucket, key, uploadID string) error
	CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (ObjectMetadata, error)
	ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, error)
	ListObjectParts(bucket, key string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, error)
}

// Management is a donut management system interface
type Management interface {
	Heal() error
	Rebalance() error
	Info() (map[string][]string, error)

	AttachNode(hostname string, disks []string) error
	DetachNode(hostname string) error

	SaveConfig() error
	LoadConfig() error
}
