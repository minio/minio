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
	GetBucketMetadata(bucket string, signature *Signature) (BucketMetadata, error)
	SetBucketMetadata(bucket string, metadata map[string]string, signature *Signature) error
	ListBuckets(signature *Signature) ([]BucketMetadata, error)
	MakeBucket(bucket string, ACL string, location io.Reader, signature *Signature) error

	// Bucket operations
	ListObjects(string, BucketResourcesMetadata, *Signature) ([]ObjectMetadata, BucketResourcesMetadata, error)

	// Object operations
	GetObject(w io.Writer, bucket, object string) (int64, error)
	GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error)
	GetObjectMetadata(bucket, object string, signature *Signature) (ObjectMetadata, error)
	// bucket, object, expectedMD5Sum, size, reader, metadata, signature
	CreateObject(string, string, string, int64, io.Reader, map[string]string, *Signature) (ObjectMetadata, error)

	Multipart
}

// Multipart API
type Multipart interface {
	NewMultipartUpload(bucket, key, contentType string, signature *Signature) (string, error)
	AbortMultipartUpload(bucket, key, uploadID string, signature *Signature) error
	CreateObjectPart(string, string, string, int, string, string, int64, io.Reader, *Signature) (string, error)
	CompleteMultipartUpload(bucket, key, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, error)
	ListMultipartUploads(string, BucketMultipartResourcesMetadata, *Signature) (BucketMultipartResourcesMetadata, error)
	ListObjectParts(string, string, ObjectResourcesMetadata, *Signature) (ObjectResourcesMetadata, error)
}

// Management is a donut management system interface
type Management interface {
	Heal() error
	Rebalance() error
	Info() (map[string][]string, error)

	AttachNode(hostname string, disks []string) error
	DetachNode(hostname string) error
}
