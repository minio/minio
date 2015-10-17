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
	"io"

	"github.com/minio/minio-xl/pkg/probe"
)

// CloudStorage is a fs cloud storage interface
type CloudStorage interface {
	// Storage service operations
	GetBucketMetadata(bucket string) (BucketMetadata, *probe.Error)
	SetBucketMetadata(bucket string, metadata map[string]string) *probe.Error
	ListBuckets() ([]BucketMetadata, *probe.Error)
	MakeBucket(bucket, acl string) *probe.Error
	DeleteBucket(bucket string) *probe.Error

	// Bucket operations
	ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error)

	// Object operations
	GetObject(w io.Writer, bucket, object string, start, length int64) (int64, *probe.Error)
	GetObjectMetadata(bucket, object string) (ObjectMetadata, *probe.Error)
	// bucket, object, expectedMD5Sum, size, reader, metadata, signature
	CreateObject(bucket, object, md5sum string, size int64, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error)
	DeleteObject(bucket, object string) *probe.Error

	// Multipart API
	Multipart

	// ACL API
	ACL
}

// Multipart API
type Multipart interface {
	NewMultipartUpload(bucket, object string) (string, *probe.Error)
	AbortMultipartUpload(bucket, object, uploadID string) *probe.Error
	CreateObjectPart(bucket, object, uploadID, md5sum string, partID int, size int64, data io.Reader, signature *Signature) (string, *probe.Error)
	CompleteMultipartUpload(bucket, object, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, *probe.Error)
	ListMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error)
	ListObjectParts(bucket, object string, objectResources ObjectResourcesMetadata) (ObjectResourcesMetadata, *probe.Error)
}

// ACL API
type ACL interface {
	IsPublicBucket(bucket string) bool
	IsPrivateBucket(bucket string) bool
	IsReadOnlyBucket(bucket string) bool
}
