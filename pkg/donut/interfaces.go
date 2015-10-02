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

package donut

import (
	"io"

	"github.com/minio/minio/pkg/probe"
	signv4 "github.com/minio/minio/pkg/signature"
)

// Collection of Donut specification interfaces

// Interface is a collection of cloud storage and management interface
type Interface interface {
	CloudStorage
	Management
}

// CloudStorage is a donut cloud storage interface
type CloudStorage interface {
	// Storage service operations
	GetBucketMetadata(bucket string, signature *signv4.Signature) (BucketMetadata, *probe.Error)
	SetBucketMetadata(bucket string, metadata map[string]string, signature *signv4.Signature) *probe.Error
	ListBuckets(signature *signv4.Signature) ([]BucketMetadata, *probe.Error)
	MakeBucket(bucket string, ACL string, location io.Reader, signature *signv4.Signature) *probe.Error

	// Bucket operations
	ListObjects(string, BucketResourcesMetadata, *signv4.Signature) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error)

	// Object operations
	GetObject(w io.Writer, bucket, object string, start, length int64) (int64, *probe.Error)
	GetObjectMetadata(bucket, object string, signature *signv4.Signature) (ObjectMetadata, *probe.Error)
	// bucket, object, expectedMD5Sum, size, reader, metadata, signature
	CreateObject(string, string, string, int64, io.Reader, map[string]string, *signv4.Signature) (ObjectMetadata, *probe.Error)

	Multipart
}

// Multipart API
type Multipart interface {
	NewMultipartUpload(bucket, key, contentType string, signature *signv4.Signature) (string, *probe.Error)
	AbortMultipartUpload(bucket, key, uploadID string, signature *signv4.Signature) *probe.Error
	CreateObjectPart(string, string, string, int, string, string, int64, io.Reader, *signv4.Signature) (string, *probe.Error)
	CompleteMultipartUpload(bucket, key, uploadID string, data io.Reader, signature *signv4.Signature) (ObjectMetadata, *probe.Error)
	ListMultipartUploads(string, BucketMultipartResourcesMetadata, *signv4.Signature) (BucketMultipartResourcesMetadata, *probe.Error)
	ListObjectParts(string, string, ObjectResourcesMetadata, *signv4.Signature) (ObjectResourcesMetadata, *probe.Error)
}

// Management is a donut management system interface
type Management interface {
	Heal() *probe.Error
	Rebalance() *probe.Error
	Info() (map[string][]string, *probe.Error)

	AttachNode(hostname string, disks []string) *probe.Error
	DetachNode(hostname string) *probe.Error
}
