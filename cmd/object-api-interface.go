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
	"io"
	"time"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Storage operations.
	Shutdown() error
	StorageInfo() StorageInfo

	// Bucket operations.
	MakeBucketWithLocation(bucket string, location string) error
	GetBucketInfo(bucket string) (bucketInfo BucketInfo, err error)
	ListBuckets() (buckets []BucketInfo, err error)
	DeleteBucket(bucket string) error
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error)
	ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error)

	// Object operations.
	GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error)
	GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error)
	PutObject(bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error)
	CopyObject(srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo) (objInfo ObjectInfo, err error)
	DeleteObject(bucket, object string) error

	// Multipart operations.
	ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error)
	NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error)
	CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
		startOffset int64, length int64, srcInfo ObjectInfo) (info PartInfo, err error)
	PutObjectPart(bucket, object, uploadID string, partID int, data *hash.Reader) (info PartInfo, err error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error)
	AbortMultipartUpload(bucket, object, uploadID string) error
	CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []CompletePart) (objInfo ObjectInfo, err error)

	// Healing operations.
	HealFormat(dryRun bool) (madmin.HealResultItem, error)
	HealBucket(bucket string, dryRun bool) ([]madmin.HealResultItem, error)
	HealObject(bucket, object string, dryRun bool) (madmin.HealResultItem, error)
	ListBucketsHeal() (buckets []BucketInfo, err error)
	ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error)

	// Locking operations
	ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error)
	ClearLocks([]VolumeLockInfo) error

	// Policy operations
	SetBucketPolicy(string, policy.BucketAccessPolicy) error
	GetBucketPolicy(string) (policy.BucketAccessPolicy, error)
	RefreshBucketPolicy(string) error
	DeleteBucketPolicy(string) error

	// Supported operations check
	IsNotificationSupported() bool
	IsEncryptionSupported() bool
}
