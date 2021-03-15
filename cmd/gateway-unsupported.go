/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"context"
	"errors"

	"github.com/minio/minio/cmd/logger"

	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/versioning"

	"github.com/minio/minio/pkg/madmin"
)

// ObjectLayerUnsupported list of unsupported call stubs for gateway.
type ObjectLayerUnsupported struct{}

// BackendInfo returns the underlying backend information
func (a ObjectLayerUnsupported) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.Gateway}
}

// LocalStorageInfo returns the local disks information, mainly used
// in prometheus - for gateway this just a no-op
func (a ObjectLayerUnsupported) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return StorageInfo{}, nil
}

// NSScanner - scanner is not implemented for gateway
func (a ObjectLayerUnsupported) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- madmin.DataUsageInfo) error {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return NotImplemented{}
}

// PutObjectMetadata - not implemented for gateway.
func (a ObjectLayerUnsupported) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return ObjectInfo{}, NotImplemented{}
}

// NewNSLock is a dummy stub for gateway.
func (a ObjectLayerUnsupported) NewNSLock(bucket string, objects ...string) RWLocker {
	logger.CriticalIf(context.Background(), errors.New("not implemented"))
	return nil
}

// SetDriveCounts no-op
func (a ObjectLayerUnsupported) SetDriveCounts() []int {
	return nil
}

// ListMultipartUploads lists all multipart uploads.
func (a ObjectLayerUnsupported) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, err error) {
	return lmi, NotImplemented{}
}

// NewMultipartUpload upload object in multiple parts
func (a ObjectLayerUnsupported) NewMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (uploadID string, err error) {
	return "", NotImplemented{}
}

// CopyObjectPart copy part of object to uploadID for another object
func (a ObjectLayerUnsupported) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, err error) {
	return pi, NotImplemented{}
}

// PutObjectPart puts a part of object in bucket
func (a ObjectLayerUnsupported) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return pi, NotImplemented{}
}

// GetMultipartInfo returns metadata associated with the uploadId
func (a ObjectLayerUnsupported) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return MultipartInfo{}, NotImplemented{}
}

// ListObjectVersions returns all object parts for specified object in specified bucket
func (a ObjectLayerUnsupported) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ListObjectVersionsInfo{}, NotImplemented{}
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (a ObjectLayerUnsupported) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (lpi ListPartsInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return lpi, NotImplemented{}
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (a ObjectLayerUnsupported) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, opts ObjectOptions) error {
	return NotImplemented{}
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (a ObjectLayerUnsupported) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return oi, NotImplemented{}
}

// SetBucketPolicy sets policy on bucket
func (a ObjectLayerUnsupported) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketPolicy will get policy on bucket
func (a ObjectLayerUnsupported) GetBucketPolicy(ctx context.Context, bucket string) (bucketPolicy *policy.Policy, err error) {
	return nil, NotImplemented{}
}

// DeleteBucketPolicy deletes all policies on bucket
func (a ObjectLayerUnsupported) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// SetBucketVersioning enables versioning on a bucket.
func (a ObjectLayerUnsupported) SetBucketVersioning(ctx context.Context, bucket string, v *versioning.Versioning) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketVersioning retrieves versioning configuration of a bucket.
func (a ObjectLayerUnsupported) GetBucketVersioning(ctx context.Context, bucket string) (*versioning.Versioning, error) {
	logger.LogIf(ctx, NotImplemented{})
	return nil, NotImplemented{}
}

// SetBucketLifecycle enables lifecycle policies on a bucket.
func (a ObjectLayerUnsupported) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketLifecycle retrieves lifecycle configuration of a bucket.
func (a ObjectLayerUnsupported) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return nil, NotImplemented{}
}

// DeleteBucketLifecycle deletes all lifecycle policies on a bucket
func (a ObjectLayerUnsupported) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// GetBucketSSEConfig returns bucket encryption config on a bucket
func (a ObjectLayerUnsupported) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return nil, NotImplemented{}
}

// SetBucketSSEConfig sets bucket encryption config on a bucket
func (a ObjectLayerUnsupported) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return NotImplemented{}
}

// DeleteBucketSSEConfig deletes bucket encryption config on a bucket
func (a ObjectLayerUnsupported) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// HealFormat - Not implemented stub
func (a ObjectLayerUnsupported) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealBucket - Not implemented stub
func (a ObjectLayerUnsupported) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - Not implemented stub
func (a ObjectLayerUnsupported) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (h madmin.HealResultItem, e error) {
	return h, NotImplemented{}
}

// ListObjectsV2 - Not implemented stub
func (a ObjectLayerUnsupported) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return result, NotImplemented{}
}

// Walk - Not implemented stub
func (a ObjectLayerUnsupported) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	return NotImplemented{}
}

// HealObjects - Not implemented stub
func (a ObjectLayerUnsupported) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	return NotImplemented{}
}

// CopyObject copies a blob from source container to destination container.
func (a ObjectLayerUnsupported) CopyObject(ctx context.Context, srcBucket string, srcObject string, destBucket string, destObject string,
	srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	return objInfo, NotImplemented{}
}

// GetMetrics - no op
func (a ObjectLayerUnsupported) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

// PutObjectTags - not implemented.
func (a ObjectLayerUnsupported) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ObjectInfo{}, NotImplemented{}
}

// GetObjectTags - not implemented.
func (a ObjectLayerUnsupported) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	logger.LogIf(ctx, NotImplemented{})
	return nil, NotImplemented{}
}

// DeleteObjectTags - not implemented.
func (a ObjectLayerUnsupported) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ObjectInfo{}, NotImplemented{}
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (a ObjectLayerUnsupported) IsNotificationSupported() bool {
	return false
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (a ObjectLayerUnsupported) IsListenSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (a ObjectLayerUnsupported) IsEncryptionSupported() bool {
	return false
}

// IsTaggingSupported returns whether object tagging is supported or not for this layer.
func (a ObjectLayerUnsupported) IsTaggingSupported() bool {
	return false
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a ObjectLayerUnsupported) IsCompressionSupported() bool {
	return false
}

// Health - No Op.
func (a ObjectLayerUnsupported) Health(_ context.Context, _ HealthOptions) HealthResult {
	return HealthResult{}
}

// ReadHealth - No Op.
func (a ObjectLayerUnsupported) ReadHealth(_ context.Context) bool {
	return true
}
