// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"

	"github.com/minio/minio/internal/logger"

	"github.com/minio/minio-go/v7/pkg/tags"
	bucketsse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/pkg/bucket/policy"

	"github.com/minio/madmin-go"
)

// GatewayUnsupported list of unsupported call stubs for gateway.
type GatewayUnsupported struct{}

// BackendInfo returns the underlying backend information
func (a GatewayUnsupported) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.Gateway}
}

// LocalStorageInfo returns the local disks information, mainly used
// in prometheus - for gateway this just a no-op
func (a GatewayUnsupported) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return StorageInfo{}, nil
}

// NSScanner - scanner is not implemented for gateway
func (a GatewayUnsupported) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- madmin.DataUsageInfo) error {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return NotImplemented{}
}

// PutObjectMetadata - not implemented for gateway.
func (a GatewayUnsupported) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return ObjectInfo{}, NotImplemented{}
}

// NewNSLock is a dummy stub for gateway.
func (a GatewayUnsupported) NewNSLock(bucket string, objects ...string) RWLocker {
	logger.CriticalIf(context.Background(), errors.New("not implemented"))
	return nil
}

// SetDriveCounts no-op
func (a GatewayUnsupported) SetDriveCounts() []int {
	return nil
}

// ListMultipartUploads lists all multipart uploads.
func (a GatewayUnsupported) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, err error) {
	return lmi, NotImplemented{}
}

// NewMultipartUpload upload object in multiple parts
func (a GatewayUnsupported) NewMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (uploadID string, err error) {
	return "", NotImplemented{}
}

// CopyObjectPart copy part of object to uploadID for another object
func (a GatewayUnsupported) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string, partID int, startOffset, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, err error) {
	return pi, NotImplemented{}
}

// PutObjectPart puts a part of object in bucket
func (a GatewayUnsupported) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return pi, NotImplemented{}
}

// GetMultipartInfo returns metadata associated with the uploadId
func (a GatewayUnsupported) GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return MultipartInfo{}, NotImplemented{}
}

// ListObjectVersions returns all object parts for specified object in specified bucket
func (a GatewayUnsupported) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ListObjectVersionsInfo{}, NotImplemented{}
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (a GatewayUnsupported) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (lpi ListPartsInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return lpi, NotImplemented{}
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (a GatewayUnsupported) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, opts ObjectOptions) error {
	return NotImplemented{}
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (a GatewayUnsupported) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return oi, NotImplemented{}
}

// SetBucketPolicy sets policy on bucket
func (a GatewayUnsupported) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketPolicy will get policy on bucket
func (a GatewayUnsupported) GetBucketPolicy(ctx context.Context, bucket string) (bucketPolicy *policy.Policy, err error) {
	return nil, NotImplemented{}
}

// DeleteBucketPolicy deletes all policies on bucket
func (a GatewayUnsupported) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// SetBucketVersioning enables versioning on a bucket.
func (a GatewayUnsupported) SetBucketVersioning(ctx context.Context, bucket string, v *versioning.Versioning) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketVersioning retrieves versioning configuration of a bucket.
func (a GatewayUnsupported) GetBucketVersioning(ctx context.Context, bucket string) (*versioning.Versioning, error) {
	logger.LogIf(ctx, NotImplemented{})
	return nil, NotImplemented{}
}

// SetBucketLifecycle enables lifecycle policies on a bucket.
func (a GatewayUnsupported) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketLifecycle retrieves lifecycle configuration of a bucket.
func (a GatewayUnsupported) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return nil, NotImplemented{}
}

// DeleteBucketLifecycle deletes all lifecycle policies on a bucket
func (a GatewayUnsupported) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// GetBucketSSEConfig returns bucket encryption config on a bucket
func (a GatewayUnsupported) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return nil, NotImplemented{}
}

// SetBucketSSEConfig sets bucket encryption config on a bucket
func (a GatewayUnsupported) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return NotImplemented{}
}

// DeleteBucketSSEConfig deletes bucket encryption config on a bucket
func (a GatewayUnsupported) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// HealFormat - Not implemented stub
func (a GatewayUnsupported) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealBucket - Not implemented stub
func (a GatewayUnsupported) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - Not implemented stub
func (a GatewayUnsupported) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (h madmin.HealResultItem, e error) {
	return h, NotImplemented{}
}

// ListObjectsV2 - Not implemented stub
func (a GatewayUnsupported) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return result, NotImplemented{}
}

// Walk - Not implemented stub
func (a GatewayUnsupported) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	return NotImplemented{}
}

// HealObjects - Not implemented stub
func (a GatewayUnsupported) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	return NotImplemented{}
}

// CopyObject copies a blob from source container to destination container.
func (a GatewayUnsupported) CopyObject(ctx context.Context, srcBucket string, srcObject string, destBucket string, destObject string,
	srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	return objInfo, NotImplemented{}
}

// GetMetrics - no op
func (a GatewayUnsupported) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

// PutObjectTags - not implemented.
func (a GatewayUnsupported) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ObjectInfo{}, NotImplemented{}
}

// GetObjectTags - not implemented.
func (a GatewayUnsupported) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	logger.LogIf(ctx, NotImplemented{})
	return nil, NotImplemented{}
}

// DeleteObjectTags - not implemented.
func (a GatewayUnsupported) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	logger.LogIf(ctx, NotImplemented{})
	return ObjectInfo{}, NotImplemented{}
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (a GatewayUnsupported) IsNotificationSupported() bool {
	return false
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (a GatewayUnsupported) IsListenSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (a GatewayUnsupported) IsEncryptionSupported() bool {
	return false
}

// IsTaggingSupported returns whether object tagging is supported or not for this layer.
func (a GatewayUnsupported) IsTaggingSupported() bool {
	return false
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a GatewayUnsupported) IsCompressionSupported() bool {
	return false
}

// Health - No Op.
func (a GatewayUnsupported) Health(_ context.Context, _ HealthOptions) HealthResult {
	return HealthResult{}
}

// ReadHealth - No Op.
func (a GatewayUnsupported) ReadHealth(_ context.Context) bool {
	return true
}

// TransitionObject - transition object content to target tier.
func (a GatewayUnsupported) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (a GatewayUnsupported) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}
