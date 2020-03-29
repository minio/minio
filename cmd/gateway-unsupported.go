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

	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/bucket/policy"

	"github.com/minio/minio/pkg/madmin"
)

// GatewayLocker implements custom NeNSLock implementation
type GatewayLocker struct {
	ObjectLayer
	nsMutex *nsLockMap
}

// NewNSLock - implements gateway level locker
func (l *GatewayLocker) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return l.nsMutex.NewNSLock(ctx, nil, bucket, objects...)
}

// NewGatewayLayerWithLocker - initialize gateway with locker.
func NewGatewayLayerWithLocker(gwLayer ObjectLayer) ObjectLayer {
	return &GatewayLocker{ObjectLayer: gwLayer, nsMutex: newNSLock(false)}
}

// GatewayUnsupported list of unsupported call stubs for gateway.
type GatewayUnsupported struct{}

// CrawlAndGetDataUsage - crawl is not implemented for gateway
func (a GatewayUnsupported) CrawlAndGetDataUsage(ctx context.Context, updates chan<- DataUsageInfo) error {
	logger.CriticalIf(ctx, errors.New("not implemented"))
	return NotImplemented{}
}

// NewNSLock is a dummy stub for gateway.
func (a GatewayUnsupported) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	logger.CriticalIf(ctx, errors.New("not implemented"))
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

// ListObjectParts returns all object parts for specified object in specified bucket
func (a GatewayUnsupported) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (lpi ListPartsInfo, err error) {
	logger.LogIf(ctx, NotImplemented{})
	return lpi, NotImplemented{}
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (a GatewayUnsupported) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
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

// SetBucketLifecycle sets lifecycle on bucket
func (a GatewayUnsupported) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetBucketLifecycle will get lifecycle on bucket
func (a GatewayUnsupported) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return nil, NotImplemented{}
}

// DeleteBucketLifecycle deletes all lifecycle on bucket
func (a GatewayUnsupported) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// GetBucketSSEConfig returns bucket encryption config on given bucket
func (a GatewayUnsupported) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return nil, NotImplemented{}
}

// SetBucketSSEConfig sets bucket encryption config on given bucket
func (a GatewayUnsupported) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return NotImplemented{}
}

// DeleteBucketSSEConfig deletes bucket encryption config on given bucket
func (a GatewayUnsupported) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return NotImplemented{}
}

// ReloadFormat - Not implemented stub.
func (a GatewayUnsupported) ReloadFormat(ctx context.Context, dryRun bool) error {
	return NotImplemented{}
}

// HealFormat - Not implemented stub
func (a GatewayUnsupported) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealBucket - Not implemented stub
func (a GatewayUnsupported) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// ListBucketsHeal - Not implemented stub
func (a GatewayUnsupported) ListBucketsHeal(ctx context.Context) (buckets []BucketInfo, err error) {
	return nil, NotImplemented{}
}

// HealObject - Not implemented stub
func (a GatewayUnsupported) HealObject(ctx context.Context, bucket, object string, opts madmin.HealOpts) (h madmin.HealResultItem, e error) {
	return h, NotImplemented{}
}

// ListObjectsV2 - Not implemented stub
func (a GatewayUnsupported) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return result, NotImplemented{}
}

// Walk - Not implemented stub
func (a GatewayUnsupported) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	return NotImplemented{}
}

// HealObjects - Not implemented stub
func (a GatewayUnsupported) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn healObjectFn) (e error) {
	return NotImplemented{}
}

// CopyObject copies a blob from source container to destination container.
func (a GatewayUnsupported) CopyObject(ctx context.Context, srcBucket string, srcObject string, destBucket string, destObject string,
	srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	return objInfo, NotImplemented{}
}

// GetMetrics - no op
func (a GatewayUnsupported) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// PutObjectTag - not implemented.
func (a GatewayUnsupported) PutObjectTag(ctx context.Context, bucket, object string, tags string) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetObjectTag - not implemented.
func (a GatewayUnsupported) GetObjectTag(ctx context.Context, bucket, object string) (tagging.Tagging, error) {
	logger.LogIf(ctx, NotImplemented{})
	return tagging.Tagging{}, NotImplemented{}
}

// DeleteObjectTag - not implemented.
func (a GatewayUnsupported) DeleteObjectTag(ctx context.Context, bucket, object string) error {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (a GatewayUnsupported) IsNotificationSupported() bool {
	return false
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (a GatewayUnsupported) IsListenBucketSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (a GatewayUnsupported) IsEncryptionSupported() bool {
	return false
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a GatewayUnsupported) IsCompressionSupported() bool {
	return false
}

// IsReady - No Op.
func (a GatewayUnsupported) IsReady(_ context.Context) bool {
	return false
}
