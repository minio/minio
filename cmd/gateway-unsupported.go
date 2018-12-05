/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
)

// GatewayUnsupported list of unsupported call stubs for gateway.
type GatewayUnsupported struct{}

// ListMultipartUploads lists all multipart uploads.
func (a GatewayUnsupported) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, err error) {
	return lmi, NotImplemented{}
}

// NewMultipartUpload upload object in multiple parts
func (a GatewayUnsupported) NewMultipartUpload(ctx context.Context, bucket string, object string, metadata map[string]string, opts ObjectOptions) (uploadID string, err error) {
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
func (a GatewayUnsupported) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, err error) {
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

// ReloadFormat - Not implemented stub.
func (a GatewayUnsupported) ReloadFormat(ctx context.Context, dryRun bool) error {
	return NotImplemented{}
}

// HealFormat - Not implemented stub
func (a GatewayUnsupported) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealBucket - Not implemented stub
func (a GatewayUnsupported) HealBucket(ctx context.Context, bucket string, dryRun bool) ([]madmin.HealResultItem, error) {
	return nil, NotImplemented{}
}

// ListBucketsHeal - Not implemented stub
func (a GatewayUnsupported) ListBucketsHeal(ctx context.Context) (buckets []BucketInfo, err error) {
	return nil, NotImplemented{}
}

// HealObject - Not implemented stub
func (a GatewayUnsupported) HealObject(ctx context.Context, bucket, object string, dryRun bool) (h madmin.HealResultItem, e error) {
	return h, NotImplemented{}
}

// ListObjectsV2 - Not implemented stub
func (a GatewayUnsupported) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return result, NotImplemented{}
}

// ListObjectsHeal - Not implemented stub
func (a GatewayUnsupported) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, NotImplemented{}
}

// CopyObject copies a blob from source container to destination container.
func (a GatewayUnsupported) CopyObject(ctx context.Context, srcBucket string, srcObject string, destBucket string, destObject string,
	srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	return objInfo, NotImplemented{}
}

// RefreshBucketPolicy refreshes cache policy with what's on disk.
func (a GatewayUnsupported) RefreshBucketPolicy(ctx context.Context, bucket string) error {
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

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (a GatewayUnsupported) IsEncryptionSupported() bool {
	return false
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a GatewayUnsupported) IsCompressionSupported() bool {
	return false
}
