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
	"io"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
)

type gatewayUnsupported struct{}

// ListMultipartUploads lists all multipart uploads.
func (a gatewayUnsupported) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, err error) {
	return lmi, errors.Trace(NotImplemented{})
}

// NewMultipartUpload upload object in multiple parts
func (a gatewayUnsupported) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	return "", errors.Trace(NotImplemented{})
}

// PutObjectPart puts a part of object in bucket
func (a gatewayUnsupported) PutObjectPart(bucket string, object string, uploadID string, partID int, data *hash.Reader) (pi PartInfo, err error) {
	return pi, errors.Trace(NotImplemented{})
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (a gatewayUnsupported) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, err error) {
	return lpi, errors.Trace(NotImplemented{})
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (a gatewayUnsupported) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	return errors.Trace(NotImplemented{})
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (a gatewayUnsupported) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []CompletePart) (oi ObjectInfo, err error) {
	return oi, errors.Trace(NotImplemented{})
}

// SetBucketPolicies sets policy on bucket
func (a gatewayUnsupported) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return errors.Trace(NotImplemented{})
}

// GetBucketPolicies will get policy on bucket
func (a gatewayUnsupported) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, err error) {
	return bal, errors.Trace(NotImplemented{})
}

// DeleteBucketPolicies deletes all policies on bucket
func (a gatewayUnsupported) DeleteBucketPolicies(bucket string) error {
	return errors.Trace(NotImplemented{})
}

// CopyObjectPart - Not implemented.
func (a gatewayUnsupported) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string,
	partID int, startOffset int64, length int64, metadata map[string]string) (info PartInfo, err error) {
	return info, errors.Trace(NotImplemented{})
}

// HealBucket - Not relevant.
func (a gatewayUnsupported) HealBucket(bucket string) error {
	return errors.Trace(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (a gatewayUnsupported) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return nil, errors.Trace(NotImplemented{})
}

// HealObject - Not relevant.
func (a gatewayUnsupported) HealObject(bucket, object string) (int, int, error) {
	return 0, 0, errors.Trace(NotImplemented{})
}

func (a gatewayUnsupported) ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return result, errors.Trace(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (a gatewayUnsupported) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, errors.Trace(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (a gatewayUnsupported) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	return lmi, errors.Trace(NotImplemented{})
}

// AnonListObjects - List objects anonymously
func (a gatewayUnsupported) AnonListObjects(bucket string, prefix string, marker string, delimiter string,
	maxKeys int) (loi ListObjectsInfo, err error) {
	return loi, errors.Trace(NotImplemented{})
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (a gatewayUnsupported) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi ListObjectsV2Info, err error) {
	return loi, errors.Trace(NotImplemented{})
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (a gatewayUnsupported) AnonGetBucketInfo(bucket string) (bi BucketInfo, err error) {
	return bi, errors.Trace(NotImplemented{})
}

// AnonPutObject creates a new object anonymously with the incoming data,
func (a gatewayUnsupported) AnonPutObject(bucket, object string, data *hash.Reader,
	metadata map[string]string) (ObjectInfo, error) {
	return ObjectInfo{}, errors.Trace(NotImplemented{})
}

// AnonGetObject downloads object anonymously.
func (a gatewayUnsupported) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return errors.Trace(NotImplemented{})
}

// AnonGetObjectInfo returns stat information about an object anonymously.
func (a gatewayUnsupported) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return objInfo, errors.Trace(NotImplemented{})
}

// CopyObject copies a blob from source container to destination container.
func (a gatewayUnsupported) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string,
	metadata map[string]string) (objInfo ObjectInfo, err error) {
	return objInfo, errors.Trace(NotImplemented{})
}
