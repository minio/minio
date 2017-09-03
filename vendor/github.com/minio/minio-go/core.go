/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2017 Minio, Inc.
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

package minio

import (
	"io"

	"github.com/minio/minio-go/pkg/policy"
)

// Core - Inherits Client and adds new methods to expose the low level S3 APIs.
type Core struct {
	*Client
}

// NewCore - Returns new initialized a Core client, this CoreClient should be
// only used under special conditions such as need to access lower primitives
// and being able to use them to write your own wrappers.
func NewCore(endpoint string, accessKeyID, secretAccessKey string, secure bool) (*Core, error) {
	var s3Client Core
	client, err := NewV4(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		return nil, err
	}
	s3Client.Client = client
	return &s3Client, nil
}

// ListObjects - List all the objects at a prefix, optionally with marker and delimiter
// you can further filter the results.
func (c Core) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListBucketResult, err error) {
	return c.listObjectsQuery(bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 - Lists all the objects at a prefix, similar to ListObjects() but uses
// continuationToken instead of marker to further filter the results.
func (c Core) ListObjectsV2(bucketName, objectPrefix, continuationToken string, fetchOwner bool, delimiter string, maxkeys int) (ListBucketV2Result, error) {
	return c.listObjectsV2Query(bucketName, objectPrefix, continuationToken, fetchOwner, delimiter, maxkeys)
}

// PutObject - Upload object. Uploads using single PUT call.
func (c Core) PutObject(bucket, object string, size int64, data io.Reader, md5Sum, sha256Sum []byte, metadata map[string][]string) (ObjectInfo, error) {
	return c.putObjectDo(bucket, object, data, md5Sum, sha256Sum, size, metadata)
}

// NewMultipartUpload - Initiates new multipart upload and returns the new uploaID.
func (c Core) NewMultipartUpload(bucket, object string, metadata map[string][]string) (uploadID string, err error) {
	result, err := c.initiateMultipartUpload(bucket, object, metadata)
	return result.UploadID, err
}

// ListMultipartUploads - List incomplete uploads.
func (c Core) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartUploadsResult, err error) {
	return c.listMultipartUploadsQuery(bucket, keyMarker, uploadIDMarker, prefix, delimiter, maxUploads)
}

// PutObjectPart - Upload an object part.
func (c Core) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Sum, sha256Sum []byte) (ObjectPart, error) {
	return c.PutObjectPartWithMetadata(bucket, object, uploadID, partID, size, data, md5Sum, sha256Sum, nil)
}

// PutObjectPartWithMetadata - upload an object part with additional request metadata.
func (c Core) PutObjectPartWithMetadata(bucket, object, uploadID string, partID int,
	size int64, data io.Reader, md5Sum, sha256Sum []byte, metadata map[string][]string) (ObjectPart, error) {
	return c.uploadPart(bucket, object, uploadID, data, partID, md5Sum, sha256Sum, size, metadata)
}

// ListObjectParts - List uploaded parts of an incomplete upload.x
func (c Core) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListObjectPartsResult, err error) {
	return c.listObjectPartsQuery(bucket, object, uploadID, partNumberMarker, maxParts)
}

// CompleteMultipartUpload - Concatenate uploaded parts and commit to an object.
func (c Core) CompleteMultipartUpload(bucket, object, uploadID string, parts []CompletePart) error {
	_, err := c.completeMultipartUpload(bucket, object, uploadID, completeMultipartUpload{
		Parts: parts,
	})
	return err
}

// AbortMultipartUpload - Abort an incomplete upload.
func (c Core) AbortMultipartUpload(bucket, object, uploadID string) error {
	return c.abortMultipartUpload(bucket, object, uploadID)
}

// GetBucketPolicy - fetches bucket access policy for a given bucket.
func (c Core) GetBucketPolicy(bucket string) (policy.BucketAccessPolicy, error) {
	return c.getBucketPolicy(bucket)
}

// PutBucketPolicy - applies a new bucket access policy for a given bucket.
func (c Core) PutBucketPolicy(bucket string, bucketPolicy policy.BucketAccessPolicy) error {
	return c.putBucketPolicy(bucket, bucketPolicy)
}

// GetObject is a lower level API implemented to support reading
// partial objects and also downloading objects with special conditions
// matching etag, modtime etc.
func (c Core) GetObject(bucketName, objectName string, reqHeaders RequestHeaders) (io.ReadCloser, ObjectInfo, error) {
	return c.getObject(bucketName, objectName, reqHeaders)
}

// StatObject is a lower level API implemented to support special
// conditions matching etag, modtime on a request.
func (c Core) StatObject(bucketName, objectName string, reqHeaders RequestHeaders) (ObjectInfo, error) {
	return c.statObject(bucketName, objectName, reqHeaders)
}
