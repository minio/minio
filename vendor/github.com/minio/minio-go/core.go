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

import "io"

// Inherits Client and adds new methods to expose the low level S3 APIs.
type Core struct {
	*Client
}

// NewCoreClient - Returns new Core.
func NewCore(endpoint string, accessKeyID, secretAccessKey string, secure bool) (*Core, error) {
	var s3Client Core
	client, err := NewV4(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		return nil, err
	}
	s3Client.Client = client
	return &s3Client, nil
}

// ListObjects - List the objects.
func (c Core) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListBucketResult, err error) {
	return c.listObjectsQuery(bucket, prefix, marker, delimiter, maxKeys)
}

// PutObject - Upload object. Uploads using single PUT call.
func (c Core) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string][]string) error {
	_, err := c.putObjectSingle(bucket, object, data, size, metadata, nil)
	return err
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
func (c Core) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex, sha256sum []byte) (ObjectPart, error) {
	return c.uploadPart(bucket, object, uploadID, data, partID, md5Hex, sha256sum, size)
}

// ListObjectParts - List uploaded parts of an incomplete upload.
func (c Core) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListObjectPartsResult, err error) {
	return c.listObjectPartsQuery(bucket, object, uploadID, partNumberMarker, maxParts)
}

// CompleteMultipartUpload - Concatenate uploaded parts and commit to an object.
func (c Core) CompleteMultipartUpload(bucket, object, uploadID string, parts []CompletePart) error {
	_, err := c.completeMultipartUpload(bucket, object, uploadID, completeMultipartUpload{Parts: parts})
	return err
}

// AbortMultipartUpload - Abort an incomplete upload.
func (c Core) AbortMultipartUpload(bucket, object, uploadID string) error {
	return c.abortMultipartUpload(bucket, object, uploadID)
}
