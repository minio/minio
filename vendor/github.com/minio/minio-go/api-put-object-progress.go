/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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
	"strings"

	"github.com/minio/minio-go/pkg/credentials"
	"github.com/minio/minio-go/pkg/encrypt"
	"github.com/minio/minio-go/pkg/s3utils"
)

// PutObjectWithProgress - with progress.
func (c Client) PutObjectWithProgress(bucketName, objectName string, reader io.Reader, contentType string, progress io.Reader) (n int64, err error) {
	metaData := make(map[string][]string)
	metaData["Content-Type"] = []string{contentType}
	return c.PutObjectWithMetadata(bucketName, objectName, reader, metaData, progress)
}

// PutEncryptedObject - Encrypt and store object.
func (c Client) PutEncryptedObject(bucketName, objectName string, reader io.Reader, encryptMaterials encrypt.Materials, metaData map[string][]string, progress io.Reader) (n int64, err error) {

	if encryptMaterials == nil {
		return 0, ErrInvalidArgument("Unable to recognize empty encryption properties")
	}

	if err := encryptMaterials.SetupEncryptMode(reader); err != nil {
		return 0, err
	}

	if metaData == nil {
		metaData = make(map[string][]string)
	}

	// Set the necessary encryption headers, for future decryption.
	metaData[amzHeaderIV] = []string{encryptMaterials.GetIV()}
	metaData[amzHeaderKey] = []string{encryptMaterials.GetKey()}
	metaData[amzHeaderMatDesc] = []string{encryptMaterials.GetDesc()}

	return c.PutObjectWithMetadata(bucketName, objectName, encryptMaterials, metaData, progress)
}

// PutObjectWithMetadata - with metadata.
func (c Client) PutObjectWithMetadata(bucketName, objectName string, reader io.Reader, metaData map[string][]string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}
	if reader == nil {
		return 0, ErrInvalidArgument("Input reader is invalid, cannot be nil.")
	}

	// Size of the object.
	var size int64

	// Get reader size.
	size, err = getReaderSize(reader)
	if err != nil {
		return 0, err
	}

	// Check for largest object size allowed.
	if size > int64(maxMultipartPutObjectSize) {
		return 0, ErrEntityTooLarge(size, maxMultipartPutObjectSize, bucketName, objectName)
	}

	// NOTE: Google Cloud Storage does not implement Amazon S3 Compatible multipart PUT.
	// So we fall back to single PUT operation with the maximum limit of 5GiB.
	if s3utils.IsGoogleEndpoint(c.endpointURL) {
		if size <= -1 {
			return 0, ErrorResponse{
				Code:       "NotImplemented",
				Message:    "Content-Length cannot be negative for file uploads to Google Cloud Storage.",
				Key:        objectName,
				BucketName: bucketName,
			}
		}
		if size > maxSinglePutObjectSize {
			return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
		}
		// Do not compute MD5 for Google Cloud Storage. Uploads up to 5GiB in size.
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, metaData, progress)
	}

	// putSmall object.
	if size < minPartSize && size >= 0 {
		return c.putObjectSingle(bucketName, objectName, reader, size, metaData, progress)
	}

	// For all sizes greater than 5MiB do multipart.
	n, err = c.putObjectMultipart(bucketName, objectName, reader, size, metaData, progress)
	if err != nil {
		errResp := ToErrorResponse(err)
		// Verify if multipart functionality is not available, if not
		// fall back to single PutObject operation.
		if errResp.Code == "AccessDenied" && strings.Contains(errResp.Message, "Access Denied") {
			// Verify if size of reader is greater than '5GiB'.
			if size > maxSinglePutObjectSize {
				return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
			}
			// Fall back to uploading as single PutObject operation.
			return c.putObjectSingle(bucketName, objectName, reader, size, metaData, progress)
		}
		return n, err
	}
	return n, nil
}

// PutObjectStreaming using AWS streaming signature V4
func (c Client) PutObjectStreaming(bucketName, objectName string, reader io.Reader) (n int64, err error) {
	return c.PutObjectStreamingWithProgress(bucketName, objectName, reader, nil, nil)
}

// PutObjectStreamingWithMetadata using AWS streaming signature V4
func (c Client) PutObjectStreamingWithMetadata(bucketName, objectName string, reader io.Reader, metadata map[string][]string) (n int64, err error) {
	return c.PutObjectStreamingWithProgress(bucketName, objectName, reader, metadata, nil)
}

// PutObjectStreamingWithProgress using AWS streaming signature V4
func (c Client) PutObjectStreamingWithProgress(bucketName, objectName string, reader io.Reader, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	// NOTE: Streaming signature is not supported by GCS.
	if s3utils.IsGoogleEndpoint(c.endpointURL) {
		return 0, ErrorResponse{
			Code:       "NotImplemented",
			Message:    "AWS streaming signature v4 is not supported with Google Cloud Storage",
			Key:        objectName,
			BucketName: bucketName,
		}
	}

	if c.overrideSignerType.IsV2() {
		return 0, ErrorResponse{
			Code:       "NotImplemented",
			Message:    "AWS streaming signature v4 is not supported with minio client initialized for AWS signature v2",
			Key:        objectName,
			BucketName: bucketName,
		}
	}

	// Size of the object.
	var size int64

	// Get reader size.
	size, err = getReaderSize(reader)
	if err != nil {
		return 0, err
	}

	// Check for largest object size allowed.
	if size > int64(maxMultipartPutObjectSize) {
		return 0, ErrEntityTooLarge(size, maxMultipartPutObjectSize, bucketName, objectName)
	}

	// If size cannot be found on a stream, it is not possible
	// to upload using streaming signature, fall back to multipart.
	if size < 0 {
		return c.putObjectMultipartStream(bucketName, objectName, reader, size, metadata, progress)
	}

	// Set streaming signature.
	c.overrideSignerType = credentials.SignatureV4Streaming

	if size < minPartSize && size >= 0 {
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
	}

	// For all sizes greater than 64MiB do multipart.
	n, err = c.putObjectMultipartStreamNoChecksum(bucketName, objectName, reader, size, metadata, progress)
	if err != nil {
		errResp := ToErrorResponse(err)
		// Verify if multipart functionality is not available, if not
		// fall back to single PutObject operation.
		if errResp.Code == "AccessDenied" && strings.Contains(errResp.Message, "Access Denied") {
			// Verify if size of reader is greater than '5GiB'.
			if size > maxSinglePutObjectSize {
				return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
			}
			// Fall back to uploading as single PutObject operation.
			return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
		}
		return n, err
	}

	return n, nil
}
