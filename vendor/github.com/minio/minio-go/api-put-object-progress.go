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

import "io"

// PutObjectWithProgress - With progress.
func (c Client) PutObjectWithProgress(bucketName, objectName string, reader io.Reader, contentType string, progress io.Reader) (n int64, err error) {
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
	if isGoogleEndpoint(c.endpointURL) {
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
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, contentType, progress)
	}

	// NOTE: S3 doesn't allow anonymous multipart requests.
	if isAmazonEndpoint(c.endpointURL) && c.anonymous {
		if size <= -1 {
			return 0, ErrorResponse{
				Code:       "NotImplemented",
				Message:    "Content-Length cannot be negative for anonymous requests.",
				Key:        objectName,
				BucketName: bucketName,
			}
		}
		if size > maxSinglePutObjectSize {
			return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
		}
		// Do not compute MD5 for anonymous requests to Amazon
		// S3. Uploads up to 5GiB in size.
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, contentType, progress)
	}

	// putSmall object.
	if size < minPartSize && size >= 0 {
		return c.putObjectSingle(bucketName, objectName, reader, size, contentType, progress)
	}
	// For all sizes greater than 5MiB do multipart.
	n, err = c.putObjectMultipart(bucketName, objectName, reader, size, contentType, progress)
	if err != nil {
		errResp := ToErrorResponse(err)
		// Verify if multipart functionality is not available, if not
		// fall back to single PutObject operation.
		if errResp.Code == "AccessDenied" && errResp.Message == "Access Denied." {
			// Verify if size of reader is greater than '5GiB'.
			if size > maxSinglePutObjectSize {
				return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
			}
			// Fall back to uploading as single PutObject operation.
			return c.putObjectSingle(bucketName, objectName, reader, size, contentType, progress)
		}
		return n, err
	}
	return n, nil
}
