/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2016 Minio, Inc.
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
	"net/http"

	"github.com/minio/minio-go/pkg/s3utils"
)

// CopyObject - copy a source object into a new object with the provided name in the provided bucket
func (c Client) CopyObject(bucketName string, objectName string, objectSource string, cpCond CopyConditions) error {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	if err := isValidObjectName(objectName); err != nil {
		return err
	}
	if objectSource == "" {
		return ErrInvalidArgument("Object source cannot be empty.")
	}

	// customHeaders apply headers.
	customHeaders := make(http.Header)
	for _, cond := range cpCond.conditions {
		customHeaders.Set(cond.key, cond.value)
	}

	// Set copy source.
	customHeaders.Set("x-amz-copy-source", s3utils.EncodePath(objectSource))

	// Execute PUT on objectName.
	resp, err := c.executeMethod("PUT", requestMetadata{
		bucketName:   bucketName,
		objectName:   objectName,
		customHeader: customHeaders,
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	// Decode copy response on success.
	cpObjRes := copyObjectResult{}
	err = xmlDecoder(resp.Body, &cpObjRes)
	if err != nil {
		return err
	}

	// Return nil on success.
	return nil
}
