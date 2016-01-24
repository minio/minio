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
	"net/http"
	"strconv"
	"strings"
	"time"
)

// BucketExists verify if bucket exists and you have permission to access it.
func (c Client) BucketExists(bucketName string) error {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	// Instantiate a new request.
	req, err := c.newRequest("HEAD", requestMetadata{
		bucketName: bucketName,
	})
	if err != nil {
		return err
	}
	// Initiate the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return httpRespToErrorResponse(resp, bucketName, "")
		}
	}
	return nil
}

// StatObject verifies if object exists and you have permission to access.
func (c Client) StatObject(bucketName, objectName string) (ObjectInfo, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return ObjectInfo{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return ObjectInfo{}, err
	}
	// Instantiate a new request.
	req, err := c.newRequest("HEAD", requestMetadata{
		bucketName: bucketName,
		objectName: objectName,
	})
	if err != nil {
		return ObjectInfo{}, err
	}
	// Initiate the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return ObjectInfo{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return ObjectInfo{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	// Trim off the odd double quotes from ETag in the beginning and end.
	md5sum := strings.TrimPrefix(resp.Header.Get("ETag"), "\"")
	md5sum = strings.TrimSuffix(md5sum, "\"")

	// Parse content length.
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return ObjectInfo{}, ErrorResponse{
			Code:            "InternalError",
			Message:         "Content-Length is invalid. " + reportIssue,
			BucketName:      bucketName,
			Key:             objectName,
			RequestID:       resp.Header.Get("x-amz-request-id"),
			HostID:          resp.Header.Get("x-amz-id-2"),
			AmzBucketRegion: resp.Header.Get("x-amz-bucket-region"),
		}
	}
	// Parse Last-Modified has http time format.
	date, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	if err != nil {
		return ObjectInfo{}, ErrorResponse{
			Code:            "InternalError",
			Message:         "Last-Modified time format is invalid. " + reportIssue,
			BucketName:      bucketName,
			Key:             objectName,
			RequestID:       resp.Header.Get("x-amz-request-id"),
			HostID:          resp.Header.Get("x-amz-id-2"),
			AmzBucketRegion: resp.Header.Get("x-amz-bucket-region"),
		}
	}
	// Fetch content type if any present.
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// Save object metadata info.
	var objectStat ObjectInfo
	objectStat.ETag = md5sum
	objectStat.Key = objectName
	objectStat.Size = size
	objectStat.LastModified = date
	objectStat.ContentType = contentType
	return objectStat, nil
}
