/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 Minio, Inc.
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
	"bytes"
	"context"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/pkg/s3utils"
)

/// Bucket operations

// MakeBucket creates a new bucket with bucketName.
//
// Location is an optional argument, by default all buckets are
// created in US Standard Region.
//
// For Amazon S3 for more supported regions - http://docs.aws.amazon.com/general/latest/gr/rande.html
// For Google Cloud Storage for more supported regions - https://cloud.google.com/storage/docs/bucket-locations
func (c Client) MakeBucket(bucketName string, location string) (err error) {
	defer func() {
		// Save the location into cache on a successful makeBucket response.
		if err == nil {
			c.bucketLocCache.Set(bucketName, location)
		}
	}()

	// Validate the input arguments.
	if err := s3utils.CheckValidBucketNameStrict(bucketName); err != nil {
		return err
	}

	// If location is empty, treat is a default region 'us-east-1'.
	if location == "" {
		location = "us-east-1"
		// For custom region clients, default
		// to custom region instead not 'us-east-1'.
		if c.region != "" {
			location = c.region
		}
	}
	// PUT bucket request metadata.
	reqMetadata := requestMetadata{
		bucketName:     bucketName,
		bucketLocation: location,
	}

	// If location is not 'us-east-1' create bucket location config.
	if location != "us-east-1" && location != "" {
		createBucketConfig := createBucketConfiguration{}
		createBucketConfig.Location = location
		var createBucketConfigBytes []byte
		createBucketConfigBytes, err = xml.Marshal(createBucketConfig)
		if err != nil {
			return err
		}
		reqMetadata.contentMD5Base64 = sumMD5Base64(createBucketConfigBytes)
		reqMetadata.contentSHA256Hex = sum256Hex(createBucketConfigBytes)
		reqMetadata.contentBody = bytes.NewReader(createBucketConfigBytes)
		reqMetadata.contentLength = int64(len(createBucketConfigBytes))
	}

	// Execute PUT to create a new bucket.
	resp, err := c.executeMethod(context.Background(), "PUT", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return httpRespToErrorResponse(resp, bucketName, "")
		}
	}

	// Success.
	return nil
}

// SetBucketPolicy set the access permissions on an existing bucket.
func (c Client) SetBucketPolicy(bucketName, policy string) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}

	// If policy is empty then delete the bucket policy.
	if policy == "" {
		return c.removeBucketPolicy(bucketName)
	}

	// Save the updated policies.
	return c.putBucketPolicy(bucketName, policy)
}

// Saves a new bucket policy.
func (c Client) putBucketPolicy(bucketName, policy string) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}

	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	// Content-length is mandatory for put policy request
	policyReader := strings.NewReader(policy)
	b, err := ioutil.ReadAll(policyReader)
	if err != nil {
		return err
	}

	reqMetadata := requestMetadata{
		bucketName:    bucketName,
		queryValues:   urlValues,
		contentBody:   policyReader,
		contentLength: int64(len(b)),
	}

	// Execute PUT to upload a new bucket policy.
	resp, err := c.executeMethod(context.Background(), "PUT", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusNoContent {
			return httpRespToErrorResponse(resp, bucketName, "")
		}
	}
	return nil
}

// Removes all policies on a bucket.
func (c Client) removeBucketPolicy(bucketName string) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}
	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	// Execute DELETE on objectName.
	resp, err := c.executeMethod(context.Background(), "DELETE", requestMetadata{
		bucketName:       bucketName,
		queryValues:      urlValues,
		contentSHA256Hex: emptySHA256Hex,
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	return nil
}

// SetBucketNotification saves a new bucket notification.
func (c Client) SetBucketNotification(bucketName string, bucketNotification BucketNotification) error {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return err
	}

	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("notification", "")

	notifBytes, err := xml.Marshal(bucketNotification)
	if err != nil {
		return err
	}

	notifBuffer := bytes.NewReader(notifBytes)
	reqMetadata := requestMetadata{
		bucketName:       bucketName,
		queryValues:      urlValues,
		contentBody:      notifBuffer,
		contentLength:    int64(len(notifBytes)),
		contentMD5Base64: sumMD5Base64(notifBytes),
		contentSHA256Hex: sum256Hex(notifBytes),
	}

	// Execute PUT to upload a new bucket notification.
	resp, err := c.executeMethod(context.Background(), "PUT", reqMetadata)
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

// RemoveAllBucketNotification - Remove bucket notification clears all previously specified config
func (c Client) RemoveAllBucketNotification(bucketName string) error {
	return c.SetBucketNotification(bucketName, BucketNotification{})
}
