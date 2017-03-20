/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015, 2016 Minio, Inc.
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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/s3signer"
)

/// Bucket operations

// MakeBucket creates a new bucket with bucketName.
//
// Location is an optional argument, by default all buckets are
// created in US Standard Region.
//
// For Amazon S3 for more supported regions - http://docs.aws.amazon.com/general/latest/gr/rande.html
// For Google Cloud Storage for more supported regions - https://cloud.google.com/storage/docs/bucket-locations
func (c Client) MakeBucket(bucketName string, location string) error {
	// Validate the input arguments.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}

	// If location is empty, treat is a default region 'us-east-1'.
	if location == "" {
		location = "us-east-1"
	}

	// Instantiate the request.
	req, err := c.makeBucketRequest(bucketName, location)
	if err != nil {
		return err
	}

	// Execute the request.
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

	// Save the location into cache on a successful makeBucket response.
	c.bucketLocCache.Set(bucketName, location)

	// Return.
	return nil
}

// makeBucketRequest constructs request for makeBucket.
func (c Client) makeBucketRequest(bucketName string, location string) (*http.Request, error) {
	// Validate input arguments.
	if err := isValidBucketName(bucketName); err != nil {
		return nil, err
	}

	// In case of Amazon S3.  The make bucket issued on already
	// existing bucket would fail with 'AuthorizationMalformed' error
	// if virtual style is used. So we default to 'path style' as that
	// is the preferred method here. The final location of the
	// 'bucket' is provided through XML LocationConstraint data with
	// the request.
	targetURL := c.endpointURL
	targetURL.Path = path.Join(bucketName, "") + "/"

	// get a new HTTP request for the method.
	req, err := http.NewRequest("PUT", targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// set UserAgent for the request.
	c.setUserAgent(req)

	// set sha256 sum for signature calculation only with signature version '4'.
	if c.signature.isV4() {
		req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum256([]byte{})))
	}

	// If location is not 'us-east-1' create bucket location config.
	if location != "us-east-1" && location != "" {
		createBucketConfig := createBucketConfiguration{}
		createBucketConfig.Location = location
		var createBucketConfigBytes []byte
		createBucketConfigBytes, err = xml.Marshal(createBucketConfig)
		if err != nil {
			return nil, err
		}
		createBucketConfigBuffer := bytes.NewBuffer(createBucketConfigBytes)
		req.Body = ioutil.NopCloser(createBucketConfigBuffer)
		req.ContentLength = int64(len(createBucketConfigBytes))
		// Set content-md5.
		req.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(sumMD5(createBucketConfigBytes)))
		if c.signature.isV4() {
			// Set sha256.
			req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum256(createBucketConfigBytes)))
		}
	}

	// Sign the request.
	if c.signature.isV4() {
		// Signature calculated for MakeBucket request should be for 'us-east-1',
		// regardless of the bucket's location constraint.
		req = s3signer.SignV4(*req, c.accessKeyID, c.secretAccessKey, "us-east-1")
	} else if c.signature.isV2() {
		req = s3signer.SignV2(*req, c.accessKeyID, c.secretAccessKey)
	}

	// Return signed request.
	return req, nil
}

// SetBucketPolicy set the access permissions on an existing bucket.
//
// For example
//
//  none - owner gets full access [default].
//  readonly - anonymous get access for everyone at a given object prefix.
//  readwrite - anonymous list/put/delete access to a given object prefix.
//  writeonly - anonymous put/delete access to a given object prefix.
func (c Client) SetBucketPolicy(bucketName string, objectPrefix string, bucketPolicy policy.BucketPolicy) error {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	if err := isValidObjectPrefix(objectPrefix); err != nil {
		return err
	}
	if !bucketPolicy.IsValidBucketPolicy() {
		return ErrInvalidArgument(fmt.Sprintf("Invalid bucket policy provided. %s", bucketPolicy))
	}
	policyInfo, err := c.getBucketPolicy(bucketName, objectPrefix)
	if err != nil {
		return err
	}

	if bucketPolicy == policy.BucketPolicyNone && policyInfo.Statements == nil {
		// As the request is for removing policy and the bucket
		// has empty policy statements, just return success.
		return nil
	}

	policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, bucketPolicy, bucketName, objectPrefix)

	// Save the updated policies.
	return c.putBucketPolicy(bucketName, policyInfo)
}

// Saves a new bucket policy.
func (c Client) putBucketPolicy(bucketName string, policyInfo policy.BucketAccessPolicy) error {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}

	// If there are no policy statements, we should remove entire policy.
	if len(policyInfo.Statements) == 0 {
		return c.removeBucketPolicy(bucketName)
	}

	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	policyBytes, err := json.Marshal(&policyInfo)
	if err != nil {
		return err
	}

	policyBuffer := bytes.NewReader(policyBytes)
	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		queryValues:        urlValues,
		contentBody:        policyBuffer,
		contentLength:      int64(len(policyBytes)),
		contentMD5Bytes:    sumMD5(policyBytes),
		contentSHA256Bytes: sum256(policyBytes),
	}

	// Execute PUT to upload a new bucket policy.
	resp, err := c.executeMethod("PUT", reqMetadata)
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
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	// Execute DELETE on objectName.
	resp, err := c.executeMethod("DELETE", requestMetadata{
		bucketName:  bucketName,
		queryValues: urlValues,
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
	if err := isValidBucketName(bucketName); err != nil {
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
		bucketName:         bucketName,
		queryValues:        urlValues,
		contentBody:        notifBuffer,
		contentLength:      int64(len(notifBytes)),
		contentMD5Bytes:    sumMD5(notifBytes),
		contentSHA256Bytes: sum256(notifBytes),
	}

	// Execute PUT to upload a new bucket notification.
	resp, err := c.executeMethod("PUT", reqMetadata)
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
