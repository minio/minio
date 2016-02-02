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
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/url"
)

/// Bucket operations

// MakeBucket makes a new bucket.
//
// Optional arguments are acl and location - by default all buckets are created
// with ``private`` acl and in US Standard region.
//
// ACL valid values - http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
//
//  private - owner gets full access [default].
//  public-read - owner gets full access, all others get read access.
//  public-read-write - owner gets full access, all others get full access too.
//  authenticated-read - owner gets full access, authenticated users get read access.
//
// For Amazon S3 for more supported regions - http://docs.aws.amazon.com/general/latest/gr/rande.html
// For Google Cloud Storage for more supported regions - https://cloud.google.com/storage/docs/bucket-locations
func (c Client) MakeBucket(bucketName string, acl BucketACL, location string) error {
	// Validate the input arguments.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	if !acl.isValidBucketACL() {
		return ErrInvalidArgument("Unrecognized ACL " + acl.String())
	}

	// If location is empty, treat is a default region 'us-east-1'.
	if location == "" {
		location = "us-east-1"
	}

	// Instantiate the request.
	req, err := c.makeBucketRequest(bucketName, acl, location)
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

	// Save the location into cache on a successfull makeBucket response.
	c.bucketLocCache.Set(bucketName, location)

	// Return.
	return nil
}

// makeBucketRequest constructs request for makeBucket.
func (c Client) makeBucketRequest(bucketName string, acl BucketACL, location string) (*http.Request, error) {
	// Validate input arguments.
	if err := isValidBucketName(bucketName); err != nil {
		return nil, err
	}
	if !acl.isValidBucketACL() {
		return nil, ErrInvalidArgument("Unrecognized ACL " + acl.String())
	}

	// In case of Amazon S3.  The make bucket issued on already
	// existing bucket would fail with 'AuthorizationMalformed' error
	// if virtual style is used. So we default to 'path style' as that
	// is the preferred method here. The final location of the
	// 'bucket' is provided through XML LocationConstraint data with
	// the request.
	targetURL := *c.endpointURL
	targetURL.Path = "/" + bucketName + "/"

	// get a new HTTP request for the method.
	req, err := http.NewRequest("PUT", targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// by default bucket acl is set to private.
	req.Header.Set("x-amz-acl", "private")
	if acl != "" {
		req.Header.Set("x-amz-acl", string(acl))
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
		req.ContentLength = int64(createBucketConfigBuffer.Len())
		if c.signature.isV4() {
			req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum256(createBucketConfigBuffer.Bytes())))
		}
	}

	// Sign the request.
	if c.signature.isV4() {
		// Signature calculated for MakeBucket request should be for 'us-east-1',
		// regardless of the bucket's location constraint.
		req = signV4(*req, c.accessKeyID, c.secretAccessKey, "us-east-1")
	} else if c.signature.isV2() {
		req = signV2(*req, c.accessKeyID, c.secretAccessKey)
	}

	// Return signed request.
	return req, nil
}

// SetBucketACL set the permissions on an existing bucket using access control lists (ACL).
//
// For example
//
//  private - owner gets full access [default].
//  public-read - owner gets full access, all others get read access.
//  public-read-write - owner gets full access, all others get full access too.
//  authenticated-read - owner gets full access, authenticated users get read access.
func (c Client) SetBucketACL(bucketName string, acl BucketACL) error {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return err
	}
	if !acl.isValidBucketACL() {
		return ErrInvalidArgument("Unrecognized ACL " + acl.String())
	}

	// Set acl query.
	urlValues := make(url.Values)
	urlValues.Set("acl", "")

	// Add misc headers.
	customHeader := make(http.Header)

	if acl != "" {
		customHeader.Set("x-amz-acl", acl.String())
	} else {
		customHeader.Set("x-amz-acl", "private")
	}

	// Instantiate a new request.
	req, err := c.newRequest("PUT", requestMetadata{
		bucketName:   bucketName,
		queryValues:  urlValues,
		customHeader: customHeader,
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
		// if error return.
		if resp.StatusCode != http.StatusOK {
			return httpRespToErrorResponse(resp, bucketName, "")
		}
	}

	// return
	return nil
}
