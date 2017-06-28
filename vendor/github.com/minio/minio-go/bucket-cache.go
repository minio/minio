/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * (C) 2015, 2016, 2017 Minio, Inc.
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
	"encoding/hex"
	"net/http"
	"net/url"
	"path"
	"sync"

	"github.com/minio/minio-go/pkg/credentials"
	"github.com/minio/minio-go/pkg/s3signer"
	"github.com/minio/minio-go/pkg/s3utils"
)

// bucketLocationCache - Provides simple mechanism to hold bucket
// locations in memory.
type bucketLocationCache struct {
	// mutex is used for handling the concurrent
	// read/write requests for cache.
	sync.RWMutex

	// items holds the cached bucket locations.
	items map[string]string
}

// newBucketLocationCache - Provides a new bucket location cache to be
// used internally with the client object.
func newBucketLocationCache() *bucketLocationCache {
	return &bucketLocationCache{
		items: make(map[string]string),
	}
}

// Get - Returns a value of a given key if it exists.
func (r *bucketLocationCache) Get(bucketName string) (location string, ok bool) {
	r.RLock()
	defer r.RUnlock()
	location, ok = r.items[bucketName]
	return
}

// Set - Will persist a value into cache.
func (r *bucketLocationCache) Set(bucketName string, location string) {
	r.Lock()
	defer r.Unlock()
	r.items[bucketName] = location
}

// Delete - Deletes a bucket name from cache.
func (r *bucketLocationCache) Delete(bucketName string) {
	r.Lock()
	defer r.Unlock()
	delete(r.items, bucketName)
}

// GetBucketLocation - get location for the bucket name from location cache, if not
// fetch freshly by making a new request.
func (c Client) GetBucketLocation(bucketName string) (string, error) {
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return "", err
	}
	return c.getBucketLocation(bucketName)
}

// getBucketLocation - Get location for the bucketName from location map cache, if not
// fetch freshly by making a new request.
func (c Client) getBucketLocation(bucketName string) (string, error) {
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return "", err
	}

	// Region set then no need to fetch bucket location.
	if c.region != "" {
		return c.region, nil
	}

	if s3utils.IsAmazonChinaEndpoint(c.endpointURL) {
		// For china specifically we need to set everything to
		// cn-north-1 for now, there is no easier way until AWS S3
		// provides a cleaner compatible API across "us-east-1" and
		// China region.
		return "cn-north-1", nil
	} else if s3utils.IsAmazonGovCloudEndpoint(c.endpointURL) {
		// For us-gov specifically we need to set everything to
		// us-gov-west-1 for now, there is no easier way until AWS S3
		// provides a cleaner compatible API across "us-east-1" and
		// Gov cloud region.
		return "us-gov-west-1", nil
	}

	if location, ok := c.bucketLocCache.Get(bucketName); ok {
		return location, nil
	}

	// Initialize a new request.
	req, err := c.getBucketLocationRequest(bucketName)
	if err != nil {
		return "", err
	}

	// Initiate the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return "", err
	}
	location, err := processBucketLocationResponse(resp, bucketName)
	if err != nil {
		return "", err
	}
	c.bucketLocCache.Set(bucketName, location)
	return location, nil
}

// processes the getBucketLocation http response from the server.
func processBucketLocationResponse(resp *http.Response, bucketName string) (bucketLocation string, err error) {
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			err = httpRespToErrorResponse(resp, bucketName, "")
			errResp := ToErrorResponse(err)
			// For access denied error, it could be an anonymous
			// request. Move forward and let the top level callers
			// succeed if possible based on their policy.
			if errResp.Code == "AccessDenied" {
				return "us-east-1", nil
			}
			return "", err
		}
	}

	// Extract location.
	var locationConstraint string
	err = xmlDecoder(resp.Body, &locationConstraint)
	if err != nil {
		return "", err
	}

	location := locationConstraint
	// Location is empty will be 'us-east-1'.
	if location == "" {
		location = "us-east-1"
	}

	// Location can be 'EU' convert it to meaningful 'eu-west-1'.
	if location == "EU" {
		location = "eu-west-1"
	}

	// Save the location into cache.

	// Return.
	return location, nil
}

// getBucketLocationRequest - Wrapper creates a new getBucketLocation request.
func (c Client) getBucketLocationRequest(bucketName string) (*http.Request, error) {
	// Set location query.
	urlValues := make(url.Values)
	urlValues.Set("location", "")

	// Set get bucket location always as path style.
	targetURL := c.endpointURL
	targetURL.Path = path.Join(bucketName, "") + "/"
	targetURL.RawQuery = urlValues.Encode()

	// Get a new HTTP request for the method.
	req, err := http.NewRequest("GET", targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set UserAgent for the request.
	c.setUserAgent(req)

	// Get credentials from the configured credentials provider.
	value, err := c.credsProvider.Get()
	if err != nil {
		return nil, err
	}

	var (
		signerType      = value.SignerType
		accessKeyID     = value.AccessKeyID
		secretAccessKey = value.SecretAccessKey
		sessionToken    = value.SessionToken
	)

	// Custom signer set then override the behavior.
	if c.overrideSignerType != credentials.SignatureDefault {
		signerType = c.overrideSignerType
	}

	// If signerType returned by credentials helper is anonymous,
	// then do not sign regardless of signerType override.
	if value.SignerType == credentials.SignatureAnonymous {
		signerType = credentials.SignatureAnonymous
	}

	// Set sha256 sum for signature calculation only with signature version '4'.
	switch {
	case signerType.IsV4():
		var contentSha256 string
		if c.secure {
			contentSha256 = unsignedPayload
		} else {
			contentSha256 = hex.EncodeToString(sum256([]byte{}))
		}
		req.Header.Set("X-Amz-Content-Sha256", contentSha256)
		req = s3signer.SignV4(*req, accessKeyID, secretAccessKey, sessionToken, "us-east-1")
	case signerType.IsV2():
		req = s3signer.SignV2(*req, accessKeyID, secretAccessKey)
	}

	return req, nil
}
