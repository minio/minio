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
	"encoding/hex"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
)

// bucketLocationCache - Provides simple mechansim to hold bucket
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

// getBucketLocation - Get location for the bucketName from location map cache.
func (c Client) getBucketLocation(bucketName string) (string, error) {
	// For anonymous requests, default to "us-east-1" and let other calls
	// move forward.
	if c.anonymous {
		return "us-east-1", nil
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
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return "", httpRespToErrorResponse(resp, bucketName, "")
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
	c.bucketLocCache.Set(bucketName, location)

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
	targetURL.Path = filepath.Join(bucketName, "") + "/"
	targetURL.RawQuery = urlValues.Encode()

	// Get a new HTTP request for the method.
	req, err := http.NewRequest("GET", targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set UserAgent for the request.
	c.setUserAgent(req)

	// Set sha256 sum for signature calculation only with signature version '4'.
	if c.signature.isV4() {
		req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum256([]byte{})))
	}

	// Sign the request.
	if c.signature.isV4() {
		req = signV4(*req, c.accessKeyID, c.secretAccessKey, "us-east-1")
	} else if c.signature.isV2() {
		req = signV2(*req, c.accessKeyID, c.secretAccessKey)
	}
	return req, nil
}
