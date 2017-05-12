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
	"crypto/md5"
	"crypto/sha256"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/minio/minio-go/pkg/s3utils"
)

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

// sum256 calculate sha256 sum for an input byte array.
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumMD5 calculate sumMD5 sum for an input byte array.
func sumMD5(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// getEndpointURL - construct a new endpoint.
func getEndpointURL(endpoint string, secure bool) (*url.URL, error) {
	if strings.Contains(endpoint, ":") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, err
		}
		if !s3utils.IsValidIP(host) && !s3utils.IsValidDomain(host) {
			msg := "Endpoint: " + endpoint + " does not follow ip address or domain name standards."
			return nil, ErrInvalidArgument(msg)
		}
	} else {
		if !s3utils.IsValidIP(endpoint) && !s3utils.IsValidDomain(endpoint) {
			msg := "Endpoint: " + endpoint + " does not follow ip address or domain name standards."
			return nil, ErrInvalidArgument(msg)
		}
	}
	// If secure is false, use 'http' scheme.
	scheme := "https"
	if !secure {
		scheme = "http"
	}

	// Construct a secured endpoint URL.
	endpointURLStr := scheme + "://" + endpoint
	endpointURL, err := url.Parse(endpointURLStr)
	if err != nil {
		return nil, err
	}

	// Validate incoming endpoint URL.
	if err := isValidEndpointURL(*endpointURL); err != nil {
		return nil, err
	}
	return endpointURL, nil
}

// closeResponse close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func closeResponse(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

var emptySHA256 = sum256(nil)

// Sentinel URL is the default url value which is invalid.
var sentinelURL = url.URL{}

// Verify if input endpoint URL is valid.
func isValidEndpointURL(endpointURL url.URL) error {
	if endpointURL == sentinelURL {
		return ErrInvalidArgument("Endpoint url cannot be empty.")
	}
	if endpointURL.Path != "/" && endpointURL.Path != "" {
		return ErrInvalidArgument("Endpoint url cannot have fully qualified paths.")
	}
	if strings.Contains(endpointURL.Host, ".amazonaws.com") {
		if !s3utils.IsAmazonEndpoint(endpointURL) {
			return ErrInvalidArgument("Amazon S3 endpoint should be 's3.amazonaws.com'.")
		}
	}
	if strings.Contains(endpointURL.Host, ".googleapis.com") {
		if !s3utils.IsGoogleEndpoint(endpointURL) {
			return ErrInvalidArgument("Google Cloud Storage endpoint should be 'storage.googleapis.com'.")
		}
	}
	return nil
}

// Verify if input expires value is valid.
func isValidExpiry(expires time.Duration) error {
	expireSeconds := int64(expires / time.Second)
	if expireSeconds < 1 {
		return ErrInvalidArgument("Expires cannot be lesser than 1 second.")
	}
	if expireSeconds > 604800 {
		return ErrInvalidArgument("Expires cannot be greater than 7 days.")
	}
	return nil
}

// We support '.' with bucket names but we fallback to using path
// style requests instead for such buckets.
var validBucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)

// Invalid bucket name with double dot.
var invalidDotBucketName = regexp.MustCompile(`\.\.`)

// isValidBucketName - verify bucket name in accordance with
//  - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func isValidBucketName(bucketName string) error {
	if strings.TrimSpace(bucketName) == "" {
		return ErrInvalidBucketName("Bucket name cannot be empty.")
	}
	if len(bucketName) < 3 {
		return ErrInvalidBucketName("Bucket name cannot be smaller than 3 characters.")
	}
	if len(bucketName) > 63 {
		return ErrInvalidBucketName("Bucket name cannot be greater than 63 characters.")
	}
	if bucketName[0] == '.' || bucketName[len(bucketName)-1] == '.' {
		return ErrInvalidBucketName("Bucket name cannot start or end with a '.' dot.")
	}
	if invalidDotBucketName.MatchString(bucketName) {
		return ErrInvalidBucketName("Bucket name cannot have successive periods.")
	}
	if !validBucketName.MatchString(bucketName) {
		return ErrInvalidBucketName("Bucket name contains invalid characters.")
	}
	return nil
}

// isValidObjectName - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func isValidObjectName(objectName string) error {
	if strings.TrimSpace(objectName) == "" {
		return ErrInvalidObjectName("Object name cannot be empty.")
	}
	if len(objectName) > 1024 {
		return ErrInvalidObjectName("Object name cannot be greater than 1024 characters.")
	}
	if !utf8.ValidString(objectName) {
		return ErrInvalidBucketName("Object name with non UTF-8 strings are not supported.")
	}
	return nil
}

// isValidObjectPrefix - verify if object prefix is valid.
func isValidObjectPrefix(objectPrefix string) error {
	if len(objectPrefix) > 1024 {
		return ErrInvalidObjectPrefix("Object prefix cannot be greater than 1024 characters.")
	}
	if !utf8.ValidString(objectPrefix) {
		return ErrInvalidObjectPrefix("Object prefix with non UTF-8 strings are not supported.")
	}
	return nil
}

// make a copy of http.Header
func cloneHeader(h http.Header) http.Header {
	h2 := make(http.Header, len(h))
	for k, vv := range h {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2
	}
	return h2
}

// Filter relevant response headers from
// the HEAD, GET http response. The function takes
// a list of headers which are filtered out and
// returned as a new http header.
func filterHeader(header http.Header, filterKeys []string) (filteredHeader http.Header) {
	filteredHeader = cloneHeader(header)
	for _, key := range filterKeys {
		filteredHeader.Del(key)
	}
	return filteredHeader
}
