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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
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

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// getEndpointURL - construct a new endpoint.
func getEndpointURL(endpoint string, inSecure bool) (*url.URL, error) {
	if strings.Contains(endpoint, ":") {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, err
		}
		if !isValidIP(host) && !isValidDomain(host) {
			msg := "Endpoint: " + endpoint + " does not follow ip address or domain name standards."
			return nil, ErrInvalidArgument(msg)
		}
	} else {
		if !isValidIP(endpoint) && !isValidDomain(endpoint) {
			msg := "Endpoint: " + endpoint + " does not follow ip address or domain name standards."
			return nil, ErrInvalidArgument(msg)
		}
	}
	// if inSecure is true, use 'http' scheme.
	scheme := "https"
	if inSecure {
		scheme = "http"
	}

	// Construct a secured endpoint URL.
	endpointURLStr := scheme + "://" + endpoint
	endpointURL, err := url.Parse(endpointURLStr)
	if err != nil {
		return nil, err
	}

	// Validate incoming endpoint URL.
	if err := isValidEndpointURL(endpointURL); err != nil {
		return nil, err
	}
	return endpointURL, nil
}

// isValidDomain validates if input string is a valid domain name.
func isValidDomain(host string) bool {
	// See RFC 1035, RFC 3696.
	host = strings.TrimSpace(host)
	if len(host) == 0 || len(host) > 255 {
		return false
	}
	// host cannot start or end with "-"
	if host[len(host)-1:] == "-" || host[:1] == "-" {
		return false
	}
	// host cannot start or end with "_"
	if host[len(host)-1:] == "_" || host[:1] == "_" {
		return false
	}
	// host cannot start or end with a "."
	if host[len(host)-1:] == "." || host[:1] == "." {
		return false
	}
	// All non alphanumeric characters are invalid.
	if strings.ContainsAny(host, "`~!@#$%^&*()+={}[]|\\\"';:><?/") {
		return false
	}
	// No need to regexp match, since the list is non-exhaustive.
	// We let it valid and fail later.
	return true
}

// isValidIP parses input string for ip address validity.
func isValidIP(ip string) bool {
	return net.ParseIP(ip) != nil
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

// isVirtualHostSupported - verifies if bucketName can be part of
// virtual host. Currently only Amazon S3 and Google Cloud Storage would
// support this.
func isVirtualHostSupported(endpointURL *url.URL, bucketName string) bool {
	// bucketName can be valid but '.' in the hostname will fail SSL
	// certificate validation. So do not use host-style for such buckets.
	if endpointURL.Scheme == "https" && strings.Contains(bucketName, ".") {
		return false
	}
	// Return true for all other cases
	return isAmazonEndpoint(endpointURL) || isGoogleEndpoint(endpointURL)
}

// Match if it is exactly Amazon S3 endpoint.
func isAmazonEndpoint(endpointURL *url.URL) bool {
	if endpointURL == nil {
		return false
	}
	if endpointURL.Host == "s3.amazonaws.com" {
		return true
	}
	return false
}

// Match if it is exactly Google cloud storage endpoint.
func isGoogleEndpoint(endpointURL *url.URL) bool {
	if endpointURL == nil {
		return false
	}
	if endpointURL.Host == "storage.googleapis.com" {
		return true
	}
	return false
}

// Verify if input endpoint URL is valid.
func isValidEndpointURL(endpointURL *url.URL) error {
	if endpointURL == nil {
		return ErrInvalidArgument("Endpoint url cannot be empty.")
	}
	if endpointURL.Path != "/" && endpointURL.Path != "" {
		return ErrInvalidArgument("Endpoint url cannot have fully qualified paths.")
	}
	if strings.Contains(endpointURL.Host, ".amazonaws.com") {
		if !isAmazonEndpoint(endpointURL) {
			return ErrInvalidArgument("Amazon S3 endpoint should be 's3.amazonaws.com'.")
		}
	}
	if strings.Contains(endpointURL.Host, ".googleapis.com") {
		if !isGoogleEndpoint(endpointURL) {
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
	if match, _ := regexp.MatchString("\\.\\.", bucketName); match == true {
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

// queryEncode - encodes query values in their URL encoded form.
func queryEncode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := urlEncodePath(k) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(urlEncodePath(v))
		}
	}
	return buf.String()
}

// urlEncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncodePath(pathName string) string {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	if reservedNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}
