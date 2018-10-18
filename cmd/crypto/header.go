// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"strings"
)

// SSEHeader is the general AWS SSE HTTP header key.
const SSEHeader = "X-Amz-Server-Side-Encryption"

const (
	// SSEKmsID is the HTTP header key referencing the SSE-KMS
	// key ID.
	SSEKmsID = SSEHeader + "-Aws-Kms-Key-Id"

	// SSEKmsContext is the HTTP header key referencing the
	// SSE-KMS encryption context.
	SSEKmsContext = SSEHeader + "-Context"
)

const (
	// SSECAlgorithm is the HTTP header key referencing
	// the SSE-C algorithm.
	SSECAlgorithm = SSEHeader + "-Customer-Algorithm"

	// SSECKey is the HTTP header key referencing the
	// SSE-C client-provided key..
	SSECKey = SSEHeader + "-Customer-Key"

	// SSECKeyMD5 is the HTTP header key referencing
	// the MD5 sum of the client-provided key.
	SSECKeyMD5 = SSEHeader + "-Customer-Key-Md5"
)

const (
	// SSECopyAlgorithm is the HTTP header key referencing
	// the SSE-C algorithm for SSE-C copy requests.
	SSECopyAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"

	// SSECopyKey is the HTTP header key referencing the SSE-C
	// client-provided key for SSE-C copy requests.
	SSECopyKey = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"

	// SSECopyKeyMD5 is the HTTP header key referencing the
	// MD5 sum of the client key for SSE-C copy requests.
	SSECopyKeyMD5 = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"
)

const (
	// SSEAlgorithmAES256 is the only supported value for the SSE-S3 or SSE-C algorithm header.
	// For SSE-S3 see: https://docs.aws.amazon.com/AmazonS3/latest/dev/SSEUsingRESTAPI.html
	// For SSE-C  see: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
	SSEAlgorithmAES256 = "AES256"

	// SSEAlgorithmKMS is the value of 'X-Amz-Server-Side-Encryption' for SSE-KMS.
	// See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
	SSEAlgorithmKMS = "aws:kms"
)

// RemoveSensitiveHeaders removes confidential encryption
// information - e.g. the SSE-C key - from the HTTP headers.
// It has the same semantics as RemoveSensitiveEntires.
func RemoveSensitiveHeaders(h http.Header) {
	h.Del(SSECKey)
	h.Del(SSECopyKey)
}

// S3 represents AWS SSE-S3. It provides functionality to handle
// SSE-S3 requests.
var S3 = s3{}

type s3 struct{}

// IsRequested returns true if the HTTP headers indicates that
// the S3 client requests SSE-S3.
func (s3) IsRequested(h http.Header) bool {
	_, ok := h[SSEHeader]
	return ok && strings.ToLower(h.Get(SSEHeader)) != SSEAlgorithmKMS // Return only true if the SSE header is specified and does not contain the SSE-KMS value
}

// ParseHTTP parses the SSE-S3 related HTTP headers and checks
// whether they contain valid values.
func (s3) ParseHTTP(h http.Header) (err error) {
	if h.Get(SSEHeader) != SSEAlgorithmAES256 {
		err = ErrInvalidEncryptionMethod
	}
	return
}

// S3KMS represents AWS SSE-KMS. It provides functionality to
// handle SSE-KMS requests.
var S3KMS = s3KMS{}

type s3KMS struct{}

// IsRequested returns true if the HTTP headers indicates that
// the S3 client requests SSE-KMS.
func (s3KMS) IsRequested(h http.Header) bool {
	if _, ok := h[SSEKmsID]; ok {
		return true
	}
	if _, ok := h[SSEKmsContext]; ok {
		return true
	}
	if _, ok := h[SSEHeader]; ok {
		return strings.ToUpper(h.Get(SSEHeader)) != SSEAlgorithmAES256 // Return only true if the SSE header is specified and does not contain the SSE-S3 value
	}
	return false
}

var (
	// SSEC represents AWS SSE-C. It provides functionality to handle
	// SSE-C requests.
	SSEC = ssec{}

	// SSECopy represents AWS SSE-C for copy requests. It provides
	// functionality to handle SSE-C copy requests.
	SSECopy = ssecCopy{}
)

type ssec struct{}
type ssecCopy struct{}

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C header. SSE-C copy headers are ignored.
func (ssec) IsRequested(h http.Header) bool {
	if _, ok := h[SSECAlgorithm]; ok {
		return true
	}
	if _, ok := h[SSECKey]; ok {
		return true
	}
	if _, ok := h[SSECKeyMD5]; ok {
		return true
	}
	return false
}

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C copy header. Regular SSE-C headers
// are ignored.
func (ssecCopy) IsRequested(h http.Header) bool {
	if _, ok := h[SSECopyAlgorithm]; ok {
		return true
	}
	if _, ok := h[SSECopyKey]; ok {
		return true
	}
	if _, ok := h[SSECopyKeyMD5]; ok {
		return true
	}
	return false
}

// ParseHTTP parses the SSE-C headers and returns the SSE-C client key
// on success. SSE-C copy headers are ignored.
func (ssec) ParseHTTP(h http.Header) (key [32]byte, err error) {
	if h.Get(SSECAlgorithm) != SSEAlgorithmAES256 {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(SSECKey) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(SSECKeyMD5) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(SSECKey))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(SSECKeyMD5))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}

// ParseHTTP parses the SSE-C copy headers and returns the SSE-C client key
// on success. Regular SSE-C headers are ignored.
func (ssecCopy) ParseHTTP(h http.Header) (key [32]byte, err error) {
	if h.Get(SSECopyAlgorithm) != SSEAlgorithmAES256 {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(SSECopyKey) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(SSECopyKeyMD5) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(SSECopyKey))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(SSECopyKeyMD5))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}
