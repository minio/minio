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
	"net/http"
)

// SSEHeader is the general AWS SSE HTTP header key.
const SSEHeader = "X-Amz-Server-Side-Encryption"

// SSEAlgorithmAES256 is the only supported value for the SSE-S3 or SSE-C algorithm header.
// For SSE-S3 see: https://docs.aws.amazon.com/AmazonS3/latest/dev/SSEUsingRESTAPI.html
// For SSE-C  see: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
const SSEAlgorithmAES256 = "AES256"

// S3 represents AWS SSE-S3. It provides functionality to handle
// SSE-S3 requests.
var S3 = s3{}

type s3 struct{}

// IsRequested returns true if the HTTP headers indicates that
// the S3 client requests SSE-S3.
func (s3) IsRequested(h http.Header) bool {
	_, ok := h[SSEHeader]
	return ok
}

// Parse parses the SSE-S3 related HTTP headers and checks
// whether they contain valid values.
func (s3) Parse(h http.Header) (err error) {
	if h.Get(SSEHeader) != SSEAlgorithmAES256 {
		err = ErrInvalidEncryptionMethod
	}
	return
}
