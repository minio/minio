/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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

package cmd

import (
	"bytes"
	"crypto/hmac"
	"encoding/hex"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/sha256-simd"
)

// http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD" indicates that the
// client did not calculate sha256 of the payload.
const unsignedPayload = "UNSIGNED-PAYLOAD"

// skipContentSha256Cksum returns true if caller needs to skip
// payload checksum, false if not.
func skipContentSha256Cksum(r *http.Request) bool {
	var (
		v  []string
		ok bool
	)

	if isRequestPresignedSignatureV4(r) {
		v, ok = r.URL.Query()[xhttp.AmzContentSha256]
		if !ok {
			v, ok = r.Header[xhttp.AmzContentSha256]
		}
	} else {
		v, ok = r.Header[xhttp.AmzContentSha256]
	}

	// If x-amz-content-sha256 is set and the value is not
	// 'UNSIGNED-PAYLOAD' we should validate the content sha256.
	return !(ok && v[0] != unsignedPayload)
}

// Returns SHA256 for calculating canonical-request.
func getContentSha256Cksum(r *http.Request, stype serviceType) string {
	if stype == serviceSTS {
		payload, err := ioutil.ReadAll(io.LimitReader(r.Body, stsRequestBodyLimit))
		if err != nil {
			logger.CriticalIf(GlobalContext, err)
		}
		sum256 := sha256.New()
		sum256.Write(payload)
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
		return hex.EncodeToString(sum256.Sum(nil))
	}

	var (
		defaultSha256Cksum string
		v                  []string
		ok                 bool
	)

	// For a presigned request we look at the query param for sha256.
	if isRequestPresignedSignatureV4(r) {
		// X-Amz-Content-Sha256, if not set in presigned requests, checksum
		// will default to 'UNSIGNED-PAYLOAD'.
		defaultSha256Cksum = unsignedPayload
		v, ok = r.URL.Query()[xhttp.AmzContentSha256]
		if !ok {
			v, ok = r.Header[xhttp.AmzContentSha256]
		}
	} else {
		// X-Amz-Content-Sha256, if not set in signed requests, checksum
		// will default to sha256([]byte("")).
		defaultSha256Cksum = emptySHA256
		v, ok = r.Header[xhttp.AmzContentSha256]
	}

	// We found 'X-Amz-Content-Sha256' return the captured value.
	if ok {
		return v[0]
	}

	// We couldn't find 'X-Amz-Content-Sha256'.
	return defaultSha256Cksum
}

// isValidRegion - verify if incoming region value is valid with configured Region.
func isValidRegion(reqRegion string, confRegion string) bool {
	if confRegion == "" {
		return true
	}
	if confRegion == "US" {
		confRegion = globalMinioDefaultRegion
	}
	// Some older s3 clients set region as "US" instead of
	// globalMinioDefaultRegion, handle it.
	if reqRegion == "US" {
		reqRegion = globalMinioDefaultRegion
	}
	return reqRegion == confRegion
}

// check if the access key is valid and recognized, additionally
// also returns if the access key is owner/admin.
func checkKeyValid(accessKey string) (auth.Credentials, bool, APIErrorCode) {
	var owner = true
	var cred = globalActiveCred
	if cred.AccessKey != accessKey {
		if globalIAMSys == nil {
			return cred, false, ErrInvalidAccessKeyID
		}
		// Check if the access key is part of users credentials.
		var ok bool
		if cred, ok = globalIAMSys.GetUser(accessKey); !ok {
			return cred, false, ErrInvalidAccessKeyID
		}
		owner = false
	}
	return cred, owner, ErrNone
}

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// extractSignedHeaders extract signed headers from Authorization header
func extractSignedHeaders(signedHeaders []string, r *http.Request) (http.Header, APIErrorCode) {
	reqHeaders := r.Header
	reqQueries := r.URL.Query()
	// find whether "host" is part of list of signed headers.
	// if not return ErrUnsignedHeaders. "host" is mandatory.
	if !contains(signedHeaders, "host") {
		return nil, ErrUnsignedHeaders
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		// `host` will not be found in the headers, can be found in r.Host.
		// but its alway necessary that the list of signed headers containing host in it.
		val, ok := reqHeaders[http.CanonicalHeaderKey(header)]
		if !ok {
			// try to set headers from Query String
			val, ok = reqQueries[header]
		}
		if ok {
			for _, enc := range val {
				extractedSignedHeaders.Add(header, enc)
			}
			continue
		}
		switch header {
		case "expect":
			// Golang http server strips off 'Expect' header, if the
			// client sent this as part of signed headers we need to
			// handle otherwise we would see a signature mismatch.
			// `aws-cli` sets this as part of signed headers.
			//
			// According to
			// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.20
			// Expect header is always of form:
			//
			//   Expect       =  "Expect" ":" 1#expectation
			//   expectation  =  "100-continue" | expectation-extension
			//
			// So it safe to assume that '100-continue' is what would
			// be sent, for the time being keep this work around.
			// Adding a *TODO* to remove this later when Golang server
			// doesn't filter out the 'Expect' header.
			extractedSignedHeaders.Set(header, "100-continue")
		case "host":
			// Go http server removes "host" from Request.Header
			extractedSignedHeaders.Set(header, r.Host)
		case "transfer-encoding":
			// Go http server removes "host" from Request.Header
			for _, enc := range r.TransferEncoding {
				extractedSignedHeaders.Add(header, enc)
			}
		case "content-length":
			// Signature-V4 spec excludes Content-Length from signed headers list for signature calculation.
			// But some clients deviate from this rule. Hence we consider Content-Length for signature
			// calculation to be compatible with such clients.
			extractedSignedHeaders.Set(header, strconv.FormatInt(r.ContentLength, 10))
		default:
			return nil, ErrUnsignedHeaders
		}
	}
	return extractedSignedHeaders, ErrNone
}

// Trim leading and trailing spaces and replace sequential spaces with one space, following Trimall()
// in http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to one space and return
	return strings.Join(strings.Fields(input), " ")
}
