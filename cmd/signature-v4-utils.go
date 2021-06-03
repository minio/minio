// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
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
		sum256 := sha256.Sum256(payload)
		r.Body = ioutil.NopCloser(bytes.NewReader(payload))
		return hex.EncodeToString(sum256[:])
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
	if !globalIAMSys.Initialized() && !globalIsGateway {
		// Check if server has initialized, then only proceed
		// to check for IAM users otherwise its okay for clients
		// to retry with 503 errors when server is coming up.
		return auth.Credentials{}, false, ErrServerNotInitialized
	}
	var owner = true
	var cred = globalActiveCred
	if cred.AccessKey != accessKey {
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
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = val
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
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = r.TransferEncoding
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
