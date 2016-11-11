/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/minio/sha256-simd"
)

// http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD" indicates that the
// client did not calculate sha256 of the payload.
const unsignedPayload = "UNSIGNED-PAYLOAD"

// http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD" indicates that the
// client did not calculate sha256 of the payload. Hence we skip calculating sha256.
// We also skip calculating sha256 for presigned requests without "x-amz-content-sha256"
// query header.
func skipContentSha256Cksum(r *http.Request) bool {
	queryContentSha256 := r.URL.Query().Get("X-Amz-Content-Sha256")
	isRequestPresignedUnsignedPayload := func(r *http.Request) bool {
		if isRequestPresignedSignatureV4(r) {
			return queryContentSha256 == "" || queryContentSha256 == unsignedPayload
		}
		return false
	}
	return isRequestUnsignedPayload(r) || isRequestPresignedUnsignedPayload(r)
}

// isValidRegion - verify if incoming region value is valid with configured Region.
func isValidRegion(reqRegion string, confRegion string) bool {
	if confRegion == "" || confRegion == "US" {
		confRegion = "us-east-1"
	}
	// Some older s3 clients set region as "US" instead of
	// "us-east-1", handle it.
	if reqRegion == "US" {
		reqRegion = "us-east-1"
	}
	return reqRegion == confRegion
}

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// Reserved string regexp.
var reservedNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// getURLEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func getURLEncodedName(name string) string {
	// if object matches reserved string, no need to encode them
	if reservedNames.MatchString(name) {
		return name
	}
	var encodedName string
	for _, s := range name {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len > 0 {
				u := make([]byte, len)
				utf8.EncodeRune(u, s)
				for _, r := range u {
					hex := hex.EncodeToString([]byte{r})
					encodedName = encodedName + "%" + strings.ToUpper(hex)
				}
			}
		}
	}
	return encodedName
}

func findHost(signedHeaders []string) APIErrorCode {
	if contains(signedHeaders, "host") {
		return ErrNone
	}
	return ErrUnsignedHeaders
}

// extractSignedHeaders extract signed headers from Authorization header
func extractSignedHeaders(signedHeaders []string, reqHeaders http.Header) (http.Header, APIErrorCode) {
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
			if header == "expect" {
				extractedSignedHeaders[header] = []string{"100-continue"}
				continue
			}
			// the "host" field will not be found in the header map, it can be found in req.Host.
			// but its necessary to make sure that the "host" field exists in the list of signed parameters,
			// the check is done above.
			if header == "host" {
				continue
			}
			// If not found continue, we will stop here.
			return nil, ErrUnsignedHeaders
		}
		extractedSignedHeaders[header] = val
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
