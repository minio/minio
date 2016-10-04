/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Signature and API related constants.
const (
	signV2Algorithm = "AWS"
)

// TODO add post policy signature.

// doesPresignV2SignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationQueryStringAuth
// returns ErrNone if matches. S3 errors otherwise.
func doesPresignV2SignatureMatch(r *http.Request) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Copy request
	req := *r

	// Validate if we do have query params.
	if req.URL.Query().Encode() == "" {
		return ErrInvalidQueryParams
	}

	// Validate if access key id same.
	if req.URL.Query().Get("AWSAccessKeyId") != cred.AccessKeyID {
		return ErrInvalidAccessKeyID
	}

	// Parse expires param into its native form.
	expired, err := strconv.ParseInt(req.URL.Query().Get("Expires"), 10, 64)
	if err != nil {
		errorIf(err, "Unable to parse expires query param")
		return ErrMalformedExpires
	}

	// Validate if the request has already expired.
	if expired < time.Now().UTC().Unix() {
		return ErrExpiredPresignRequest
	}

	// Save incoming signature to be validated later.
	incomingSignature := req.URL.Query().Get("Signature")

	// Set the expires header for string to sign.
	req.Header.Set("Expires", strconv.FormatInt(expired, 10))

	/// Empty out the query params, we only need to validate signature.
	query := req.URL.Query()
	// Remove all the query params added for signature alone, we need
	// a proper URL for string to sign.
	query.Del("Expires")
	query.Del("AWSAccessKeyId")
	query.Del("Signature")
	// Query encode whatever is left back to RawQuery.
	req.URL.RawQuery = queryEncode(query)

	// Get presigned string to sign.
	stringToSign := preStringifyHTTPReq(req)
	hm := hmac.New(sha1.New, []byte(cred.SecretAccessKey))
	hm.Write([]byte(stringToSign))

	// Calculate signature and validate.
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))
	if incomingSignature != signature {
		return ErrSignatureDoesNotMatch
	}

	// Success.
	return ErrNone
}

// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
//
// StringToSign = HTTP-Verb + "\n" +
//  	Content-Md5 + "\n" +
//  	Content-Type + "\n" +
//  	Date + "\n" +
//  	CanonicalizedProtocolHeaders +
//  	CanonicalizedResource;
//
// CanonicalizedResource = [ "/" + Bucket ] +
//  	<HTTP-Request-URI, from the protocol name up to the query string> +
//  	[ subresource, if present. For example "?acl", "?location", "?logging", or "?torrent"];
//
// CanonicalizedProtocolHeaders = <described below>

// doesSignV2Match - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func doesSignV2Match(r *http.Request) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Copy request.
	req := *r

	// Save authorization header.
	v2Auth := req.Header.Get("Authorization")
	if v2Auth == "" {
		return ErrAuthHeaderEmpty
	}

	// Add date if not present.
	if date := req.Header.Get("Date"); date == "" {
		if date = req.Header.Get("X-Amz-Date"); date == "" {
			return ErrMissingDateHeader
		}
	}

	// Calculate HMAC for secretAccessKey.
	stringToSign := stringifyHTTPReq(req)
	hm := hmac.New(sha1.New, []byte(cred.SecretAccessKey))
	hm.Write([]byte(stringToSign))

	// Prepare auth header.
	authHeader := new(bytes.Buffer)
	authHeader.WriteString(fmt.Sprintf("%s %s:", signV2Algorithm, cred.AccessKeyID))
	encoder := base64.NewEncoder(base64.StdEncoding, authHeader)
	encoder.Write(hm.Sum(nil))
	encoder.Close()

	// Verify if signature match.
	if authHeader.String() != v2Auth {
		return ErrSignatureDoesNotMatch
	}

	return ErrNone
}

// From the Amazon docs:
//
// StringToSign = HTTP-Verb + "\n" +
// 	 Content-Md5 + "\n" +
//	 Content-Type + "\n" +
//	 Expires + "\n" +
//	 CanonicalizedProtocolHeaders +
//	 CanonicalizedResource;
func preStringifyHTTPReq(req http.Request) string {
	buf := new(bytes.Buffer)
	// Write standard headers.
	writePreSignV2Headers(buf, req)
	// Write canonicalized protocol headers if any.
	writeCanonicalizedHeaders(buf, req)
	// Write canonicalized Query resources if any.
	isPreSign := true
	writeCanonicalizedResource(buf, req, isPreSign)
	return buf.String()
}

// writePreSignV2Headers - write preSign v2 required headers.
func writePreSignV2Headers(buf *bytes.Buffer, req http.Request) {
	buf.WriteString(req.Method + "\n")
	buf.WriteString(req.Header.Get("Content-Md5") + "\n")
	buf.WriteString(req.Header.Get("Content-Type") + "\n")
	buf.WriteString(req.Header.Get("Expires") + "\n")
}

// From the Amazon docs:
//
// StringToSign = HTTP-Verb + "\n" +
// 	 Content-Md5 + "\n" +
//	 Content-Type + "\n" +
//	 Date + "\n" +
//	 CanonicalizedProtocolHeaders +
//	 CanonicalizedResource;
func stringifyHTTPReq(req http.Request) string {
	buf := new(bytes.Buffer)
	// Write standard headers.
	writeSignV2Headers(buf, req)
	// Write canonicalized protocol headers if any.
	writeCanonicalizedHeaders(buf, req)
	// Write canonicalized Query resources if any.
	isPreSign := false
	writeCanonicalizedResource(buf, req, isPreSign)
	return buf.String()
}

// writeSignV2Headers - write signV2 required headers.
func writeSignV2Headers(buf *bytes.Buffer, req http.Request) {
	buf.WriteString(req.Method + "\n")
	buf.WriteString(req.Header.Get("Content-Md5") + "\n")
	buf.WriteString(req.Header.Get("Content-Type") + "\n")
	buf.WriteString(req.Header.Get("Date") + "\n")
}

// writeCanonicalizedHeaders - write canonicalized headers.
func writeCanonicalizedHeaders(buf *bytes.Buffer, req http.Request) {
	var protoHeaders []string
	vals := make(map[string][]string)
	for k, vv := range req.Header {
		// All the AMZ headers should be lowercase
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz") {
			protoHeaders = append(protoHeaders, lk)
			vals[lk] = vv
		}
	}
	sort.Strings(protoHeaders)
	for _, k := range protoHeaders {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			if strings.Contains(v, "\n") {
				// TODO: "Unfold" long headers that
				// span multiple lines (as allowed by
				// RFC 2616, section 4.2) by replacing
				// the folding white-space (including
				// new-line) by a single space.
				buf.WriteString(v)
			} else {
				buf.WriteString(v)
			}
		}
		buf.WriteByte('\n')
	}
}

// The following list is already sorted and should always be, otherwise we could
// have signature-related issues
var resourceList = []string{
	"acl",
	"delete",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

// From the Amazon docs:
//
// CanonicalizedResource = [ "/" + Bucket ] +
// 	  <HTTP-Request-URI, from the protocol name up to the query string> +
// 	  [ sub-resource, if present. For example "?acl", "?location", "?logging", or "?torrent"];
func writeCanonicalizedResource(buf *bytes.Buffer, req http.Request, isPreSign bool) {
	// Save request URL.
	requestURL := req.URL
	// Get encoded URL path.
	path := getURLEncodedName(requestURL.Path)
	if isPreSign {
		// Get encoded URL path.
		if len(requestURL.Query()) > 0 {
			// Keep the usual queries unescaped for string to sign.
			query, _ := url.QueryUnescape(queryEncode(requestURL.Query()))
			path = path + "?" + query
		}
		buf.WriteString(path)
		return
	}
	buf.WriteString(path)
	if requestURL.RawQuery != "" {
		var n int
		vals, _ := url.ParseQuery(requestURL.RawQuery)
		// Verify if any sub resource queries are present, if yes
		// canonicallize them.
		for _, resource := range resourceList {
			if vv, ok := vals[resource]; ok && len(vv) > 0 {
				n++
				// First element
				switch n {
				case 1:
					buf.WriteByte('?')
				// The rest
				default:
					buf.WriteByte('&')
				}
				buf.WriteString(resource)
				// Request parameters
				if len(vv[0]) > 0 {
					buf.WriteByte('=')
					buf.WriteString(strings.Replace(url.QueryEscape(vv[0]), "+", "%20", -1))
				}
			}
		}
	}
}
