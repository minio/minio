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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Signature and API related constants.
const (
	signV2Algorithm = "AWS"
)

// AWS S3 Signature V2 calculation rule is give here:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationStringToSign

// Whitelist resource list that will be used in query string for signature-V2 calculation.
var resourceList = []string{
	"acl",
	"delete",
	"lifecycle",
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

// TODO add post policy signature.

// doesPresignV2SignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationQueryStringAuth
// returns ErrNone if matches. S3 errors otherwise.
func doesPresignV2SignatureMatch(r *http.Request) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// url.RawPath will be valid if path has any encoded characters, if not it will
	// be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := r.URL.RawPath
	encodedQuery := r.URL.RawQuery
	if encodedResource == "" {
		splits := strings.Split(r.URL.Path, "?")
		if len(splits) > 0 {
			encodedResource = splits[0]
		}
	}
	queries := strings.Split(encodedQuery, "&")
	var filteredQueries []string
	var gotSignature string
	var expires string
	var accessKey string
	for _, query := range queries {
		keyval := strings.Split(query, "=")
		switch keyval[0] {
		case "AWSAccessKeyId":
			accessKey = keyval[1]
		case "Signature":
			gotSignature = keyval[1]
		case "Expires":
			expires = keyval[1]
		default:
			filteredQueries = append(filteredQueries, query)
		}
	}

	if accessKey == "" {
		return ErrInvalidQueryParams
	}

	// Validate if access key id same.
	if accessKey != cred.AccessKeyID {
		return ErrInvalidAccessKeyID
	}

	// Make sure the request has not expired.
	expiresInt, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return ErrMalformedExpires
	}

	if expiresInt < time.Now().UTC().Unix() {
		return ErrExpiredPresignRequest
	}

	expectedSignature := preSignatureV2(r.Method, encodedResource, strings.Join(filteredQueries, "&"), r.Header, expires)
	if gotSignature != getURLEncodedName(expectedSignature) {
		return ErrSignatureDoesNotMatch
	}

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
	gotAuth := r.Header.Get("Authorization")
	if gotAuth == "" {
		return ErrAuthHeaderEmpty
	}

	// url.RawPath will be valid if path has any encoded characters, if not it will
	// be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := r.URL.RawPath
	encodedQuery := r.URL.RawQuery
	if encodedResource == "" {
		splits := strings.Split(r.URL.Path, "?")
		if len(splits) > 0 {
			encodedResource = splits[0]
		}
	}

	expectedAuth := signatureV2(r.Method, encodedResource, encodedQuery, r.Header)
	if gotAuth != expectedAuth {
		return ErrSignatureDoesNotMatch
	}

	return ErrNone
}

// Return signature-v2 for the presigned request.
func preSignatureV2(method string, encodedResource string, encodedQuery string, headers http.Header, expires string) string {
	cred := serverConfig.GetCredential()

	stringToSign := presignV2STS(method, encodedResource, encodedQuery, headers, expires)
	hm := hmac.New(sha1.New, []byte(cred.SecretAccessKey))
	hm.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))
	return signature
}

// Return signature-v2 authrization header.
func signatureV2(method string, encodedResource string, encodedQuery string, headers http.Header) string {
	cred := serverConfig.GetCredential()

	stringToSign := signV2STS(method, encodedResource, encodedQuery, headers)

	hm := hmac.New(sha1.New, []byte(cred.SecretAccessKey))
	hm.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))
	return fmt.Sprintf("%s %s:%s", signV2Algorithm, cred.AccessKeyID, signature)
}

// Return canonical headers.
func canonicalizedAmzHeadersV2(headers http.Header) string {
	var keys []string
	keyval := make(map[string]string)
	for key := range headers {
		lkey := strings.ToLower(key)
		if !strings.HasPrefix(lkey, "x-amz-") {
			continue
		}
		keys = append(keys, lkey)
		keyval[lkey] = strings.Join(headers[key], ",")
	}
	sort.Strings(keys)
	var canonicalHeaders []string
	for _, key := range keys {
		canonicalHeaders = append(canonicalHeaders, key+":"+keyval[key])
	}
	return strings.Join(canonicalHeaders, "\n")
}

// Return canonical resource string.
func canonicalizedResourceV2(encodedPath string, encodedQuery string) string {
	queries := strings.Split(encodedQuery, "&")
	keyval := make(map[string]string)
	for _, query := range queries {
		key := query
		val := ""
		index := strings.Index(query, "=")
		if index != -1 {
			key = query[:index]
			val = query[index+1:]
		}
		keyval[key] = val
	}
	var canonicalQueries []string
	for _, key := range resourceList {
		val, ok := keyval[key]
		if !ok {
			continue
		}
		if val == "" {
			canonicalQueries = append(canonicalQueries, key)
			continue
		}
		canonicalQueries = append(canonicalQueries, key+"="+val)
	}
	if len(canonicalQueries) == 0 {
		return encodedPath
	}
	// the queries will be already sorted as resourceList is sorted.
	return encodedPath + "?" + strings.Join(canonicalQueries, "&")
}

// Return string to sign for authz header calculation.
func signV2STS(method string, encodedResource string, encodedQuery string, headers http.Header) string {
	canonicalHeaders := canonicalizedAmzHeadersV2(headers)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}

	// From the Amazon docs:
	//
	// StringToSign = HTTP-Verb + "\n" +
	// 	 Content-Md5 + "\n" +
	//	 Content-Type + "\n" +
	//	 Date + "\n" +
	//	 CanonicalizedProtocolHeaders +
	//	 CanonicalizedResource;
	stringToSign := strings.Join([]string{
		method,
		headers.Get("Content-MD5"),
		headers.Get("Content-Type"),
		headers.Get("Date"),
		canonicalHeaders,
	}, "\n") + canonicalizedResourceV2(encodedResource, encodedQuery)

	return stringToSign
}

// Return string to sign for pre-sign signature calculation.
func presignV2STS(method string, encodedResource string, encodedQuery string, headers http.Header, expires string) string {
	canonicalHeaders := canonicalizedAmzHeadersV2(headers)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}

	// From the Amazon docs:
	//
	// StringToSign = HTTP-Verb + "\n" +
	// 	 Content-Md5 + "\n" +
	//	 Content-Type + "\n" +
	//	 Expires + "\n" +
	//	 CanonicalizedProtocolHeaders +
	//	 CanonicalizedResource;
	stringToSign := strings.Join([]string{
		method,
		headers.Get("Content-MD5"),
		headers.Get("Content-Type"),
		expires,
		canonicalHeaders,
	}, "\n") + canonicalizedResourceV2(encodedResource, encodedQuery)
	return stringToSign
}
