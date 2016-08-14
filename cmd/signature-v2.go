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

func doesPolicySignatureV2Match(formValues map[string]string) error {
	cred := serverConfig.GetCredential()
	accessKey := formValues["Awsaccesskeyid"]
	if accessKey != cred.AccessKeyID {
		return eInvalidAccessKeyID()
	}
	signature := formValues["Signature"]
	policy := formValues["Policy"]
	if signature != calculateSignatureV2(policy, cred.SecretAccessKey) {
		return eSignatureDoesNotMatch(cred.AccessKeyID,
			signature, "", "", "", "")
	}
	return nil
}

// doesPresignV2SignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#RESTAuthenticationQueryStringAuth
// returns ErrNone if matches. S3 errors otherwise.
func doesPresignV2SignatureMatch(r *http.Request) error {
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
		return eInvalidQueryParams()
	}

	// Validate if access key id same.
	if accessKey != cred.AccessKeyID {
		return eInvalidAccessKeyID()
	}

	// Make sure the request has not expired.
	expiresInt, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return eMalformedExpires()
	}

	if expiresInt < time.Now().UTC().Unix() {
		return eExpiredPresignRequest()
	}

	stringToSign := presignV2STS(r.Method, encodedResource, strings.Join(filteredQueries, "&"), r.Header, expires)
	expectedSignature := calculateSignatureV2(stringToSign, cred.SecretAccessKey)

	if gotSignature != getURLEncodedName(expectedSignature) {
		return eSignatureDoesNotMatch(cred.AccessKeyID,
			gotSignature, stringToSign, strToByteStr(stringToSign),
			"", "")
	}
	return nil
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
func validateV2AuthHeader(v2Auth, accessKeyID string) error {
	if v2Auth == "" {
		return eAuthHeaderEmpty()
	}
	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v2Auth, signV2Algorithm) {
		return eSignatureNotSupported()
	}

	// below is V2 Signed Auth header format, splitting on `space` (after the `AWS` string).
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature
	authFields := strings.Fields(v2Auth)
	if len(authFields) != 2 {
		return eMissingFields()
	}

	// Then will be splitting on ":", this will seprate `AWSAccessKeyId` and `Signature` string.
	keySignFields := strings.Split(authFields[1], ":")
	if len(keySignFields) != 2 {
		return eMissingFields()
	}

	// Access credentials.
	if keySignFields[0] != accessKeyID {
		return eInvalidAccessKeyID()
	}
	return nil
}

func strToByteStr(str string) (bstr string) {
	for _, b := range []byte(str) {
		bstr += fmt.Sprint(b, " ")
	}
	return bstr
}

// doesSignV2Match - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func doesSignV2Match(r *http.Request) error {
	v2Auth := r.Header.Get("Authorization")
	cred := serverConfig.GetCredential()
	if apiError := validateV2AuthHeader(v2Auth, cred.AccessKeyID); apiError != nil {
		return apiError
	}

	// Encode path:
	//   url.RawPath will be valid if path has any encoded characters, if not it will
	//   be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := r.URL.RawPath
	if encodedResource == "" {
		splits := strings.Split(r.URL.Path, "?")
		if len(splits) > 0 {
			encodedResource = getURLEncodedName(splits[0])
		}
	}

	// Encode query strings
	encodedQuery := r.URL.Query().Encode()
	stringToSign := signV2STS(r.Method, encodedResource, encodedQuery, r.Header)
	expectedAuth := calculateSignatureV2(stringToSign, cred.SecretAccessKey)
	if v2Auth != expectedAuth {
		return eSignatureDoesNotMatch(
			cred.AccessKeyID,
			stringToSign,
			strToByteStr(stringToSign),
			strings.Trim(v2Auth, fmt.Sprintf("%s %s:",
				signV2Algorithm, cred.AccessKeyID)),
			"", "",
		)
	}
	return nil
}

func calculateSignatureV2(stringToSign string, secret string) string {
	hm := hmac.New(sha1.New, []byte(secret))
	hm.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
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
