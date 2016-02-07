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
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Signature and API related constants.
const (
	signV2Algorithm = "AWS"
)

// Encode input URL path to URL encoded path.
func encodeURL2Path(u *url.URL) (path string) {
	// Encode URL path.
	if isS3, _ := filepath.Match("*.s3*.amazonaws.com", u.Host); isS3 {
		hostSplits := strings.SplitN(u.Host, ".", 4)
		// First element is the bucket name.
		bucketName := hostSplits[0]
		path = "/" + bucketName
		path += u.Path
		path = urlEncodePath(path)
		return
	}
	if strings.HasSuffix(u.Host, ".storage.googleapis.com") {
		path = "/" + strings.TrimSuffix(u.Host, ".storage.googleapis.com")
		path += u.Path
		path = urlEncodePath(path)
		return
	}
	path = urlEncodePath(u.Path)
	return
}

// preSignV2 - presign the request in following style.
// https://${S3_BUCKET}.s3.amazonaws.com/${S3_OBJECT}?AWSAccessKeyId=${S3_ACCESS_KEY}&Expires=${TIMESTAMP}&Signature=${SIGNATURE}.
func preSignV2(req http.Request, accessKeyID, secretAccessKey string, expires int64) *http.Request {
	// Presign is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return &req
	}
	d := time.Now().UTC()
	// Add date if not present.
	if date := req.Header.Get("Date"); date == "" {
		req.Header.Set("Date", d.Format(http.TimeFormat))
	}

	// Get encoded URL path.
	path := encodeURL2Path(req.URL)
	if len(req.URL.Query()) > 0 {
		// Keep the usual queries unescaped for string to sign.
		query, _ := url.QueryUnescape(queryEncode(req.URL.Query()))
		path = path + "?" + query
	}

	// Find epoch expires when the request will expire.
	epochExpires := d.Unix() + expires

	// Get string to sign.
	stringToSign := fmt.Sprintf("%s\n\n\n%d\n%s", req.Method, epochExpires, path)
	hm := hmac.New(sha1.New, []byte(secretAccessKey))
	hm.Write([]byte(stringToSign))

	// Calculate signature.
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))

	query := req.URL.Query()
	// Handle specially for Google Cloud Storage.
	if strings.Contains(req.URL.Host, ".storage.googleapis.com") {
		query.Set("GoogleAccessId", accessKeyID)
	} else {
		query.Set("AWSAccessKeyId", accessKeyID)
	}

	// Fill in Expires for presigned query.
	query.Set("Expires", strconv.FormatInt(epochExpires, 10))

	// Encode query and save.
	req.URL.RawQuery = queryEncode(query)

	// Save signature finally.
	req.URL.RawQuery += "&Signature=" + urlEncodePath(signature)

	// Return.
	return &req
}

// postPresignSignatureV2 - presigned signature for PostPolicy
// request.
func postPresignSignatureV2(policyBase64, secretAccessKey string) string {
	hm := hmac.New(sha1.New, []byte(secretAccessKey))
	hm.Write([]byte(policyBase64))
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))
	return signature
}

// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
//
// StringToSign = HTTP-Verb + "\n" +
//  	Content-MD5 + "\n" +
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

// signV2 sign the request before Do() (AWS Signature Version 2).
func signV2(req http.Request, accessKeyID, secretAccessKey string) *http.Request {
	// Signature calculation is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return &req
	}

	// Initial time.
	d := time.Now().UTC()

	// Add date if not present.
	if date := req.Header.Get("Date"); date == "" {
		req.Header.Set("Date", d.Format(http.TimeFormat))
	}

	// Calculate HMAC for secretAccessKey.
	stringToSign := getStringToSignV2(req)
	hm := hmac.New(sha1.New, []byte(secretAccessKey))
	hm.Write([]byte(stringToSign))

	// Prepare auth header.
	authHeader := new(bytes.Buffer)
	authHeader.WriteString(fmt.Sprintf("%s %s:", signV2Algorithm, accessKeyID))
	encoder := base64.NewEncoder(base64.StdEncoding, authHeader)
	encoder.Write(hm.Sum(nil))
	encoder.Close()

	// Set Authorization header.
	req.Header.Set("Authorization", authHeader.String())

	return &req
}

// From the Amazon docs:
//
// StringToSign = HTTP-Verb + "\n" +
// 	 Content-MD5 + "\n" +
//	 Content-Type + "\n" +
//	 Date + "\n" +
//	 CanonicalizedProtocolHeaders +
//	 CanonicalizedResource;
func getStringToSignV2(req http.Request) string {
	buf := new(bytes.Buffer)
	// Write standard headers.
	writeDefaultHeaders(buf, req)
	// Write canonicalized protocol headers if any.
	writeCanonicalizedHeaders(buf, req)
	// Write canonicalized Query resources if any.
	writeCanonicalizedResource(buf, req)
	return buf.String()
}

// writeDefaultHeader - write all default necessary headers
func writeDefaultHeaders(buf *bytes.Buffer, req http.Request) {
	buf.WriteString(req.Method)
	buf.WriteByte('\n')
	buf.WriteString(req.Header.Get("Content-MD5"))
	buf.WriteByte('\n')
	buf.WriteString(req.Header.Get("Content-Type"))
	buf.WriteByte('\n')
	buf.WriteString(req.Header.Get("Date"))
	buf.WriteByte('\n')
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

// Must be sorted:
var resourceList = []string{
	"acl",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"response-content-type",
	"response-content-language",
	"response-expires",
	"response-cache-control",
	"response-content-disposition",
	"response-content-encoding",
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
func writeCanonicalizedResource(buf *bytes.Buffer, req http.Request) {
	// Save request URL.
	requestURL := req.URL
	// Get encoded URL path.
	path := encodeURL2Path(requestURL)
	buf.WriteString(path)

	sort.Strings(resourceList)
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
