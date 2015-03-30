/*
 * Minimalist Object Storage, (C) 2014,2015 Minio, Inc.
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

package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/minio-io/minio/pkg/api/config"
)

// SignRequest - a given http request using HMAC style signatures
func SignRequest(user config.User, req *http.Request) {
	if date := req.Header.Get("Date"); date == "" {
		req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}
	hm := hmac.New(sha1.New, []byte(user.SecretKey))
	ss := getStringToSign(req)
	io.WriteString(hm, ss)

	authHeader := new(bytes.Buffer)
	fmt.Fprintf(authHeader, "AWS %s:", user.AccessKey)
	encoder := base64.NewEncoder(base64.StdEncoding, authHeader)
	encoder.Write(hm.Sum(nil))
	encoder.Close()
	req.Header.Set("Authorization", authHeader.String())
}

// ValidateRequest - an API request by validating its signature using HMAC signatures
func ValidateRequest(user config.User, req *http.Request) (bool, error) {
	// Verify if date headers are set, if not reject the request
	if req.Header.Get("x-amz-date") == "" {
		if req.Header.Get("Date") == "" {
			return false, errors.New("Date should be set")
		}
	}
	hm := hmac.New(sha1.New, []byte(user.SecretKey))
	ss := getStringToSign(req)
	io.WriteString(hm, ss)

	authHeader := new(bytes.Buffer)
	fmt.Fprintf(authHeader, "AWS %s:", user.AccessKey)
	encoder := base64.NewEncoder(base64.StdEncoding, authHeader)
	encoder.Write(hm.Sum(nil))
	encoder.Close()

	// DEBUG
	// fmt.Println("Request header sent: ", req.Header.Get("Authorization"))
	// fmt.Println("Header calculated: ", authHeader.String())
	// fmt.Printf("%q : %x", ss, ss)
	if req.Header.Get("Authorization") != authHeader.String() {
		return false, errors.New("Authorization header mismatch")
	}
	return true, nil
}

// From the Amazon docs:
//
// StringToSign = HTTP-Verb + "\n" +
// 	 Content-MD5 + "\n" +
//	 Content-Type + "\n" +
//	 Date + "\n" +
//	 CanonicalizedAmzHeaders +
//	 CanonicalizedResource;
func getStringToSign(req *http.Request) string {
	buf := new(bytes.Buffer)
	buf.WriteString(req.Method)
	buf.WriteByte('\n')
	buf.WriteString(req.Header.Get("Content-MD5"))
	buf.WriteByte('\n')
	buf.WriteString(req.Header.Get("Content-Type"))
	buf.WriteByte('\n')
	if req.Header.Get("x-amz-date") == "" {
		buf.WriteString(req.Header.Get("Date"))
	}
	buf.WriteByte('\n')
	writeCanonicalizedAmzHeaders(buf, req)
	writeCanonicalizedResource(buf, req)

	return buf.String()
}

// Lower all upper case letters
func hasPrefixCaseInsensitive(s, pfx string) bool {
	if len(pfx) > len(s) {
		return false
	}
	shead := s[:len(pfx)]
	if shead == pfx {
		return true
	}
	shead = strings.ToLower(shead)
	return shead == pfx || shead == strings.ToLower(pfx)
}

// Canonicalize amazon special headers, headers starting with 'x-amz-'
func writeCanonicalizedAmzHeaders(buf *bytes.Buffer, req *http.Request) {
	var amzHeaders []string
	vals := make(map[string][]string)
	for k, vv := range req.Header {
		if hasPrefixCaseInsensitive(k, "x-amz-") {
			lk := strings.ToLower(k)
			amzHeaders = append(amzHeaders, lk)
			vals[lk] = vv
		}
	}
	sort.Strings(amzHeaders)
	for _, k := range amzHeaders {
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

// Resource list must be sorted:
var subResList = []string{
	"acl",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"uploadId",
	"uploads",
	"response-content-type",
	"response-content-language",
	"response-content-disposition",
	"response-content-encoding",
	"website",
}

// From the Amazon docs:
//
// CanonicalizedResource = [ "/" + Bucket ] +
// 	  <HTTP-Request-URI, from the protocol name up to the query string> +
// 	  [ sub-resource, if present. For example "?acl", "?location", "?logging", or "?torrent"];
func writeCanonicalizedResource(buf *bytes.Buffer, req *http.Request) {
	// Grab bucket name from hostname
	bucket := bucketFromHostname(req)
	if bucket != "" {
		buf.WriteByte('/')
		buf.WriteString(bucket)
	}
	buf.WriteString(req.URL.Path)
	sort.Strings(subResList)
	if req.URL.RawQuery != "" {
		n := 0
		vals, _ := url.ParseQuery(req.URL.RawQuery)
		for _, subres := range subResList {
			if vv, ok := vals[subres]; ok && len(vv) > 0 {
				n++
				if n == 1 {
					buf.WriteByte('?')
				} else {
					buf.WriteByte('&')
				}
				buf.WriteString(subres)
				if len(vv[0]) > 0 {
					buf.WriteByte('=')
					buf.WriteString(url.QueryEscape(vv[0]))
				}
			}
		}
	}
}

// Convert subdomain http request into bucketname if possible
func bucketFromHostname(req *http.Request) string {
	host, _, _ := net.SplitHostPort(req.Host)
	// Verify incoming request if only IP with no bucket subdomain
	if net.ParseIP(host) != nil {
		return ""
	}
	if host == "" {
		host = req.URL.Host
	}

	// Grab the bucket from the incoming hostname
	host = strings.TrimSpace(host)
	hostParts := strings.Split(host, ".")
	if len(hostParts) > 2 {
		return hostParts[0]
	}
	return ""
}
