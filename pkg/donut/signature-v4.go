/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"bytes"
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/iodine"
)

// request - a http request
type request struct {
	receivedReq   *http.Request
	calculatedReq *http.Request
	user          *auth.User
	body          io.Reader
}

const (
	authHeader    = "AWS4-HMAC-SHA256"
	iso8601Format = "20060102T150405Z"
	yyyymmdd      = "20060102"
)

var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// sumHMAC calculate hmac between two input byte array
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// urlEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncodeName(name string) (string, error) {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	if reservedNames.MatchString(name) {
		return name, nil
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
			if len < 0 {
				return "", errors.New("invalid utf-8")
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedName = encodedName + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedName, nil
}

// newSignV4Request - populate a new signature v4 request
func newSignV4Request(user *auth.User, req *http.Request, body io.Reader) (*request, error) {
	// save for subsequent use
	r := new(request)
	r.user = user
	r.body = body
	r.receivedReq = req
	r.calculatedReq = req
	return r, nil
}

// getHashedPayload get the hexadecimal value of the SHA256 hash of the request payload
func (r *request) getHashedPayload() string {
	hash := func() string {
		switch {
		case r.body == nil:
			return hex.EncodeToString(sha256.Sum256([]byte{}))
		default:
			sum256Bytes, _ := sha256.Sum(r.body)
			return hex.EncodeToString(sum256Bytes)
		}
	}
	hashedPayload := hash()
	r.calculatedReq.Header.Set("X-Amz-Content-Sha256", hashedPayload)
	return hashedPayload
}

// getCanonicalHeaders generate a list of request headers with their values
func (r *request) getCanonicalHeaders() string {
	var headers []string
	vals := make(map[string][]string)
	for k, vv := range r.calculatedReq.Header {
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // ignored header
		}
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(r.calculatedReq.URL.Host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func (r *request) getSignedHeaders() string {
	var headers []string
	for k := range r.calculatedReq.Header {
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // ignored header
		}
		headers = append(headers, strings.ToLower(k))
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// getCanonicalRequest generate a canonical request of style
//
// canonicalRequest =
//  <HTTPMethod>\n
//  <CanonicalURI>\n
//  <CanonicalQueryString>\n
//  <CanonicalHeaders>\n
//  <SignedHeaders>\n
//  <HashedPayload>
//
func (r *request) getCanonicalRequest(hashedPayload string) string {
	r.calculatedReq.URL.RawQuery = strings.Replace(r.calculatedReq.URL.Query().Encode(), "+", "%20", -1)
	encodedPath, _ := urlEncodeName(r.calculatedReq.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		r.calculatedReq.Method,
		encodedPath,
		r.calculatedReq.URL.RawQuery,
		r.getCanonicalHeaders(),
		r.getSignedHeaders(),
		hashedPayload,
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service
func (r *request) getScope(t time.Time) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"milkyway",
		"s3",
		"aws4_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values
func (r *request) getStringToSign(canonicalRequest string, t time.Time) string {
	stringToSign := authHeader + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + r.getScope(t) + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sha256.Sum256([]byte(canonicalRequest)))
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature
func (r *request) getSigningKey(t time.Time) []byte {
	secret := r.user.SecretAccessKey
	date := sumHMAC([]byte("AWS4"+secret), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form
func (r *request) getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// SignV4 the request before Do(), in accordance with - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
func (r *request) SignV4() (string, error) {
	// Add date if not present
	var date string
	if date = r.calculatedReq.Header.Get("Date"); date == "" {
		if date = r.calculatedReq.Header.Get("X-Amz-Date"); date == "" {
			return "", iodine.New(MissingDateHeader{}, nil)
		}
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return "", iodine.New(err, nil)
	}
	hashedPayload := r.getHashedPayload()
	signedHeaders := r.getSignedHeaders()
	canonicalRequest := r.getCanonicalRequest(hashedPayload)
	scope := r.getScope(t)
	stringToSign := r.getStringToSign(canonicalRequest, t)
	signingKey := r.getSigningKey(t)
	signature := r.getSignature(signingKey, stringToSign)

	// final Authorization header
	parts := []string{
		authHeader + " Credential=" + r.user.AccessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	return auth, nil
}
