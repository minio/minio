/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package signature

import (
	"bytes"
	"crypto/hmac"
	"encoding/hex"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/probe"
)

// Signature - local variables
type Signature struct {
	AccessKeyID     string
	SecretAccessKey string
	Presigned       bool
	PresignedPolicy []byte
	SignedHeaders   []string
	Signature       string
	Request         *http.Request
}

const (
	authHeaderPrefix = "AWS4-HMAC-SHA256"
	iso8601Format    = "20060102T150405Z"
	yyyymmdd         = "20060102"
)

// sumHMAC calculate hmac between two input byte array
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// getURLEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func getURLEncodedName(name string) string {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
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
			if len < 0 {
				return name
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedName = encodedName + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedName
}

// getCanonicalHeaders generate a list of request headers with their values
func (r *Signature) getCanonicalHeaders(signedHeaders map[string][]string) string {
	var headers []string
	vals := make(map[string][]string)
	for k, vv := range signedHeaders {
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
			buf.WriteString(r.Request.Host)
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
func (r *Signature) getSignedHeaders(signedHeaders map[string][]string) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// extractSignedHeaders extract signed headers from Authorization header
func (r Signature) extractSignedHeaders() map[string][]string {
	extractedSignedHeadersMap := make(map[string][]string)
	for _, header := range r.SignedHeaders {
		val, ok := r.Request.Header[http.CanonicalHeaderKey(header)]
		if !ok {
			// if not found continue, we will fail later
			continue
		}
		extractedSignedHeadersMap[header] = val
	}
	return extractedSignedHeadersMap
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
func (r *Signature) getCanonicalRequest() string {
	payload := r.Request.Header.Get(http.CanonicalHeaderKey("x-amz-content-sha256"))
	r.Request.URL.RawQuery = strings.Replace(r.Request.URL.Query().Encode(), "+", "%20", -1)
	encodedPath := getURLEncodedName(r.Request.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		r.Request.Method,
		encodedPath,
		r.Request.URL.RawQuery,
		r.getCanonicalHeaders(r.extractSignedHeaders()),
		r.getSignedHeaders(r.extractSignedHeaders()),
		payload,
	}, "\n")
	return canonicalRequest
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
func (r *Signature) getPresignedCanonicalRequest(presignedQuery string) string {
	rawQuery := strings.Replace(presignedQuery, "+", "%20", -1)
	encodedPath := getURLEncodedName(r.Request.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		r.Request.Method,
		encodedPath,
		rawQuery,
		r.getCanonicalHeaders(r.extractSignedHeaders()),
		r.getSignedHeaders(r.extractSignedHeaders()),
		"UNSIGNED-PAYLOAD",
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service
func (r *Signature) getScope(t time.Time) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"milkyway",
		"s3",
		"aws4_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values
func (r *Signature) getStringToSign(canonicalRequest string, t time.Time) string {
	stringToSign := authHeaderPrefix + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + r.getScope(t) + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sha256.Sum256([]byte(canonicalRequest)))
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature
func (r *Signature) getSigningKey(t time.Time) []byte {
	secret := r.SecretAccessKey
	date := sumHMAC([]byte("AWS4"+secret), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form
func (r *Signature) getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// DoesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (r *Signature) DoesPolicySignatureMatch(date string) (bool, *probe.Error) {
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return false, probe.NewError(err)
	}
	signingKey := r.getSigningKey(t)
	stringToSign := string(r.PresignedPolicy)
	newSignature := r.getSignature(signingKey, stringToSign)
	if newSignature != r.Signature {
		return false, nil
	}
	return true, nil
}

// DoesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (r *Signature) DoesPresignedSignatureMatch() (bool, *probe.Error) {
	query := make(url.Values)
	query.Set("X-Amz-Algorithm", authHeaderPrefix)

	var date string
	if date = r.Request.URL.Query().Get("X-Amz-Date"); date == "" {
		return false, probe.NewError(MissingDateHeader{})
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return false, probe.NewError(err)
	}
	if _, ok := r.Request.URL.Query()["X-Amz-Expires"]; !ok {
		return false, probe.NewError(MissingExpiresQuery{})
	}
	expireSeconds, err := strconv.Atoi(r.Request.URL.Query().Get("X-Amz-Expires"))
	if err != nil {
		return false, probe.NewError(err)
	}
	if time.Now().UTC().Sub(t) > time.Duration(expireSeconds)*time.Second {
		return false, probe.NewError(ExpiredPresignedRequest{})
	}
	query.Set("X-Amz-Date", t.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.Itoa(expireSeconds))
	query.Set("X-Amz-SignedHeaders", r.getSignedHeaders(r.extractSignedHeaders()))
	query.Set("X-Amz-Credential", r.AccessKeyID+"/"+r.getScope(t))

	encodedQuery := query.Encode()
	newSignature := r.getSignature(r.getSigningKey(t), r.getStringToSign(r.getPresignedCanonicalRequest(encodedQuery), t))
	encodedQuery += "&X-Amz-Signature=" + newSignature

	if encodedQuery != r.Request.URL.RawQuery {
		return false, nil
	}
	return true, nil
}

// DoesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (r *Signature) DoesSignatureMatch(hashedPayload string) (bool, *probe.Error) {
	// set new calulated payload
	r.Request.Header.Set("X-Amz-Content-Sha256", hashedPayload)

	// Add date if not present throw error
	var date string
	if date = r.Request.Header.Get(http.CanonicalHeaderKey("x-amz-date")); date == "" {
		if date = r.Request.Header.Get("Date"); date == "" {
			return false, probe.NewError(MissingDateHeader{})
		}
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return false, probe.NewError(err)
	}
	canonicalRequest := r.getCanonicalRequest()
	stringToSign := r.getStringToSign(canonicalRequest, t)
	signingKey := r.getSigningKey(t)
	newSignature := r.getSignature(signingKey, stringToSign)

	if newSignature != r.Signature {
		return false, nil
	}
	return true, nil
}
