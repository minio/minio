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
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/iodine"
)

// Signature - local variables
type Signature struct {
	AccessKeyID     string
	SecretAccessKey string
	AuthHeader      string
	Request         *http.Request
}

const (
	authHeaderPrefix = "AWS4-HMAC-SHA256"
	iso8601Format    = "20060102T150405Z"
	yyyymmdd         = "20060102"
)

var ignoredHeaders = map[string]bool{
	"Authorization":   true,
	"Content-Type":    true,
	"Accept-Encoding": true,
	"Content-Length":  true,
	"User-Agent":      true,
	"Connection":      true,
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
func (r *Signature) extractSignedHeaders() map[string][]string {
	authFields := strings.Split(strings.TrimSpace(r.AuthHeader), ",")
	extractedHeaders := strings.Split(strings.Split(strings.TrimSpace(authFields[1]), "=")[1], ";")
	extractedSignedHeadersMap := make(map[string][]string)
	for _, header := range extractedHeaders {
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
	r.Request.URL.RawQuery = strings.Replace(r.Request.URL.Query().Encode(), "+", "%20", -1)
	encodedPath, _ := urlEncodeName(r.Request.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		r.Request.Method,
		encodedPath,
		r.Request.URL.RawQuery,
		r.getCanonicalHeaders(r.extractSignedHeaders()),
		r.getSignedHeaders(r.extractSignedHeaders()),
		r.Request.Header.Get(http.CanonicalHeaderKey("x-amz-content-sha256")),
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

// DoesSignatureMatch - Verify authorization header with calculated header in accordance with - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns true if matches, false other wise if error is not nil then it is always false
func (r *Signature) DoesSignatureMatch(hashedPayload string) (bool, error) {
	// set new calulated payload
	r.Request.Header.Set("X-Amz-Content-Sha256", hashedPayload)

	// Add date if not present
	var date string
	if date = r.Request.Header.Get(http.CanonicalHeaderKey("x-amz-date")); date == "" {
		if date = r.Request.Header.Get("Date"); date == "" {
			return false, iodine.New(MissingDateHeader{}, nil)
		}
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil {
		return false, iodine.New(err, nil)
	}
	canonicalRequest := r.getCanonicalRequest()
	stringToSign := r.getStringToSign(canonicalRequest, t)
	signingKey := r.getSigningKey(t)
	newSignature := r.getSignature(signingKey, stringToSign)

	authFields := strings.Split(strings.TrimSpace(r.AuthHeader), ",")
	signature := strings.Split(strings.TrimSpace(authFields[2]), "=")[1]
	if newSignature != signature {
		return false, nil
	}
	return true, nil
}
