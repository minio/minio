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

// Package signature4 implements helper functions to validate AWS
// Signature Version '4' authorization header.
//
// This package provides comprehensive helpers for following signature
// types.
// - Based on Authorization header.
// - Based on Query parameters.
// - Based on Form POST policy.
package signature4

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/probe"
)

// Sign - local variables
type Sign struct {
	accessKeyID            string
	secretAccessKey        string
	region                 string
	httpRequest            *http.Request
	extractedSignedHeaders http.Header
}

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

// New - initialize a new authorization checkes.
func New(accessKeyID, secretAccessKey, region string) (*Sign, *probe.Error) {
	if !isValidAccessKey.MatchString(accessKeyID) {
		return nil, ErrInvalidAccessKeyID("Invalid access key id.", accessKeyID).Trace(accessKeyID)
	}
	if !isValidSecretKey.MatchString(secretAccessKey) {
		return nil, ErrInvalidAccessKeyID("Invalid secret key.", secretAccessKey).Trace(secretAccessKey)
	}
	if region == "" {
		return nil, ErrRegionISEmpty("Region is empty.").Trace()
	}
	signature := &Sign{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		region:          region,
	}
	return signature, nil
}

// SetHTTPRequestToVerify - sets the http request which needs to be verified.
func (s *Sign) SetHTTPRequestToVerify(r *http.Request) Sign {
	// Do not set http request if its 'nil'.
	if r == nil {
		return *s
	}
	s.httpRequest = r
	return *s
}

// getCanonicalHeaders generate a list of request headers with their values
func (s Sign) getCanonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
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
			buf.WriteString(s.httpRequest.Host)
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
func (s Sign) getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
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
func (s *Sign) getCanonicalRequest() string {
	payload := s.httpRequest.Header.Get(http.CanonicalHeaderKey("x-amz-content-sha256"))
	s.httpRequest.URL.RawQuery = strings.Replace(s.httpRequest.URL.Query().Encode(), "+", "%20", -1)
	encodedPath := getURLEncodedName(s.httpRequest.URL.Path)
	// Convert any space strings back to "+".
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		s.httpRequest.Method,
		encodedPath,
		s.httpRequest.URL.RawQuery,
		s.getCanonicalHeaders(s.extractedSignedHeaders),
		s.getSignedHeaders(s.extractedSignedHeaders),
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
func (s Sign) getPresignedCanonicalRequest(presignedQuery string) string {
	rawQuery := strings.Replace(presignedQuery, "+", "%20", -1)
	encodedPath := getURLEncodedName(s.httpRequest.URL.Path)
	// Convert any space strings back to "+".
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		s.httpRequest.Method,
		encodedPath,
		rawQuery,
		s.getCanonicalHeaders(s.extractedSignedHeaders),
		s.getSignedHeaders(s.extractedSignedHeaders),
		"UNSIGNED-PAYLOAD",
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service.
func (s Sign) getScope(t time.Time) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		s.region,
		"s3",
		"aws4_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values.
func (s Sign) getStringToSign(canonicalRequest string, t time.Time) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + s.getScope(t) + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func (s Sign) getSigningKey(t time.Time) []byte {
	secret := s.secretAccessKey
	date := sumHMAC([]byte("AWS4"+secret), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte(s.region))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func (s Sign) getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// DoesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (s *Sign) DoesPolicySignatureMatch(formValues map[string]string) (bool, *probe.Error) {
	// Parse credential tag.
	credential, err := parseCredential("Credential=" + formValues["X-Amz-Credential"])
	if err != nil {
		return false, err.Trace(formValues["X-Amz-Credential"])
	}

	// Verify if the access key id matches.
	if credential.accessKeyID != s.accessKeyID {
		return false, ErrInvalidAccessKeyID("Access key id does not match with our records.", credential.accessKeyID).Trace(credential.accessKeyID)
	}

	// Verify if the region is valid.
	reqRegion := credential.scope.region
	if !isValidRegion(reqRegion, s.region) {
		return false, ErrInvalidRegion("Requested region is not recognized.", reqRegion).Trace(reqRegion)
	}

	// Save region.
	s.region = reqRegion

	// Parse date string.
	t, e := time.Parse(iso8601Format, formValues["X-Amz-Date"])
	if e != nil {
		return false, probe.NewError(e)
	}
	signingKey := s.getSigningKey(t)
	newSignature := s.getSignature(signingKey, formValues["Policy"])
	if newSignature != formValues["X-Amz-Signature"] {
		return false, nil
	}
	return true, nil
}

// DoesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (s *Sign) DoesPresignedSignatureMatch() (bool, *probe.Error) {
	// Parse request query string.
	preSignValues, err := parsePreSignV4(s.httpRequest.URL.Query())
	if err != nil {
		return false, err.Trace(s.httpRequest.URL.String())
	}

	// Verify if the access key id matches.
	if preSignValues.Credential.accessKeyID != s.accessKeyID {
		return false, ErrInvalidAccessKeyID("Access key id does not match with our records.", preSignValues.Credential.accessKeyID).Trace(preSignValues.Credential.accessKeyID)
	}

	// Verify if region is valid.
	reqRegion := preSignValues.Credential.scope.region
	if !isValidRegion(reqRegion, s.region) {
		return false, ErrInvalidRegion("Requested region is not recognized.", reqRegion).Trace(reqRegion)
	}

	// Save region.
	s.region = reqRegion

	// Extract all the signed headers along with its values.
	s.extractedSignedHeaders = extractSignedHeaders(preSignValues.SignedHeaders, s.httpRequest.Header)

	// Construct new query.
	query := make(url.Values)
	query.Set("X-Amz-Algorithm", signV4Algorithm)

	if time.Now().UTC().Sub(preSignValues.Date) > time.Duration(preSignValues.Expires) {
		return false, ErrExpiredPresignRequest("Presigned request already expired, please initiate a new request.")
	}

	// Save the date and expires.
	t := preSignValues.Date
	expireSeconds := int(time.Duration(preSignValues.Expires) / time.Second)

	// Construct the query.
	query.Set("X-Amz-Date", t.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.Itoa(expireSeconds))
	query.Set("X-Amz-SignedHeaders", s.getSignedHeaders(s.extractedSignedHeaders))
	query.Set("X-Amz-Credential", s.accessKeyID+"/"+s.getScope(t))

	// Save other headers available in the request parameters.
	for k, v := range s.httpRequest.URL.Query() {
		if strings.HasPrefix(strings.ToLower(k), "x-amz") {
			continue
		}
		query[k] = v
	}

	// Get the encoded query.
	encodedQuery := query.Encode()

	// Verify if date query is same.
	if s.httpRequest.URL.Query().Get("X-Amz-Date") != query.Get("X-Amz-Date") {
		return false, nil
	}
	// Verify if expires query is same.
	if s.httpRequest.URL.Query().Get("X-Amz-Expires") != query.Get("X-Amz-Expires") {
		return false, nil
	}
	// Verify if signed headers query is same.
	if s.httpRequest.URL.Query().Get("X-Amz-SignedHeaders") != query.Get("X-Amz-SignedHeaders") {
		return false, nil
	}
	// Verify if credential query is same.
	if s.httpRequest.URL.Query().Get("X-Amz-Credential") != query.Get("X-Amz-Credential") {
		return false, nil
	}
	// Verify finally if signature is same.
	newSignature := s.getSignature(s.getSigningKey(t), s.getStringToSign(s.getPresignedCanonicalRequest(encodedQuery), t))
	if s.httpRequest.URL.Query().Get("X-Amz-Signature") != newSignature {
		return false, nil
	}
	return true, nil
}

// DoesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func (s *Sign) DoesSignatureMatch(hashedPayload string) (bool, *probe.Error) {
	// Save authorization header.
	v4Auth := s.httpRequest.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth)
	if err != nil {
		return false, err.Trace(v4Auth)
	}

	// Extract all the signed headers along with its values.
	s.extractedSignedHeaders = extractSignedHeaders(signV4Values.SignedHeaders, s.httpRequest.Header)

	// Verify if the access key id matches.
	if signV4Values.Credential.accessKeyID != s.accessKeyID {
		return false, ErrInvalidAccessKeyID("Access key id does not match with our records.", signV4Values.Credential.accessKeyID).Trace(signV4Values.Credential.accessKeyID, s.accessKeyID)
	}

	// Verify if region is valid.
	reqRegion := signV4Values.Credential.scope.region
	if !isValidRegion(reqRegion, s.region) {
		return false, ErrInvalidRegion("Requested region is not recognized.", reqRegion).Trace(reqRegion)
	}

	// Save region.
	s.region = reqRegion

	// Set input payload.
	s.httpRequest.Header.Set("X-Amz-Content-Sha256", hashedPayload)

	// Extract date, if not present throw error.
	var date string
	if date = s.httpRequest.Header.Get(http.CanonicalHeaderKey("x-amz-date")); date == "" {
		if date = s.httpRequest.Header.Get("Date"); date == "" {
			return false, ErrMissingDateHeader("Date header is missing from the request.").Trace()
		}
	}
	// Parse date header.
	t, e := time.Parse(iso8601Format, date)
	if e != nil {
		return false, probe.NewError(e)
	}

	// Signature version '4'.
	canonicalRequest := s.getCanonicalRequest()
	stringToSign := s.getStringToSign(canonicalRequest, t)
	signingKey := s.getSigningKey(t)
	newSignature := s.getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if newSignature != signV4Values.Signature {
		return false, nil
	}
	return true, nil
}
