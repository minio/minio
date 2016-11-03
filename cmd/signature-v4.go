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

// Package cmd This file implements helper functions to validate AWS
// Signature Version '4' authorization header.
//
// This package provides comprehensive helpers for following signature
// types.
// - Based on Authorization header.
// - Based on Query parameters.
// - Based on Form POST policy.
package cmd

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/sha256-simd"
)

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header, host string) string {
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
			buf.WriteString(host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(signV4TrimAll(v))
			}
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
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
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method, host string) string {
	rawQuery := strings.Replace(queryStr, "+", "%20", -1)
	encodedPath := getURLEncodedName(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders, host),
		getSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		region,
		"s3",
		"aws4_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, region string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + getScope(t, region) + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// Check to see if Policy is signed correctly.
func doesPolicySignatureMatch(formValues map[string]string) APIErrorCode {
	// For SignV2 - Signature field will be valid
	if formValues["Signature"] != "" {
		return doesPolicySignatureV2Match(formValues)
	}
	return doesPolicySignatureV4Match(formValues)
}

// doesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns ErrNone if the signature matches.
func doesPolicySignatureV4Match(formValues map[string]string) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Server region.
	region := serverConfig.GetRegion()

	// Parse credential tag.
	credHeader, err := parseCredentialHeader("Credential=" + formValues["X-Amz-Credential"])
	if err != ErrNone {
		return ErrMissingFields
	}

	// Verify if the access key id matches.
	if credHeader.accessKey != cred.AccessKeyID {
		return ErrInvalidAccessKeyID
	}

	// Verify if the region is valid.
	sRegion := credHeader.scope.region
	if !isValidRegion(sRegion, region) {
		return ErrInvalidRegion
	}

	// Parse date string.
	t, e := time.Parse(iso8601Format, formValues["X-Amz-Date"])
	if e != nil {
		return ErrMalformedDate
	}

	// Get signing key.
	signingKey := getSigningKey(cred.SecretAccessKey, t, region)

	// Get signature.
	newSignature := getSignature(signingKey, formValues["Policy"])

	// Verify signature.
	if newSignature != formValues["X-Amz-Signature"] {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}

// doesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns ErrNone if the signature matches.
func doesPresignedSignatureMatch(hashedPayload string, r *http.Request, region string) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Copy request
	req := *r

	// Parse request query string.
	pSignValues, err := parsePreSignV4(req.URL.Query())
	if err != ErrNone {
		return err
	}

	// Verify if the access key id matches.
	if pSignValues.Credential.accessKey != cred.AccessKeyID {
		return ErrInvalidAccessKeyID
	}

	// Hashed payload mismatch, return content sha256 mismatch.
	contentSha256 := req.URL.Query().Get("X-Amz-Content-Sha256")
	if contentSha256 != "" && hashedPayload != contentSha256 {
		return ErrContentSHA256Mismatch
	}

	// Verify if region is valid.
	sRegion := pSignValues.Credential.scope.region
	// Should validate region, only if region is set.
	if region == "" {
		region = sRegion
	}
	if !isValidRegion(sRegion, region) {
		return ErrInvalidRegion
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(pSignValues.SignedHeaders, req.Header)
	if errCode != ErrNone {
		return errCode
	}
	// Construct new query.
	query := make(url.Values)
	if contentSha256 != "" {
		query.Set("X-Amz-Content-Sha256", hashedPayload)
	}

	query.Set("X-Amz-Algorithm", signV4Algorithm)

	if pSignValues.Date.After(time.Now().UTC()) {
		return ErrRequestNotReadyYet
	}

	if time.Now().UTC().Sub(pSignValues.Date) > time.Duration(pSignValues.Expires) {
		return ErrExpiredPresignRequest
	}

	// Save the date and expires.
	t := pSignValues.Date
	expireSeconds := int(time.Duration(pSignValues.Expires) / time.Second)

	// Construct the query.
	query.Set("X-Amz-Date", t.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.Itoa(expireSeconds))
	query.Set("X-Amz-SignedHeaders", getSignedHeaders(extractedSignedHeaders))
	query.Set("X-Amz-Credential", cred.AccessKeyID+"/"+getScope(t, sRegion))

	// Save other headers available in the request parameters.
	for k, v := range req.URL.Query() {
		if strings.HasPrefix(strings.ToLower(k), "x-amz") {
			continue
		}
		query[k] = v
	}

	// Get the encoded query.
	encodedQuery := query.Encode()

	// Verify if date query is same.
	if req.URL.Query().Get("X-Amz-Date") != query.Get("X-Amz-Date") {
		return ErrSignatureDoesNotMatch
	}
	// Verify if expires query is same.
	if req.URL.Query().Get("X-Amz-Expires") != query.Get("X-Amz-Expires") {
		return ErrSignatureDoesNotMatch
	}
	// Verify if signed headers query is same.
	if req.URL.Query().Get("X-Amz-SignedHeaders") != query.Get("X-Amz-SignedHeaders") {
		return ErrSignatureDoesNotMatch
	}
	// Verify if credential query is same.
	if req.URL.Query().Get("X-Amz-Credential") != query.Get("X-Amz-Credential") {
		return ErrSignatureDoesNotMatch
	}
	// Verify if sha256 payload query is same.
	if req.URL.Query().Get("X-Amz-Content-Sha256") != "" {
		if req.URL.Query().Get("X-Amz-Content-Sha256") != query.Get("X-Amz-Content-Sha256") {
			return ErrSignatureDoesNotMatch
		}
	}

	/// Verify finally if signature is same.

	// Get canonical request.
	presignedCanonicalReq := getCanonicalRequest(extractedSignedHeaders, hashedPayload, encodedQuery, req.URL.Path, req.Method, req.Host)

	// Get string to sign from canonical request.
	presignedStringToSign := getStringToSign(presignedCanonicalReq, t, region)

	// Get hmac presigned signing key.
	presignedSigningKey := getSigningKey(cred.SecretAccessKey, t, region)

	// Get new signature.
	newSignature := getSignature(presignedSigningKey, presignedStringToSign)

	// Verify signature.
	if req.URL.Query().Get("X-Amz-Signature") != newSignature {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}

// doesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns ErrNone if signature matches.
func doesSignatureMatch(hashedPayload string, r *http.Request, region string) APIErrorCode {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth)
	if err != ErrNone {
		return err
	}

	// Hashed payload mismatch, return content sha256 mismatch.
	if hashedPayload != req.Header.Get("X-Amz-Content-Sha256") {
		return ErrContentSHA256Mismatch
	}

	header := req.Header

	// Signature-V4 spec excludes Content-Length from signed headers list for signature calculation.
	// But some clients deviate from this rule. Hence we consider Content-Length for signature
	// calculation to be compatible with such clients.
	for _, h := range signV4Values.SignedHeaders {
		if h == "content-length" {
			header = cloneHeader(req.Header)
			header.Set("content-length", strconv.FormatInt(r.ContentLength, 10))
			break
		}
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, header)
	if errCode != ErrNone {
		return errCode
	}

	// Verify if the access key id matches.
	if signV4Values.Credential.accessKey != cred.AccessKeyID {
		return ErrInvalidAccessKeyID
	}

	// Verify if region is valid.
	sRegion := signV4Values.Credential.scope.region
	// Region is set to be empty, we use whatever was sent by the
	// request and proceed further. This is a work-around to address
	// an important problem for ListBuckets() getting signed with
	// different regions.
	if region == "" {
		region = sRegion
	}
	// Should validate region, only if region is set.
	if !isValidRegion(sRegion, region) {
		return ErrInvalidRegion
	}

	// Extract date, if not present throw error.
	var date string
	if date = req.Header.Get(http.CanonicalHeaderKey("x-amz-date")); date == "" {
		if date = r.Header.Get("Date"); date == "" {
			return ErrMissingDateHeader
		}
	}
	// Parse date header.
	t, e := time.Parse(iso8601Format, date)
	if e != nil {
		return ErrMalformedDate
	}

	// Query string.
	queryStr := req.URL.Query().Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method, req.Host)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, region)

	// Get hmac signing key.
	signingKey := getSigningKey(cred.SecretAccessKey, t, region)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if newSignature != signV4Values.Signature {
		return ErrSignatureDoesNotMatch
	}

	// Return error none.
	return ErrNone
}
