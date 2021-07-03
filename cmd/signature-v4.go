// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

type serviceType string

const (
	serviceS3  serviceType = "s3"
	serviceSTS serviceType = "sts"
)

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
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
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
	rawQuery := strings.Replace(queryStr, "+", "%20", -1)
	encodedPath := s3utils.EncodePath(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders),
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
		string(serviceS3),
		"aws4_request",
	}, SlashSeparator)
	return scope
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, t time.Time, region string, stype serviceType) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte(stype))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// Check to see if Policy is signed correctly.
func doesPolicySignatureMatch(formValues http.Header) (auth.Credentials, APIErrorCode) {
	// For SignV2 - Signature field will be valid
	if _, ok := formValues["Signature"]; ok {
		return doesPolicySignatureV2Match(formValues)
	}
	return doesPolicySignatureV4Match(formValues)
}

// compareSignatureV4 returns true if and only if both signatures
// are equal. The signatures are expected to be HEX encoded strings
// according to the AWS S3 signature V4 spec.
func compareSignatureV4(sig1, sig2 string) bool {
	// The CTC using []byte(str) works because the hex encoding
	// is unique for a sequence of bytes. See also compareSignatureV2.
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}

// doesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns ErrNone if the signature matches.
func doesPolicySignatureV4Match(formValues http.Header) (auth.Credentials, APIErrorCode) {
	// Server region.
	region := globalServerRegion

	// Parse credential tag.
	credHeader, s3Err := parseCredentialHeader("Credential="+formValues.Get(xhttp.AmzCredential), region, serviceS3)
	if s3Err != ErrNone {
		return auth.Credentials{}, s3Err
	}

	cred, _, s3Err := checkKeyValid(credHeader.accessKey)
	if s3Err != ErrNone {
		return cred, s3Err
	}

	// Get signing key.
	signingKey := getSigningKey(cred.SecretKey, credHeader.scope.date, credHeader.scope.region, serviceS3)

	// Get signature.
	newSignature := getSignature(signingKey, formValues.Get("Policy"))

	// Verify signature.
	if !compareSignatureV4(newSignature, formValues.Get(xhttp.AmzSignature)) {
		return cred, ErrSignatureDoesNotMatch
	}

	// Success.
	return cred, ErrNone
}

// doesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns ErrNone if the signature matches.
func doesPresignedSignatureMatch(hashedPayload string, r *http.Request, region string, stype serviceType) APIErrorCode {
	// Copy request
	req := *r

	// Parse request query string.
	pSignValues, err := parsePreSignV4(req.URL.Query(), region, stype)
	if err != ErrNone {
		return err
	}

	cred, _, s3Err := checkKeyValid(pSignValues.Credential.accessKey)
	if s3Err != ErrNone {
		return s3Err
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(pSignValues.SignedHeaders, r)
	if errCode != ErrNone {
		return errCode
	}

	// If the host which signed the request is slightly ahead in time (by less than globalMaxSkewTime) the
	// request should still be allowed.
	if pSignValues.Date.After(UTCNow().Add(globalMaxSkewTime)) {
		return ErrRequestNotReadyYet
	}

	if UTCNow().Sub(pSignValues.Date) > pSignValues.Expires {
		return ErrExpiredPresignRequest
	}

	// Save the date and expires.
	t := pSignValues.Date
	expireSeconds := int(pSignValues.Expires / time.Second)

	// Construct new query.
	query := make(url.Values)
	clntHashedPayload := req.URL.Query().Get(xhttp.AmzContentSha256)
	if clntHashedPayload != "" {
		query.Set(xhttp.AmzContentSha256, hashedPayload)
	}

	token := req.URL.Query().Get(xhttp.AmzSecurityToken)
	if token != "" {
		query.Set(xhttp.AmzSecurityToken, cred.SessionToken)
	}

	query.Set(xhttp.AmzAlgorithm, signV4Algorithm)

	// Construct the query.
	query.Set(xhttp.AmzDate, t.Format(iso8601Format))
	query.Set(xhttp.AmzExpires, strconv.Itoa(expireSeconds))
	query.Set(xhttp.AmzSignedHeaders, getSignedHeaders(extractedSignedHeaders))
	query.Set(xhttp.AmzCredential, cred.AccessKey+SlashSeparator+pSignValues.Credential.getScope())

	defaultSigParams := set.CreateStringSet(
		xhttp.AmzContentSha256,
		xhttp.AmzSecurityToken,
		xhttp.AmzAlgorithm,
		xhttp.AmzDate,
		xhttp.AmzExpires,
		xhttp.AmzSignedHeaders,
		xhttp.AmzCredential,
		xhttp.AmzSignature,
	)

	// Add missing query parameters if any provided in the request URL
	for k, v := range req.URL.Query() {
		if !defaultSigParams.Contains(k) {
			query[k] = v
		}
	}

	// Get the encoded query.
	encodedQuery := query.Encode()

	// Verify if date query is same.
	if req.URL.Query().Get(xhttp.AmzDate) != query.Get(xhttp.AmzDate) {
		return ErrSignatureDoesNotMatch
	}
	// Verify if expires query is same.
	if req.URL.Query().Get(xhttp.AmzExpires) != query.Get(xhttp.AmzExpires) {
		return ErrSignatureDoesNotMatch
	}
	// Verify if signed headers query is same.
	if req.URL.Query().Get(xhttp.AmzSignedHeaders) != query.Get(xhttp.AmzSignedHeaders) {
		return ErrSignatureDoesNotMatch
	}
	// Verify if credential query is same.
	if req.URL.Query().Get(xhttp.AmzCredential) != query.Get(xhttp.AmzCredential) {
		return ErrSignatureDoesNotMatch
	}
	// Verify if sha256 payload query is same.
	if clntHashedPayload != "" && clntHashedPayload != query.Get(xhttp.AmzContentSha256) {
		return ErrContentSHA256Mismatch
	}
	// Verify if security token is correct.
	if token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(cred.SessionToken)) != 1 {
		return ErrInvalidToken
	}

	/// Verify finally if signature is same.

	// Get canonical request.
	presignedCanonicalReq := getCanonicalRequest(extractedSignedHeaders, hashedPayload, encodedQuery, req.URL.Path, req.Method)

	// Get string to sign from canonical request.
	presignedStringToSign := getStringToSign(presignedCanonicalReq, t, pSignValues.Credential.getScope())

	// Get hmac presigned signing key.
	presignedSigningKey := getSigningKey(cred.SecretKey, pSignValues.Credential.scope.date,
		pSignValues.Credential.scope.region, stype)

	// Get new signature.
	newSignature := getSignature(presignedSigningKey, presignedStringToSign)

	// Verify signature.
	if !compareSignatureV4(req.URL.Query().Get(xhttp.AmzSignature), newSignature) {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}

// doesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns ErrNone if signature matches.
func doesSignatureMatch(hashedPayload string, r *http.Request, region string, stype serviceType) APIErrorCode {
	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get(xhttp.Authorization)

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth, region, stype)
	if err != ErrNone {
		return err
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if errCode != ErrNone {
		return errCode
	}

	cred, _, s3Err := checkKeyValid(signV4Values.Credential.accessKey)
	if s3Err != ErrNone {
		return s3Err
	}

	// Extract date, if not present throw error.
	var date string
	if date = req.Header.Get(xhttp.AmzDate); date == "" {
		if date = r.Header.Get(xhttp.Date); date == "" {
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
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, signV4Values.Credential.getScope())

	// Get hmac signing key.
	signingKey := getSigningKey(cred.SecretKey, signV4Values.Credential.scope.date,
		signV4Values.Credential.scope.region, stype)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return ErrSignatureDoesNotMatch
	}

	// Return error none.
	return ErrNone
}
