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

package cmd

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

// credentialHeader data type represents structured form of Credential
// string from authorization header.
type credentialHeader struct {
	accessKey string
	scope     struct {
		date    time.Time
		region  string
		service string
		request string
	}
}

// Return scope string.
func (c credentialHeader) getScope() string {
	return strings.Join([]string{
		c.scope.date.Format(yyyymmdd),
		c.scope.region,
		c.scope.service,
		c.scope.request,
	}, SlashSeparator)
}

func getReqAccessKeyV4(r *http.Request, region string, stype serviceType) (auth.Credentials, bool, APIErrorCode) {
	ch, s3Err := parseCredentialHeader("Credential="+r.URL.Query().Get(xhttp.AmzCredential), region, stype)
	if s3Err != ErrNone {
		// Strip off the Algorithm prefix.
		v4Auth := strings.TrimPrefix(r.Header.Get("Authorization"), signV4Algorithm)
		authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
		if len(authFields) != 3 {
			return auth.Credentials{}, false, ErrMissingFields
		}
		ch, s3Err = parseCredentialHeader(authFields[0], region, stype)
		if s3Err != ErrNone {
			return auth.Credentials{}, false, s3Err
		}
	}
	return checkKeyValid(ch.accessKey)
}

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string, region string, stype serviceType) (ch credentialHeader, aec APIErrorCode) {
	creds := strings.SplitN(strings.TrimSpace(credElement), "=", 2)
	if len(creds) != 2 {
		return ch, ErrMissingFields
	}
	if creds[0] != "Credential" {
		return ch, ErrMissingCredTag
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), SlashSeparator)
	if len(credElements) < 5 {
		return ch, ErrCredMalformed
	}
	accessKey := strings.Join(credElements[:len(credElements)-4], SlashSeparator) // The access key may contain one or more `/`
	if !auth.IsAccessKeyValid(accessKey) {
		return ch, ErrInvalidAccessKeyID
	}
	// Save access key id.
	cred := credentialHeader{
		accessKey: accessKey,
	}
	credElements = credElements[len(credElements)-4:]
	var e error
	cred.scope.date, e = time.Parse(yyyymmdd, credElements[0])
	if e != nil {
		return ch, ErrMalformedCredentialDate
	}

	cred.scope.region = credElements[1]
	// Verify if region is valid.
	sRegion := cred.scope.region
	// Region is set to be empty, we use whatever was sent by the
	// request and proceed further. This is a work-around to address
	// an important problem for ListBuckets() getting signed with
	// different regions.
	if region == "" {
		region = sRegion
	}
	// Should validate region, only if region is set.
	if !isValidRegion(sRegion, region) {
		return ch, ErrAuthorizationHeaderMalformed

	}
	if credElements[2] != string(stype) {
		switch stype {
		case serviceSTS:
			return ch, ErrInvalidServiceSTS
		}
		return ch, ErrInvalidServiceS3
	}
	cred.scope.service = credElements[2]
	if credElements[3] != "aws4_request" {
		return ch, ErrInvalidRequestVersion
	}
	cred.scope.request = credElements[3]
	return cred, ErrNone
}

// Parse signature from signature tag.
func parseSignature(signElement string) (string, APIErrorCode) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if len(signFields) != 2 {
		return "", ErrMissingFields
	}
	if signFields[0] != "Signature" {
		return "", ErrMissingSignTag
	}
	if signFields[1] == "" {
		return "", ErrMissingFields
	}
	signature := signFields[1]
	return signature, ErrNone
}

// Parse slice of signed headers from signed headers tag.
func parseSignedHeader(signedHdrElement string) ([]string, APIErrorCode) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if len(signedHdrFields) != 2 {
		return nil, ErrMissingFields
	}
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, ErrMissingSignHeadersTag
	}
	if signedHdrFields[1] == "" {
		return nil, ErrMissingFields
	}
	signedHeaders := strings.Split(signedHdrFields[1], ";")
	return signedHeaders, ErrNone
}

// signValues data type represents structured form of AWS Signature V4 header.
type signValues struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
}

// preSignValues data type represents structued form of AWS Signature V4 query string.
type preSignValues struct {
	signValues
	Date    time.Time
	Expires time.Duration
}

// Parses signature version '4' query string of the following form.
//
//   querystring = X-Amz-Algorithm=algorithm
//   querystring += &X-Amz-Credential= urlencode(accessKey + '/' + credential_scope)
//   querystring += &X-Amz-Date=date
//   querystring += &X-Amz-Expires=timeout interval
//   querystring += &X-Amz-SignedHeaders=signed_headers
//   querystring += &X-Amz-Signature=signature
//
// verifies if any of the necessary query params are missing in the presigned request.
func doesV4PresignParamsExist(query url.Values) APIErrorCode {
	v4PresignQueryParams := []string{xhttp.AmzAlgorithm, xhttp.AmzCredential, xhttp.AmzSignature, xhttp.AmzDate, xhttp.AmzSignedHeaders, xhttp.AmzExpires}
	for _, v4PresignQueryParam := range v4PresignQueryParams {
		if _, ok := query[v4PresignQueryParam]; !ok {
			return ErrInvalidQueryParams
		}
	}
	return ErrNone
}

// Parses all the presigned signature values into separate elements.
func parsePreSignV4(query url.Values, region string, stype serviceType) (psv preSignValues, aec APIErrorCode) {
	// verify whether the required query params exist.
	aec = doesV4PresignParamsExist(query)
	if aec != ErrNone {
		return psv, aec
	}

	// Verify if the query algorithm is supported or not.
	if query.Get(xhttp.AmzAlgorithm) != signV4Algorithm {
		return psv, ErrInvalidQuerySignatureAlgo
	}

	// Initialize signature version '4' structured header.
	preSignV4Values := preSignValues{}

	// Save credential.
	preSignV4Values.Credential, aec = parseCredentialHeader("Credential="+query.Get(xhttp.AmzCredential), region, stype)
	if aec != ErrNone {
		return psv, aec
	}

	var e error
	// Save date in native time.Time.
	preSignV4Values.Date, e = time.Parse(iso8601Format, query.Get(xhttp.AmzDate))
	if e != nil {
		return psv, ErrMalformedPresignedDate
	}

	// Save expires in native time.Duration.
	preSignV4Values.Expires, e = time.ParseDuration(query.Get(xhttp.AmzExpires) + "s")
	if e != nil {
		return psv, ErrMalformedExpires
	}

	if preSignV4Values.Expires < 0 {
		return psv, ErrNegativeExpires
	}

	// Check if Expiry time is less than 7 days (value in seconds).
	if preSignV4Values.Expires.Seconds() > 604800 {
		return psv, ErrMaximumExpires
	}

	// Save signed headers.
	preSignV4Values.SignedHeaders, aec = parseSignedHeader("SignedHeaders=" + query.Get(xhttp.AmzSignedHeaders))
	if aec != ErrNone {
		return psv, aec
	}

	// Save signature.
	preSignV4Values.Signature, aec = parseSignature("Signature=" + query.Get(xhttp.AmzSignature))
	if aec != ErrNone {
		return psv, aec
	}

	// Return structed form of signature query string.
	return preSignV4Values, ErrNone
}

// Parses signature version '4' header of the following form.
//
//    Authorization: algorithm Credential=accessKeyID/credScope, \
//            SignedHeaders=signedHeaders, Signature=signature
//
func parseSignV4(v4Auth string, region string, stype serviceType) (sv signValues, aec APIErrorCode) {
	// credElement is fetched first to skip replacing the space in access key.
	credElement := strings.TrimPrefix(strings.Split(strings.TrimSpace(v4Auth), ",")[0], signV4Algorithm)
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.Replace(v4Auth, " ", "", -1)
	if v4Auth == "" {
		return sv, ErrAuthHeaderEmpty
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v4Auth, signV4Algorithm) {
		return sv, ErrSignatureVersionNotSupported
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return sv, ErrMissingFields
	}

	// Initialize signature version '4' structured header.
	signV4Values := signValues{}

	var s3Err APIErrorCode
	// Save credentail values.
	signV4Values.Credential, s3Err = parseCredentialHeader(strings.TrimSpace(credElement), region, stype)
	if s3Err != ErrNone {
		return sv, s3Err
	}

	// Save signed headers.
	signV4Values.SignedHeaders, s3Err = parseSignedHeader(authFields[1])
	if s3Err != ErrNone {
		return sv, s3Err
	}

	// Save signature.
	signV4Values.Signature, s3Err = parseSignature(authFields[2])
	if s3Err != ErrNone {
		return sv, s3Err
	}

	// Return the structure here.
	return signV4Values, ErrNone
}
