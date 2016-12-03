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

package cmd

import (
	"net/url"
	"strings"
	"time"
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

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string) (credentialHeader, error) {
	creds := strings.Split(strings.TrimSpace(credElement), "=")
	if creds[0] != "Credential" {
		return credentialHeader{}, eMissingCredTag()
	}
	if len(creds) != 2 {
		return credentialHeader{}, eMissingFields()
	}
	if creds[1] == "" {
		return credentialHeader{}, eMissingFields()
	}
	accessKeyID := serverConfig.GetCredential().AccessKeyID
	credElements := strings.Split(strings.TrimSpace(creds[1]), "/")
	if len(credElements) != 5 {
		return credentialHeader{}, eCredMalformed(accessKeyID)
	}

	if !isValidAccessKey(credElements[0]) {
		return credentialHeader{}, eInvalidAccessKeyID()
	}

	// Save access key id.
	cred := credentialHeader{
		accessKey: credElements[0],
	}
	var err error
	cred.scope.date, err = time.Parse(yyyymmdd, credElements[1])
	if err != nil {
		errorIf(err, "Unable to parse credential date %s", credElements[1])
		return credentialHeader{}, eMalformedCredentialDate(credElements[1])
	}
	serverRegion := serverConfig.GetRegion()
	if credElements[2] == "" || credElements[2] != serverRegion {
		err := eMalformedCredentialRegion(serverRegion, credElements[2])
		return credentialHeader{}, err
	}
	cred.scope.region = credElements[2]
	if credElements[3] != "s3" {
		return credentialHeader{}, eInvalidService(credElements[3])
	}
	cred.scope.service = credElements[3]
	if credElements[4] != "aws4_request" {
		return credentialHeader{}, eInvalidReqVersion(credElements[4])
	}
	cred.scope.request = credElements[4]
	return cred, nil
}

// Parse signature from signature tag.
func parseSignature(signElement string) (string, error) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if signFields[0] != "Signature" {
		return "", eMissingSignTag()
	}
	if len(signFields) != 2 {
		return "", eMissingFields()
	}
	if signFields[1] == "" {
		return "", eMissingFields()
	}
	signature := signFields[1]
	return signature, nil
}

// Parse signed headers tag into slice of signed headers.
func parseSignedHeader(signedHdrElement string) ([]string, error) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, eMissingSignHeadersTag()
	}
	if len(signedHdrFields) != 2 {
		return nil, eMissingFields()
	}
	if signedHdrFields[1] == "" {
		return nil, eMissingFields()
	}
	signedHeaders := strings.Split(signedHdrFields[1], ";")
	return signedHeaders, nil
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
func doesV4PresignParamsExist(query url.Values) error {
	v4PresignQueryParams := []string{
		"X-Amz-Algorithm",
		"X-Amz-Credential",
		"X-Amz-Signature",
		"X-Amz-Date",
		"X-Amz-SignedHeaders",
		"X-Amz-Expires",
	}
	for _, v4PresignQueryParam := range v4PresignQueryParams {
		if _, ok := query[v4PresignQueryParam]; !ok {
			return eInvalidQueryParams()
		}
	}
	return nil
}

// Parses all the presigned signature values into separate elements.
func parsePreSignV4(query url.Values) (preSignValues, error) {
	// verify whether the required query params exist.
	err := doesV4PresignParamsExist(query)
	if err != nil {
		return preSignValues{}, err
	}

	// Verify if the query algorithm is supported or not.
	if query.Get("X-Amz-Algorithm") != signV4Algorithm {
		return preSignValues{}, eInvalidQuerySignAlgo()
	}

	// Initialize signature version '4' structured header.
	preSignV4Values := preSignValues{}

	// Save credential.
	preSignV4Values.Credential, err = parseCredentialHeader("Credential=" + query.Get("X-Amz-Credential"))
	if err != nil {
		return preSignValues{}, err
	}

	var e error
	// Save date in native time.Time.
	preSignV4Values.Date, e = time.Parse(iso8601Format, query.Get("X-Amz-Date"))
	if e != nil {
		return preSignValues{}, eMalformedPresignedDate(query.Get("X-Amz-Date"))
	}

	// Save expires in native time.Duration.
	preSignV4Values.Expires, e = time.ParseDuration(query.Get("X-Amz-Expires") + "s")
	if e != nil {
		return preSignValues{}, eMalformedExpires()
	}

	if preSignV4Values.Expires < 0 {
		return preSignValues{}, eNegativeExpires()
	}
	// Save signed headers.
	preSignV4Values.SignedHeaders, err = parseSignedHeader("SignedHeaders=" + query.Get("X-Amz-SignedHeaders"))
	if err != nil {
		return preSignValues{}, err
	}

	// Save signature.
	preSignV4Values.Signature, err = parseSignature("Signature=" + query.Get("X-Amz-Signature"))
	if err != nil {
		return preSignValues{}, err
	}

	// Return structured form of signature query string.
	return preSignV4Values, nil
}

// Parses signature version '4' header of the following form.
//
//    Authorization: algorithm Credential=accessKeyID/credScope, \
//            SignedHeaders=signedHeaders, Signature=signature
//
func parseSignV4(v4Auth string) (signValues, error) {
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.Replace(v4Auth, " ", "", -1)
	if v4Auth == "" {
		return signValues{}, eAuthHeaderEmpty()
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v4Auth, signV4Algorithm) {
		return signValues{}, eSignatureNotSupported()
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return signValues{}, eMissingFields()
	}

	// Initialize signature version '4' structured header.
	signV4Values := signValues{}

	var err error
	// Save credentail values.
	signV4Values.Credential, err = parseCredentialHeader(authFields[0])
	if err != nil {
		return signValues{}, err
	}

	// Save signed headers.
	signV4Values.SignedHeaders, err = parseSignedHeader(authFields[1])
	if err != nil {
		return signValues{}, err
	}

	// Save signature.
	signV4Values.Signature, err = parseSignature(authFields[2])
	if err != nil {
		return signValues{}, err
	}

	// Return the structure here.
	return signV4Values, nil
}
