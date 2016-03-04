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

package signature4

import (
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio/pkg/probe"
)

// credential data type represents structured form of Credential
// string from authorization header.
type credential struct {
	accessKeyID string
	scope       struct {
		date    time.Time
		region  string
		service string
		request string
	}
}

// parse credential string into its structured form.
func parseCredential(credElement string) (credential, *probe.Error) {
	creds := strings.Split(strings.TrimSpace(credElement), "=")
	if len(creds) != 2 {
		return credential{}, ErrMissingFields("Credential tag has missing fields.", credElement).Trace(credElement)
	}
	if creds[0] != "Credential" {
		return credential{}, ErrMissingCredTag("Missing credentials tag.", credElement).Trace(credElement)
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), "/")
	if len(credElements) != 5 {
		return credential{}, ErrCredMalformed("Credential values malformed.", credElement).Trace(credElement)
	}
	// Access key id cannot be empty.
	accessKeyID := credElements[0]
	if accessKeyID == "" {
		return credential{}, ErrInvalidAccessKeyID("Invalid access key id.", credElement).Trace(credElement)
	}
	cred := credential{
		accessKeyID: credElements[0],
	}
	var e error
	cred.scope.date, e = time.Parse(yyyymmdd, credElements[1])
	if e != nil {
		return credential{}, ErrInvalidDateFormat("Invalid date format.", credElement).Trace(credElement)
	}
	if credElements[2] == "" {
		return credential{}, ErrRegionISEmpty("Region is empty.", credElement).Trace(credElement)
	}
	cred.scope.region = credElements[2]
	if credElements[3] != "s3" {
		return credential{}, ErrInvalidService("Invalid service detected.", credElement).Trace(credElement)
	}
	cred.scope.service = credElements[3]
	if credElements[4] != "aws4_request" {
		return credential{}, ErrInvalidRequestVersion("Invalid request version detected.", credElement).Trace(credElement)
	}
	cred.scope.request = credElements[4]
	return cred, nil
}

// Parse signature string.
func parseSignature(signElement string) (string, *probe.Error) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if len(signFields) != 2 {
		return "", ErrMissingFields("Signature tag has missing fields.", signElement).Trace(signElement)
	}
	if signFields[0] != "Signature" {
		return "", ErrMissingSignTag("Signature tag is missing", signElement).Trace(signElement)
	}
	signature := signFields[1]
	return signature, nil
}

// Parse signed headers string.
func parseSignedHeaders(signedHdrElement string) ([]string, *probe.Error) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if len(signedHdrFields) != 2 {
		return nil, ErrMissingFields("Signed headers tag has missing fields.", signedHdrElement).Trace(signedHdrElement)
	}
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, ErrMissingSignHeadersTag("Signed headers tag is missing.", signedHdrElement).Trace(signedHdrElement)
	}
	signedHeaders := strings.Split(signedHdrFields[1], ";")
	return signedHeaders, nil
}

// signValues data type represents structured form of AWS Signature V4 header.
type signValues struct {
	Credential    credential
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
//   querystring += &X-Amz-Credential= urlencode(access_key_ID + '/' + credential_scope)
//   querystring += &X-Amz-Date=date
//   querystring += &X-Amz-Expires=timeout interval
//   querystring += &X-Amz-SignedHeaders=signed_headers
//   querystring += &X-Amz-Signature=signature
//
func parsePreSignV4(query url.Values) (preSignValues, *probe.Error) {
	// Verify if the query algorithm is supported or not.
	if query.Get("X-Amz-Algorithm") != signV4Algorithm {
		return preSignValues{}, ErrUnsuppSignAlgo("Unsupported algorithm in query string.", query.Get("X-Amz-Algorithm"))
	}

	// Initialize signature version '4' structured header.
	preSignV4Values := preSignValues{}

	var err *probe.Error
	// Save credential.
	preSignV4Values.Credential, err = parseCredential("Credential=" + query.Get("X-Amz-Credential"))
	if err != nil {
		return preSignValues{}, err.Trace(query.Get("X-Amz-Credential"))
	}

	var e error
	// Save date in native time.Time.
	preSignV4Values.Date, e = time.Parse(iso8601Format, query.Get("X-Amz-Date"))
	if e != nil {
		return preSignValues{}, ErrMalformedDate("Malformed date string.", query.Get("X-Amz-Date")).Trace(query.Get("X-Amz-Date"))
	}

	// Save expires in native time.Duration.
	preSignV4Values.Expires, e = time.ParseDuration(query.Get("X-Amz-Expires") + "s")
	if e != nil {
		return preSignValues{}, ErrMalformedExpires("Malformed expires string.", query.Get("X-Amz-Expires")).Trace(query.Get("X-Amz-Expires"))
	}

	// Save signed headers.
	preSignV4Values.SignedHeaders, err = parseSignedHeaders("SignedHeaders=" + query.Get("X-Amz-SignedHeaders"))
	if err != nil {
		return preSignValues{}, err.Trace(query.Get("X-Amz-SignedHeaders"))
	}

	// Save signature.
	preSignV4Values.Signature, err = parseSignature("Signature=" + query.Get("X-Amz-Signature"))
	if err != nil {
		return preSignValues{}, err.Trace(query.Get("X-Amz-Signature"))
	}

	// Return structed form of signature query string.
	return preSignV4Values, nil
}

// Parses signature version '4' header of the following form.
//
//    Authorization: algorithm Credential=access key ID/credential scope, \
//            SignedHeaders=SignedHeaders, Signature=signature
//
func parseSignV4(v4Auth string) (signValues, *probe.Error) {
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.Replace(v4Auth, " ", "", -1)
	if v4Auth == "" {
		return signValues{}, ErrAuthHeaderEmpty("Auth header empty.").Trace(v4Auth)
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v4Auth, signV4Algorithm) {
		return signValues{}, ErrUnsuppSignAlgo("Unsupported algorithm in authorization header.", v4Auth).Trace(v4Auth)
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return signValues{}, ErrMissingFields("Missing fields in authorization header.", v4Auth).Trace(v4Auth)
	}

	// Initialize signature version '4' structured header.
	signV4Values := signValues{}

	var err *probe.Error
	// Save credentail values.
	signV4Values.Credential, err = parseCredential(authFields[0])
	if err != nil {
		return signValues{}, err.Trace(v4Auth)
	}

	// Save signed headers.
	signV4Values.SignedHeaders, err = parseSignedHeaders(authFields[1])
	if err != nil {
		return signValues{}, err.Trace(v4Auth)
	}

	// Save signature.
	signV4Values.Signature, err = parseSignature(authFields[2])
	if err != nil {
		return signValues{}, err.Trace(v4Auth)
	}

	// Return the structure here.
	return signV4Values, nil
}
