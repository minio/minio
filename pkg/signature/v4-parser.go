package signature

import (
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio/pkg/probe"
)

type credScope struct {
	accessKeyID string
	scope       struct {
		date    time.Time
		region  string
		service string
		request string
	}
}

func parseCredential(credElement string) (credScope, *probe.Error) {
	creds := strings.Split(strings.TrimSpace(credElement), "=")
	if len(creds) != 2 {
		return credScope{}, ErrMissingFields("Credential tag has missing fields.", credElement).Trace(credElement)
	}
	if creds[0] != "Credential" {
		return credScope{}, ErrMissingCredTag("Missing credentials tag.", credElement).Trace(credElement)
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), "/")
	if len(credElements) != 5 {
		return credScope{}, ErrCredMalformed("Credential values malformed.", credElement).Trace(credElement)
	}
	if !isValidAccessKey.MatchString(credElements[0]) {
		return credScope{}, ErrInvalidAccessKeyID("Invalid access key id.", credElement).Trace(credElement)
	}
	cred := credScope{
		accessKeyID: credElements[0],
	}
	var e error
	cred.scope.date, e = time.Parse(yyyymmdd, credElements[1])
	if e != nil {
		return credScope{}, ErrInvalidDateFormat("Invalid date format.", credElement).Trace(credElement)
	}
	if credElements[2] == "" {
		return credScope{}, ErrRegionISEmpty("Region is empty.", credElement).Trace(credElement)
	}
	cred.scope.region = credElements[2]
	if credElements[3] != "s3" {
		return credScope{}, ErrInvalidService("Invalid service detected.", credElement).Trace(credElement)
	}
	cred.scope.service = credElements[3]
	if credElements[4] != "aws4_request" {
		return credScope{}, ErrInvalidRequestVersion("Invalid request version detected.", credElement).Trace(credElement)
	}
	cred.scope.request = credElements[4]
	return cred, nil
}

// parse signature.
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

// parse signed headers.
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

// structured version of AWS Signature V4 header.
type signValues struct {
	Creds         credScope
	SignedHeaders []string
	Signature     string
}

// structued version of AWS Signature V4 query string.
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
	// Save credentail values.
	preSignV4Values.Creds, err = parseCredential(query.Get("X-Amz-Credential"))
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
	preSignV4Values.SignedHeaders, err = parseSignedHeaders(query.Get("X-Amz-SignedHeaders"))
	if err != nil {
		return preSignValues{}, err.Trace(query.Get("X-Amz-SignedHeaders"))
	}

	// Save signature.
	preSignV4Values.Signature, err = parseSignature(query.Get("X-Amz-Signature"))
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
	signV4Values.Creds, err = parseCredential(authFields[0])
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
