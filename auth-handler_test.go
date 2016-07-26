/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

// Provides a fully populated http request instance, fails otherwise.
func mustNewRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		t.Fatalf("Unable to initialize new http request %s", err)
	}
	return req
}

// This is similar to mustNewRequest but additionally the request
// is signed with AWS Signature V4, fails if not able to do so.
func mustNewSignedRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req := mustNewRequest(method, urlStr, contentLength, body, t)
	cred := serverConfig.GetCredential()
	if err := signRequest(req, cred.AccessKeyID, cred.SecretAccessKey); err != nil {
		t.Fatalf("Unable to inititalized new signed http request %s", err)
	}
	return req
}

// Tests is requested authenticated function, tests replies for s3 errors.
func TestIsReqAuthenticated(t *testing.T) {
	path, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(path)

	serverConfig.SetCredential(credential{"myuser", "mypassword"})

	// List of test cases for validating http request authentication.
	testCases := []struct {
		req     *http.Request
		s3Error APIErrorCode
	}{
		// When request is nil, internal error is returned.
		{nil, ErrInternalError},
		// When request is unsigned, access denied is returned.
		{mustNewRequest("GET", "http://localhost:9000", 0, nil, t), ErrAccessDenied},
		// When request is properly signed, but has bad Content-MD5 header.
		{mustNewSignedRequest("PUT", "http://localhost:9000", 5, bytes.NewReader([]byte("hello")), t), ErrBadDigest},
		// When request is properly signed, error is none.
		{mustNewSignedRequest("GET", "http://localhost:9000", 0, nil, t), ErrNone},
	}

	// Validates all testcases.
	for _, testCase := range testCases {
		if testCase.s3Error == ErrBadDigest {
			testCase.req.Header.Set("Content-Md5", "garbage")
		}
		if s3Error := isReqAuthenticated(testCase.req); s3Error != testCase.s3Error {
			t.Fatalf("Unexpected s3error returned wanted %d, got %d", testCase.s3Error, s3Error)
		}
	}
}
