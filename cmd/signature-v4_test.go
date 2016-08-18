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

package cmd

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func niceError(code APIErrorCode) string {
	// Special-handle ErrNone
	if code == ErrNone {
		return "ErrNone"
	}

	return fmt.Sprintf("%s (%s)", errorCodeResponse[code].Code, errorCodeResponse[code].Description)
}

func TestDoesPolicySignatureMatch(t *testing.T) {
	credentialTemplate := "%s/%s/%s/s3/aws4_request"
	now := time.Now().UTC()
	accessKey := serverConfig.GetCredential().AccessKeyID

	testCases := []struct {
		form     map[string]string
		expected APIErrorCode
	}{
		// (0) It should fail if 'X-Amz-Credential' is missing.
		{
			form:     map[string]string{},
			expected: ErrMissingFields,
		},
		// (1) It should fail if the access key is incorrect.
		{
			form: map[string]string{
				"X-Amz-Credential": fmt.Sprintf(credentialTemplate, "EXAMPLEINVALIDEXAMPL", now.Format(yyyymmdd), "us-east-1"),
			},
			expected: ErrInvalidAccessKeyID,
		},
		// (2) It should fail if the region is invalid.
		{
			form: map[string]string{
				"X-Amz-Credential": fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), "invalidregion"),
			},
			expected: ErrInvalidRegion,
		},
		// (3) It should fail if the date is invalid (or missing, in this case).
		{
			form: map[string]string{
				"X-Amz-Credential": fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), "us-east-1"),
			},
			expected: ErrMalformedDate,
		},
		// (4) It should fail with a bad signature.
		{
			form: map[string]string{
				"X-Amz-Credential": fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), "us-east-1"),
				"X-Amz-Date":       now.Format(iso8601Format),
				"X-Amz-Signature":  "invalidsignature",
				"Policy":           "policy",
			},
			expected: ErrSignatureDoesNotMatch,
		},
		// (5) It should succeed if everything is correct.
		{
			form: map[string]string{
				"X-Amz-Credential": fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), "us-east-1"),
				"X-Amz-Date":       now.Format(iso8601Format),
				"X-Amz-Signature":  getSignature(getSigningKey(serverConfig.GetCredential().SecretAccessKey, now, "us-east-1"), "policy"),
				"Policy":           "policy",
			},
			expected: ErrNone,
		},
	}

	// Run each test case individually.
	for i, testCase := range testCases {
		code := doesPolicySignatureMatch(testCase.form)
		if code != testCase.expected {
			t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(code))
		}
	}
}

func TestDoesPresignedSignatureMatch(t *testing.T) {
	// sha256 hash of "payload"
	payload := "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
	now := time.Now().UTC()
	credentialTemplate := "%s/%s/%s/s3/aws4_request"

	testCases := []struct {
		queryParams  map[string]string
		headers      map[string]string
		verifyRegion bool
		expected     APIErrorCode
	}{
		// (0) Should error without a set URL query.
		{
			verifyRegion: false,
			expected:     ErrInvalidQueryParams,
		},
		// (1) Should error on an invalid access key.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":     signV4Algorithm,
				"X-Amz-Date":          now.Format(iso8601Format),
				"X-Amz-Expires":       "60",
				"X-Amz-Signature":     "badsignature",
				"X-Amz-SignedHeaders": "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":    fmt.Sprintf(credentialTemplate, "Z7IXGOO6BZ0REAN1Q26I", now.Format(yyyymmdd), "us-west-1"),
			},
			verifyRegion: false,
			expected:     ErrInvalidAccessKeyID,
		},
		// (2) Should error when the payload sha256 doesn't match.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), "us-west-1"),
				"X-Amz-Content-Sha256": "ThisIsNotThePayloadHash",
			},
			verifyRegion: false,
			expected:     ErrContentSHA256Mismatch,
		},
		// (3) Should fail with an invalid region.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), "us-west-1"),
				"X-Amz-Content-Sha256": payload,
			},
			verifyRegion: true,
			expected:     ErrInvalidRegion,
		},
		// (4) Should NOT fail with an invalid region if it doesn't verify it.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), "us-west-1"),
				"X-Amz-Content-Sha256": payload,
			},
			verifyRegion: false,
			expected:     ErrUnsignedHeaders,
		},
		// (5) Should fail to extract headers if the host header is not signed.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), serverConfig.GetRegion()),
				"X-Amz-Content-Sha256": payload,
			},
			verifyRegion: true,
			expected:     ErrUnsignedHeaders,
		},
		// (6) Should give an expired request if it has expired.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.AddDate(0, 0, -2).Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), serverConfig.GetRegion()),
				"X-Amz-Content-Sha256": payload,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.AddDate(0, 0, -2).Format(iso8601Format),
				"X-Amz-Content-Sha256": payload,
			},
			verifyRegion: false,
			expected:     ErrExpiredPresignRequest,
		},
		// (7) Should error if the signature is incorrect.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, serverConfig.GetCredential().AccessKeyID, now.Format(yyyymmdd), serverConfig.GetRegion()),
				"X-Amz-Content-Sha256": payload,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Content-Sha256": payload,
			},
			verifyRegion: false,
			expected:     ErrSignatureDoesNotMatch,
		},
	}

	// Run each test case individually.
	for i, testCase := range testCases {
		// Turn the map[string]string into map[string][]string, because Go.
		query := url.Values{}
		for key, value := range testCase.queryParams {
			query.Set(key, value)
		}

		// Create a request to use.
		req, e := http.NewRequest(http.MethodGet, "http://host/a/b?"+query.Encode(), nil)
		if e != nil {
			t.Errorf("(%d) failed to create http.Request, got %v", i, e)
		}

		// Do the same for the headers.
		for key, value := range testCase.headers {
			req.Header.Set(key, value)
		}

		// Check if it matches!
		err := doesPresignedSignatureMatch(payload, req, testCase.verifyRegion)
		if err != testCase.expected {
			t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(err))
		}
	}
}
