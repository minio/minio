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
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func niceError(code APIErrorCode) string {
	// Special-handle ErrNone
	if code == ErrNone {
		return "ErrNone"
	}

	return fmt.Sprintf("%s (%s)", errorCodes[code].Code, errorCodes[code].Description)
}

func TestDoesPolicySignatureMatch(t *testing.T) {
	_, fsDir, err := prepareFS(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots([]string{fsDir})

	credentialTemplate := "%s/%s/%s/s3/aws4_request"
	now := UTCNow()
	accessKey := globalActiveCred.AccessKey

	testCases := []struct {
		form     http.Header
		expected APIErrorCode
	}{
		// (0) It should fail if 'X-Amz-Credential' is missing.
		{
			form:     http.Header{},
			expected: ErrCredMalformed,
		},
		// (1) It should fail if the access key is incorrect.
		{
			form: http.Header{
				"X-Amz-Credential": []string{fmt.Sprintf(credentialTemplate, "EXAMPLEINVALIDEXAMPL", now.Format(yyyymmdd), globalMinioDefaultRegion)},
			},
			expected: ErrInvalidAccessKeyID,
		},
		// (2) It should fail with a bad signature.
		{
			form: http.Header{
				"X-Amz-Credential": []string{fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), globalMinioDefaultRegion)},
				"X-Amz-Date":       []string{now.Format(iso8601Format)},
				"X-Amz-Signature":  []string{"invalidsignature"},
				"Policy":           []string{"policy"},
			},
			expected: ErrSignatureDoesNotMatch,
		},
		// (3) It should succeed if everything is correct.
		{
			form: http.Header{
				"X-Amz-Credential": []string{
					fmt.Sprintf(credentialTemplate, accessKey, now.Format(yyyymmdd), globalMinioDefaultRegion),
				},
				"X-Amz-Date": []string{now.Format(iso8601Format)},
				"X-Amz-Signature": []string{
					getSignature(getSigningKey(globalActiveCred.SecretKey, now,
						globalMinioDefaultRegion, serviceS3), "policy"),
				},
				"Policy": []string{"policy"},
			},
			expected: ErrNone,
		},
	}

	// Run each test case individually.
	for i, testCase := range testCases {
		_, code := doesPolicySignatureMatch(testCase.form)
		if code != testCase.expected {
			t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(code))
		}
	}
}

func TestDoesPresignedSignatureMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	obj, fsDir, err := prepareFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}

	// sha256 hash of "payload"
	payloadSHA256 := "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
	now := UTCNow()
	credentialTemplate := "%s/%s/%s/s3/aws4_request"

	region := globalSite.Region()
	accessKeyID := globalActiveCred.AccessKey
	testCases := []struct {
		queryParams map[string]string
		headers     map[string]string
		region      string
		expected    APIErrorCode
	}{
		// (0) Should error without a set URL query.
		{
			region:   globalMinioDefaultRegion,
			expected: ErrInvalidQueryParams,
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
			region:   "us-west-1",
			expected: ErrInvalidAccessKeyID,
		},
		// (2) Should NOT fail with an invalid region if it doesn't verify it.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), "us-west-1"),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   "us-west-1",
			expected: ErrUnsignedHeaders,
		},
		// (3) Should fail to extract headers if the host header is not signed.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   region,
			expected: ErrUnsignedHeaders,
		},
		// (4) Should give an expired request if it has expired.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.AddDate(0, 0, -2).Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.AddDate(0, 0, -2).Format(iso8601Format),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   region,
			expected: ErrExpiredPresignRequest,
		},
		// (5) Should error if the signature is incorrect.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   region,
			expected: ErrSignatureDoesNotMatch,
		},
		// (6) Should error if the request is not ready yet, ie X-Amz-Date is in the future.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Add(1 * time.Hour).Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   region,
			expected: ErrRequestNotReadyYet,
		},
		// (7) Should not error with invalid region instead, call should proceed
		// with signature does not match.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":      signV4Algorithm,
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Expires":        "60",
				"X-Amz-Signature":      "badsignature",
				"X-Amz-SignedHeaders":  "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":     fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			headers: map[string]string{
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   "",
			expected: ErrSignatureDoesNotMatch,
		},
		// (8) Should error with signature does not match. But handles
		// query params which do not precede with "x-amz-" header.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":       signV4Algorithm,
				"X-Amz-Date":            now.Format(iso8601Format),
				"X-Amz-Expires":         "60",
				"X-Amz-Signature":       "badsignature",
				"X-Amz-SignedHeaders":   "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":      fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256":  payloadSHA256,
				"response-content-type": "application/json",
			},
			headers: map[string]string{
				"X-Amz-Date":           now.Format(iso8601Format),
				"X-Amz-Content-Sha256": payloadSHA256,
			},
			region:   "",
			expected: ErrSignatureDoesNotMatch,
		},
		// (9) Should error with unsigned headers.
		{
			queryParams: map[string]string{
				"X-Amz-Algorithm":       signV4Algorithm,
				"X-Amz-Date":            now.Format(iso8601Format),
				"X-Amz-Expires":         "60",
				"X-Amz-Signature":       "badsignature",
				"X-Amz-SignedHeaders":   "host;x-amz-content-sha256;x-amz-date",
				"X-Amz-Credential":      fmt.Sprintf(credentialTemplate, accessKeyID, now.Format(yyyymmdd), region),
				"X-Amz-Content-Sha256":  payloadSHA256,
				"response-content-type": "application/json",
			},
			headers: map[string]string{
				"X-Amz-Date": now.Format(iso8601Format),
			},
			region:   "",
			expected: ErrUnsignedHeaders,
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

		// parse form.
		req.ParseForm()

		// Check if it matches!
		err := doesPresignedSignatureMatch(payloadSHA256, req, testCase.region, serviceS3)
		if err != testCase.expected {
			t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(err))
		}
	}
}
