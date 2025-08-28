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
	"sort"
	"testing"
)

// Tests for 'func TestResourceListSorting(t *testing.T)'.
func TestResourceListSorting(t *testing.T) {
	sortedResourceList := make([]string, len(resourceList))
	copy(sortedResourceList, resourceList)
	sort.Strings(sortedResourceList)
	for i := range resourceList {
		if resourceList[i] != sortedResourceList[i] {
			t.Errorf("Expected resourceList[%d] = \"%s\", resourceList is not correctly sorted.", i, sortedResourceList[i])
			break
		}
	}
}

// Tests presigned v2 signature.
func TestDoesPresignedV2SignatureMatch(t *testing.T) {
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

	now := UTCNow()

	var (
		accessKey = globalActiveCred.AccessKey
		secretKey = globalActiveCred.SecretKey
	)
	testCases := []struct {
		queryParams map[string]string
		expected    APIErrorCode
	}{
		// (0) Should error without a set URL query.
		{
			expected: ErrInvalidQueryParams,
		},
		// (1) Should error on an invalid access key.
		{
			queryParams: map[string]string{
				"Expires":        "60",
				"Signature":      "badsignature",
				"AWSAccessKeyId": "Z7IXGOO6BZ0REAN1Q26I",
			},
			expected: ErrInvalidAccessKeyID,
		},
		// (2) Should error with malformed expires.
		{
			queryParams: map[string]string{
				"Expires":        "60s",
				"Signature":      "badsignature",
				"AWSAccessKeyId": accessKey,
			},
			expected: ErrMalformedExpires,
		},
		// (3) Should give an expired request if it has expired.
		{
			queryParams: map[string]string{
				"Expires":        "60",
				"Signature":      "badsignature",
				"AWSAccessKeyId": accessKey,
			},
			expected: ErrExpiredPresignRequest,
		},
		// (4) Should error when the signature does not match.
		{
			queryParams: map[string]string{
				"Expires":        fmt.Sprintf("%d", now.Unix()+60),
				"Signature":      "badsignature",
				"AWSAccessKeyId": accessKey,
			},
			expected: ErrSignatureDoesNotMatch,
		},
		// (5) Should error when the signature does not match.
		{
			queryParams: map[string]string{
				"Expires":        fmt.Sprintf("%d", now.Unix()+60),
				"Signature":      "zOM2YrY/yAQe15VWmT78OlBrK6g=",
				"AWSAccessKeyId": accessKey,
			},
			expected: ErrSignatureDoesNotMatch,
		},
		// (6) Should not error signature matches with extra query params.
		{
			queryParams: map[string]string{
				"response-content-disposition": "attachment; filename=\"4K%2d4M.txt\"",
			},
			expected: ErrNone,
		},
		// (7) Should not error signature matches with no special query params.
		{
			queryParams: map[string]string{},
			expected:    ErrNone,
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
		req, err := http.NewRequest(http.MethodGet, "http://host/a/b?"+query.Encode(), nil)
		if err != nil {
			t.Errorf("(%d) failed to create http.Request, got %v", i, err)
		}
		if testCase.expected != ErrNone {
			// Should be set since we are simulating a http server.
			req.RequestURI = req.URL.RequestURI()
			// Check if it matches!
			errCode := doesPresignV2SignatureMatch(req)
			if errCode != testCase.expected {
				t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(errCode))
			}
		} else {
			err = preSignV2(req, accessKey, secretKey, now.Unix()+60)
			if err != nil {
				t.Fatalf("(%d) failed to preSignV2 http request, got %v", i, err)
			}
			// Should be set since we are simulating a http server.
			req.RequestURI = req.URL.RequestURI()
			errCode := doesPresignV2SignatureMatch(req)
			if errCode != testCase.expected {
				t.Errorf("(%d) expected to get success, instead got %s", i, niceError(errCode))
			}
		}
	}
}

// TestValidateV2AuthHeader - Tests validate the logic of V2 Authorization header validator.
func TestValidateV2AuthHeader(t *testing.T) {
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

	accessID := globalActiveCred.AccessKey
	testCases := []struct {
		authString    string
		expectedError APIErrorCode
	}{
		// Test case - 1.
		// Case with empty V2AuthString.
		{
			authString:    "",
			expectedError: ErrAuthHeaderEmpty,
		},
		// Test case - 2.
		// Test case with `signV2Algorithm` ("AWS") not being the prefix.
		{
			authString:    "NoV2Prefix",
			expectedError: ErrSignatureVersionNotSupported,
		},
		// Test case - 3.
		// Test case with missing parts in the Auth string.
		// below is the correct format of V2 Authorization header.
		// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature
		{
			authString:    signV2Algorithm,
			expectedError: ErrMissingFields,
		},
		// Test case - 4.
		// Test case with signature part missing.
		{
			authString:    fmt.Sprintf("%s %s", signV2Algorithm, accessID),
			expectedError: ErrMissingFields,
		},
		// Test case - 5.
		// Test case with wrong accessID.
		{
			authString:    fmt.Sprintf("%s %s:%s", signV2Algorithm, "InvalidAccessID", "signature"),
			expectedError: ErrInvalidAccessKeyID,
		},
		// Test case - 6.
		// Case with right accessID and format.
		{
			authString:    fmt.Sprintf("%s %s:%s", signV2Algorithm, accessID, "signature"),
			expectedError: ErrNone,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Case %d AuthStr \"%s\".", i+1, testCase.authString), func(t *testing.T) {
			req := &http.Request{
				Header: make(http.Header),
				URL:    &url.URL{},
			}
			req.Header.Set("Authorization", testCase.authString)
			_, actualErrCode := validateV2AuthHeader(req)

			if testCase.expectedError != actualErrCode {
				t.Errorf("Expected the error code to be %v, got %v.", testCase.expectedError, actualErrCode)
			}
		})
	}
}

func TestDoesPolicySignatureV2Match(t *testing.T) {
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

	creds := globalActiveCred
	policy := "policy"
	testCases := []struct {
		accessKey string
		policy    string
		signature string
		errCode   APIErrorCode
	}{
		{"invalidAccessKey", policy, calculateSignatureV2(policy, creds.SecretKey), ErrInvalidAccessKeyID},
		{creds.AccessKey, policy, calculateSignatureV2("random", creds.SecretKey), ErrSignatureDoesNotMatch},
		{creds.AccessKey, policy, calculateSignatureV2(policy, creds.SecretKey), ErrNone},
	}
	for i, test := range testCases {
		formValues := make(http.Header)
		formValues.Set("Awsaccesskeyid", test.accessKey)
		formValues.Set("Signature", test.signature)
		formValues.Set("Policy", test.policy)
		_, errCode := doesPolicySignatureV2Match(formValues)
		if errCode != test.errCode {
			t.Fatalf("(%d) expected to get %s, instead got %s", i+1, niceError(test.errCode), niceError(errCode))
		}
	}
}
