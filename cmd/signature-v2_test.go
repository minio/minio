package cmd

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"
)

// Tests for 'func TestResourceListSorting(t *testing.T)'.
func TestResourceListSorting(t *testing.T) {
	sortedResourceList := make([]string, len(resourceList))
	copy(sortedResourceList, resourceList)
	sort.Strings(sortedResourceList)
	for i := 0; i < len(resourceList); i++ {
		if resourceList[i] != sortedResourceList[i] {
			t.Errorf("Expected resourceList[%d] = \"%s\", resourceList is not correctly sorted.", i, sortedResourceList[i])
			break
		}
	}
}

func TestDoesPresignedV2SignatureMatch(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Unable to initialize test config.")
	}
	defer removeAll(root)

	now := time.Now().UTC()

	testCases := []struct {
		queryParams map[string]string
		headers     map[string]string
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
				"AWSAccessKeyId": serverConfig.GetCredential().AccessKeyID,
			},
			expected: ErrMalformedExpires,
		},
		// (3) Should give an expired request if it has expired.
		{
			queryParams: map[string]string{
				"Expires":        "60",
				"Signature":      "badsignature",
				"AWSAccessKeyId": serverConfig.GetCredential().AccessKeyID,
			},
			expected: ErrExpiredPresignRequest,
		},
		// (4) Should error when the signature does not match.
		{
			queryParams: map[string]string{
				"Expires":        fmt.Sprintf("%d", now.Unix()+60),
				"Signature":      "badsignature",
				"AWSAccessKeyId": serverConfig.GetCredential().AccessKeyID,
			},
			expected: ErrSignatureDoesNotMatch,
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
		err := doesPresignV2SignatureMatch(req)
		if err != testCase.expected {
			t.Errorf("(%d) expected to get %s, instead got %s", i, niceError(testCase.expected), niceError(err))
		}
	}
}
