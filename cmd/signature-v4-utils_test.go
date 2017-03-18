/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"net/http"
	"testing"
)

// TestSkipContentSha256Cksum - Test validate the logic which decides whether
// to skip checksum validation based on the request header.
func TestSkipContentSha256Cksum(t *testing.T) {
	testCases := []struct {
		inputHeaderKey   string
		inputHeaderValue string

		inputQueryKey   string
		inputQueryValue string

		expectedResult bool
	}{
		// Test case - 1.
		// Test case with "X-Amz-Content-Sha256" header set to empty value.
		{"X-Amz-Content-Sha256", "", "", "", false},
		// Test case - 2.
		// Test case with "X-Amz-Content-Sha256" header set to  "UNSIGNED-PAYLOAD"
		// When "X-Amz-Content-Sha256" header is set to  "UNSIGNED-PAYLOAD", validation of content sha256 has to be skipped.
		{"X-Amz-Content-Sha256", unsignedPayload, "", "", true},
		// Test case - 3.
		// Enabling PreSigned Signature v4.
		{"", "", "X-Amz-Credential", "", true},
		// Test case - 4.
		// "X-Amz-Content-Sha256" not set and  PreSigned Signature v4 not enabled, sha256 checksum calculation is not skipped.
		{"", "", "X-Amz-Credential", "", true},
	}

	for i, testCase := range testCases {
		// creating an input HTTP request.
		// Only the headers are relevant for this particular test.
		inputReq, err := http.NewRequest("GET", "http://example.com", nil)
		if err != nil {
			t.Fatalf("Error initializing input HTTP request: %v", err)
		}
		if testCase.inputHeaderKey != "" {
			inputReq.Header.Set(testCase.inputHeaderKey, testCase.inputHeaderValue)
		}
		if testCase.inputQueryKey != "" {
			q := inputReq.URL.Query()
			q.Add(testCase.inputQueryKey, testCase.inputQueryValue)
			inputReq.URL.RawQuery = q.Encode()
		}

		actualResult := skipContentSha256Cksum(inputReq)
		if testCase.expectedResult != actualResult {
			t.Errorf("Test %d: Expected the result to `%v`, but instead got `%v`", i+1, testCase.expectedResult, actualResult)
		}
	}
}

// TestIsValidRegion - Tests validate the comparison logic for asserting whether the region from http request is valid.
func TestIsValidRegion(t *testing.T) {
	testCases := []struct {
		inputReqRegion  string
		inputConfRegion string

		expectedResult bool
	}{

		{"", "", false},
		{globalMinioDefaultRegion, "", true},
		{globalMinioDefaultRegion, "US", true},
		{"us-west-1", "US", false},
		{"us-west-1", "us-west-1", true},
		// "US" was old naming convention for 'us-east-1'.
		{"US", "US", true},
	}

	for i, testCase := range testCases {
		actualResult := isValidRegion(testCase.inputReqRegion, testCase.inputConfRegion)
		if testCase.expectedResult != actualResult {
			t.Errorf("Test %d: Expected the result to `%v`, but instead got `%v`", i+1, testCase.expectedResult, actualResult)
		}
	}
}

// Tests validate the URL path encoder.
func TestGetURLEncodedName(t *testing.T) {
	testCases := []struct {
		// Input.
		inputStr string
		// Expected result.
		result string
	}{
		// % should be encoded as %25
		{"thisisthe%url", "thisisthe%25url"},
		// UTF-8 encoding.
		{"本語", "%E6%9C%AC%E8%AA%9E"},
		// UTF-8 encoding with ASCII.
		{"本語.1", "%E6%9C%AC%E8%AA%9E.1"},
		// Unusual ASCII characters.
		{">123", "%3E123"},
		// Fragment path characters.
		{"myurl#link", "myurl%23link"},
		// Space should be set to %20 not '+'.
		{"space in url", "space%20in%20url"},
		// '+' shouldn't be treated as space.
		{"url+path", "url%2Bpath"},
	}

	// Tests generated values from url encoded name.
	for i, testCase := range testCases {
		result := getURLEncodedName(testCase.inputStr)
		if testCase.result != result {
			t.Errorf("Test %d: Expected URLEncoded result to be \"%s\", but found it to be \"%s\" instead", i+1, testCase.result, result)
		}
	}
}

// TestExtractSignedHeaders - Tests validate extraction of signed headers using list of signed header keys.
func TestExtractSignedHeaders(t *testing.T) {
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}

	// If the `expect` key exists in the signed headers then golang server would have stripped out the value, expecting the `expect` header set to `100-continue` in the result.
	signedHeaders = append(signedHeaders, "expect")
	// expected header values.
	expectedHost := "play.minio.io:9000"
	expectedContentSha256 := "1234abcd"
	expectedTime := UTCNow().Format(iso8601Format)

	// Creating input http header.
	inputHeader := make(http.Header)
	inputHeader.Set(signedHeaders[0], expectedHost)
	inputHeader.Set(signedHeaders[1], expectedContentSha256)
	inputHeader.Set(signedHeaders[2], expectedTime)
	// calling the function being tested.
	extractedSignedHeaders, errCode := extractSignedHeaders(signedHeaders, inputHeader)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}

	// "x-amz-content-sha256" header value from the extracted result.
	extractedContentSha256 := extractedSignedHeaders[signedHeaders[1]]
	// "host" header value from the extracted result.
	extractedHost := extractedSignedHeaders[signedHeaders[0]]
	//  "x-amz-date" header from the extracted result.
	extractedDate := extractedSignedHeaders[signedHeaders[2]]
	// extracted `expect` header.
	extractedExpect := extractedSignedHeaders["expect"][0]
	if expectedHost != extractedHost[0] {
		t.Errorf("host header mismatch: expected `%s`, got `%s`", expectedHost, extractedHost)
	}
	// assert the result with the expected value.
	if expectedContentSha256 != extractedContentSha256[0] {
		t.Errorf("x-amz-content-sha256 header mismatch: expected `%s`, got `%s`", expectedContentSha256, extractedContentSha256)
	}
	if expectedTime != extractedDate[0] {
		t.Errorf("x-amz-date header mismatch: expected `%s`, got `%s`", expectedTime, extractedDate)
	}

	// Since the list of signed headers value contained `expect`, the default value of `100-continue` will be added to extracted signed headers.
	if extractedExpect != "100-continue" {
		t.Errorf("expect header incorrect value: expected `%s`, got `%s`", "100-continue", extractedExpect)
	}

	// case where the headers doesn't contain the one of the signed header in the signed headers list.
	signedHeaders = append(signedHeaders, " X-Amz-Credential")
	// expected to fail with `ErrUnsignedHeaders`.
	_, errCode = extractSignedHeaders(signedHeaders, inputHeader)
	if errCode != ErrUnsignedHeaders {
		t.Fatalf("Expected the APIErrorCode to %d, but got %d", ErrUnsignedHeaders, errCode)
	}

	// case where the list of signed headers doesn't contain the host field.
	signedHeaders = signedHeaders[1:]
	// expected to fail with `ErrUnsignedHeaders`.
	_, errCode = extractSignedHeaders(signedHeaders, inputHeader)
	if errCode != ErrUnsignedHeaders {
		t.Fatalf("Expected the APIErrorCode to %d, but got %d", ErrUnsignedHeaders, errCode)
	}
}

// TestSignV4TrimAll - tests the logic of TrimAll() function
func TestSignV4TrimAll(t *testing.T) {
	testCases := []struct {
		// Input.
		inputStr string
		// Expected result.
		result string
	}{
		{"本語", "本語"},
		{" abc ", "abc"},
		{" a b ", "a b"},
		{"a b ", "a b"},
		{"a  b", "a b"},
		{"a   b", "a b"},
		{"   a   b  c   ", "a b c"},
		{"a \t b  c   ", "a b c"},
		{"\"a \t b  c   ", "\"a b c"},
		{" \t\n\u000b\r\fa \t\n\u000b\r\f b \t\n\u000b\r\f c \t\n\u000b\r\f", "a b c"},
	}

	// Tests generated values from url encoded name.
	for i, testCase := range testCases {
		result := signV4TrimAll(testCase.inputStr)
		if testCase.result != result {
			t.Errorf("Test %d: Expected signV4TrimAll result to be \"%s\", but found it to be \"%s\" instead", i+1, testCase.result, result)
		}
	}
}
