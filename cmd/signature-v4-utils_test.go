// Copyright (c) 2015-2023 MinIO, Inc.
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
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

func TestCheckValid(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	objLayer, fsDir, err := prepareFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)
	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}

	initAllSubsystems(ctx)
	initConfigSubsystem(ctx, objLayer)

	globalIAMSys.Init(ctx, objLayer, globalEtcdClient, 2*time.Second)

	req, err := newTestRequest(http.MethodGet, "http://example.com:9000/bucket/object", 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = signRequestV4(req, globalActiveCred.AccessKey, globalActiveCred.SecretKey); err != nil {
		t.Fatal(err)
	}

	_, owner, s3Err := checkKeyValid(req, globalActiveCred.AccessKey)
	if s3Err != ErrNone {
		t.Fatalf("Unexpected failure with %v", errorCodes.ToAPIErr(s3Err))
	}

	if !owner {
		t.Fatalf("Expected owner to be 'true', found %t", owner)
	}

	_, _, s3Err = checkKeyValid(req, "does-not-exist")
	if s3Err != ErrInvalidAccessKeyID {
		t.Fatalf("Expected error 'ErrInvalidAccessKeyID', found %v", s3Err)
	}

	ucreds, err := auth.CreateCredentials("myuser1", "mypassword1")
	if err != nil {
		t.Fatalf("unable create credential, %s", err)
	}

	_, err = globalIAMSys.CreateUser(ctx, ucreds.AccessKey, madmin.AddOrUpdateUserReq{
		SecretKey: ucreds.SecretKey,
		Status:    madmin.AccountEnabled,
	})
	if err != nil {
		t.Fatalf("unable create credential, %s", err)
	}

	_, owner, s3Err = checkKeyValid(req, ucreds.AccessKey)
	if s3Err != ErrNone {
		t.Fatalf("Unexpected failure with %v", errorCodes.ToAPIErr(s3Err))
	}

	if owner {
		t.Fatalf("Expected owner to be 'false', found %t", owner)
	}

	_, err = globalIAMSys.PolicyDBSet(ctx, ucreds.AccessKey, "consoleAdmin", regUser, false)
	if err != nil {
		t.Fatalf("unable to attach policy to credential, %s", err)
	}

	time.Sleep(4 * time.Second)

	policies, err := globalIAMSys.PolicyDBGet(ucreds.AccessKey)
	if err != nil {
		t.Fatalf("unable to get policy to credential, %s", err)
	}

	if len(policies) == 0 {
		t.Fatal("no policies found")
	}

	if policies[0] != "consoleAdmin" {
		t.Fatalf("expected 'consoleAdmin', %s", policies[0])
	}
}

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
		// Test case with "X-Amz-Content-Sha256" header set, but to empty value but we can't skip.
		{"X-Amz-Content-Sha256", "", "", "", false},

		// Test case - 2.
		// Test case with "X-Amz-Content-Sha256" not set so we can skip.
		{"", "", "", "", true},

		// Test case - 3.
		// Test case with "X-Amz-Content-Sha256" header set to  "UNSIGNED-PAYLOAD"
		// When "X-Amz-Content-Sha256" header is set to  "UNSIGNED-PAYLOAD", validation of content sha256 has to be skipped.
		{"X-Amz-Content-Sha256", unsignedPayload, "X-Amz-Credential", "", true},

		// Test case - 4.
		// Enabling PreSigned Signature v4, but X-Amz-Content-Sha256 not set has to be skipped.
		{"", "", "X-Amz-Credential", "", true},

		// Test case - 5.
		// Enabling PreSigned Signature v4, but X-Amz-Content-Sha256 set and its not UNSIGNED-PAYLOAD, we shouldn't skip.
		{"X-Amz-Content-Sha256", "somevalue", "X-Amz-Credential", "", false},

		// Test case - 6.
		// Test case with "X-Amz-Content-Sha256" header set to  "UNSIGNED-PAYLOAD" and its not presigned, we should skip.
		{"X-Amz-Content-Sha256", unsignedPayload, "", "", true},

		// Test case - 7.
		// "X-Amz-Content-Sha256" not set and  PreSigned Signature v4 not enabled, sha256 checksum calculation is not skipped.
		{"", "", "X-Amz-Credential", "", true},

		// Test case - 8.
		// "X-Amz-Content-Sha256" has a proper value cannot skip.
		{"X-Amz-Content-Sha256", "somevalue", "", "", false},
	}

	for i, testCase := range testCases {
		// creating an input HTTP request.
		// Only the headers are relevant for this particular test.
		inputReq, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("Error initializing input HTTP request: %v", err)
		}
		if testCase.inputQueryKey != "" {
			q := inputReq.URL.Query()
			q.Add(testCase.inputQueryKey, testCase.inputQueryValue)
			if testCase.inputHeaderKey != "" {
				q.Add(testCase.inputHeaderKey, testCase.inputHeaderValue)
			}
			inputReq.URL.RawQuery = q.Encode()
		} else if testCase.inputHeaderKey != "" {
			inputReq.Header.Set(testCase.inputHeaderKey, testCase.inputHeaderValue)
		}
		inputReq.ParseForm()

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
		{"", "", true},
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

// TestExtractSignedHeaders - Tests validate extraction of signed headers using list of signed header keys.
func TestExtractSignedHeaders(t *testing.T) {
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date", "transfer-encoding"}

	// If the `expect` key exists in the signed headers then golang server would have stripped out the value, expecting the `expect` header set to `100-continue` in the result.
	signedHeaders = append(signedHeaders, "expect")
	// expected header values.
	expectedHost := "play.min.io:9000"
	expectedContentSha256 := "1234abcd"
	expectedTime := UTCNow().Format(iso8601Format)
	expectedTransferEncoding := "gzip"
	expectedExpect := "100-continue"

	r, err := http.NewRequest(http.MethodGet, "http://play.min.io:9000", nil)
	if err != nil {
		t.Fatal("Unable to create http.Request :", err)
	}
	r.TransferEncoding = []string{expectedTransferEncoding}

	// Creating input http header.
	inputHeader := r.Header
	inputHeader.Set("x-amz-content-sha256", expectedContentSha256)
	inputHeader.Set("x-amz-date", expectedTime)
	// calling the function being tested.
	extractedSignedHeaders, errCode := extractSignedHeaders(signedHeaders, r)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}

	inputQuery := r.URL.Query()
	// case where some headers need to get from request query
	signedHeaders = append(signedHeaders, "x-amz-server-side-encryption")
	// expect to fail with `ErrUnsignedHeaders` because couldn't find some header
	_, errCode = extractSignedHeaders(signedHeaders, r)
	if errCode != ErrUnsignedHeaders {
		t.Fatalf("Expected the APIErrorCode to %d, but got %d", ErrUnsignedHeaders, errCode)
	}
	// set headers value through Get parameter
	inputQuery.Add("x-amz-server-side-encryption", xhttp.AmzEncryptionAES)
	r.URL.RawQuery = inputQuery.Encode()
	r.ParseForm()
	_, errCode = extractSignedHeaders(signedHeaders, r)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}

	// "x-amz-content-sha256" header value from the extracted result.
	extractedContentSha256 := extractedSignedHeaders.Get("x-amz-content-sha256")
	// "host" header value from the extracted result.
	extractedHost := extractedSignedHeaders.Get("host")
	//  "x-amz-date" header from the extracted result.
	extractedDate := extractedSignedHeaders.Get("x-amz-date")
	// extracted `expect` header.
	extractedExpect := extractedSignedHeaders.Get("expect")

	extractedTransferEncoding := extractedSignedHeaders.Get("transfer-encoding")

	if expectedHost != extractedHost {
		t.Errorf("host header mismatch: expected `%s`, got `%s`", expectedHost, extractedHost)
	}
	// assert the result with the expected value.
	if expectedContentSha256 != extractedContentSha256 {
		t.Errorf("x-amz-content-sha256 header mismatch: expected `%s`, got `%s`", expectedContentSha256, extractedContentSha256)
	}
	if expectedTime != extractedDate {
		t.Errorf("x-amz-date header mismatch: expected `%s`, got `%s`", expectedTime, extractedDate)
	}
	if extractedTransferEncoding != expectedTransferEncoding {
		t.Errorf("transfer-encoding mismatch: expected %s, got %s", expectedTransferEncoding, extractedTransferEncoding)
	}

	// Since the list of signed headers value contained `expect`, the default value of `100-continue` will be added to extracted signed headers.
	if extractedExpect != expectedExpect {
		t.Errorf("expect header incorrect value: expected `%s`, got `%s`", expectedExpect, extractedExpect)
	}

	// case where the headers don't contain the one of the signed header in the signed headers list.
	signedHeaders = append(signedHeaders, "X-Amz-Credential")
	// expected to fail with `ErrUnsignedHeaders`.
	_, errCode = extractSignedHeaders(signedHeaders, r)
	if errCode != ErrUnsignedHeaders {
		t.Fatalf("Expected the APIErrorCode to %d, but got %d", ErrUnsignedHeaders, errCode)
	}

	// case where the list of signed headers doesn't contain the host field.
	signedHeaders = signedHeaders[2:5]
	// expected to fail with `ErrUnsignedHeaders`.
	_, errCode = extractSignedHeaders(signedHeaders, r)
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

// Test getContentSha256Cksum
func TestGetContentSha256Cksum(t *testing.T) {
	testCases := []struct {
		h        string // header SHA256
		q        string // query SHA256
		expected string // expected SHA256
	}{
		{"shastring", "", "shastring"},
		{emptySHA256, "", emptySHA256},
		{"", "", emptySHA256},
		{"", "X-Amz-Credential=random", unsignedPayload},
		{"", "X-Amz-Credential=random&X-Amz-Content-Sha256=" + unsignedPayload, unsignedPayload},
		{"", "X-Amz-Credential=random&X-Amz-Content-Sha256=shastring", "shastring"},
	}

	for i, testCase := range testCases {
		r, err := http.NewRequest(http.MethodGet, "http://localhost/?"+testCase.q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if testCase.h != "" {
			r.Header.Set("x-amz-content-sha256", testCase.h)
		}
		r.ParseForm()
		got := getContentSha256Cksum(r, serviceS3)
		if got != testCase.expected {
			t.Errorf("Test %d: got:%s expected:%s", i+1, got, testCase.expected)
		}
	}
}

// Test TestCheckMetaHeaders tests the logic of checkMetaHeaders() function
func TestCheckMetaHeaders(t *testing.T) {
	signedHeadersMap := map[string][]string{
		"X-Amz-Meta-Test":      {"test"},
		"X-Amz-Meta-Extension": {"png"},
		"X-Amz-Meta-Name":      {"imagepng"},
	}
	expectedMetaTest := "test"
	expectedMetaExtension := "png"
	expectedMetaName := "imagepng"
	r, err := http.NewRequest(http.MethodPut, "http://play.min.io:9000", nil)
	if err != nil {
		t.Fatal("Unable to create http.Request :", err)
	}

	// Creating input http header.
	inputHeader := r.Header
	inputHeader.Set("X-Amz-Meta-Test", expectedMetaTest)
	inputHeader.Set("X-Amz-Meta-Extension", expectedMetaExtension)
	inputHeader.Set("X-Amz-Meta-Name", expectedMetaName)
	// calling the function being tested.
	errCode := checkMetaHeaders(signedHeadersMap, r)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}

	// Add new metadata in inputHeader
	inputHeader.Set("X-Amz-Meta-Clone", "fail")
	// calling the function being tested.
	errCode = checkMetaHeaders(signedHeadersMap, r)
	if errCode != ErrUnsignedHeaders {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrUnsignedHeaders, errCode)
	}

	// Delete extra metadata from header to don't affect other test
	inputHeader.Del("X-Amz-Meta-Clone")
	// calling the function being tested.
	errCode = checkMetaHeaders(signedHeadersMap, r)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}

	// Creating input url values
	r, err = http.NewRequest(http.MethodPut, "http://play.min.io:9000?x-amz-meta-test=test&x-amz-meta-extension=png&x-amz-meta-name=imagepng", nil)
	if err != nil {
		t.Fatal("Unable to create http.Request :", err)
	}

	r.ParseForm()
	// calling the function being tested.
	errCode = checkMetaHeaders(signedHeadersMap, r)
	if errCode != ErrNone {
		t.Fatalf("Expected the APIErrorCode to be %d, but got %d", ErrNone, errCode)
	}
}
