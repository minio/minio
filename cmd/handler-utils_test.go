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
	"bytes"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio/pkg/errors"
)

// Tests validate bucket LocationConstraint.
func TestIsValidLocationContraint(t *testing.T) {
	path, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(path)

	// Test with corrupted XML
	malformedReq := &http.Request{
		Body:          ioutil.NopCloser(bytes.NewBuffer([]byte("<>"))),
		ContentLength: int64(len("<>")),
	}
	if _, err := parseLocationConstraint(malformedReq); err != ErrMalformedXML {
		t.Fatal("Unexpected error: ", err)
	}

	// generates the input request with XML bucket configuration set to the request body.
	createExpectedRequest := func(req *http.Request, location string) (*http.Request, error) {
		createBucketConfig := createBucketLocationConfiguration{}
		createBucketConfig.Location = location
		var createBucketConfigBytes []byte
		createBucketConfigBytes, e := xml.Marshal(createBucketConfig)
		if e != nil {
			return nil, e
		}
		createBucketConfigBuffer := bytes.NewBuffer(createBucketConfigBytes)
		req.Body = ioutil.NopCloser(createBucketConfigBuffer)
		req.ContentLength = int64(createBucketConfigBuffer.Len())
		return req, nil
	}

	testCases := []struct {
		locationForInputRequest string
		serverConfigRegion      string
		expectedCode            APIErrorCode
	}{
		// Test case - 1.
		{globalMinioDefaultRegion, globalMinioDefaultRegion, ErrNone},
		// Test case - 2.
		// In case of empty request body ErrNone is returned.
		{"", globalMinioDefaultRegion, ErrNone},
	}
	for i, testCase := range testCases {
		inputRequest, e := createExpectedRequest(&http.Request{}, testCase.locationForInputRequest)
		if e != nil {
			t.Fatalf("Test %d: Failed to Marshal bucket configuration", i+1)
		}
		globalServerConfig.SetRegion(testCase.serverConfigRegion)
		_, actualCode := parseLocationConstraint(inputRequest)
		if testCase.expectedCode != actualCode {
			t.Errorf("Test %d: Expected the APIErrCode to be %d, but instead found %d", i+1, testCase.expectedCode, actualCode)
		}
	}
}

// Test validate form field size.
func TestValidateFormFieldSize(t *testing.T) {
	testCases := []struct {
		header http.Header
		err    error
	}{
		// Empty header returns error as nil,
		{
			header: nil,
			err:    nil,
		},
		// Valid header returns error as nil.
		{
			header: http.Header{
				"Content-Type": []string{"image/png"},
			},
			err: nil,
		},
		// Invalid header value > maxFormFieldSize+1
		{
			header: http.Header{
				"Garbage": []string{strings.Repeat("a", int(maxFormFieldSize)+1)},
			},
			err: errSizeUnexpected,
		},
	}

	// Run validate form field size check under all test cases.
	for i, testCase := range testCases {
		err := validateFormFieldSize(testCase.header)
		if err != nil {
			if errors.Cause(err).Error() != testCase.err.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.err, err)
			}
		}
	}
}

// Tests validate metadata extraction from http headers.
func TestExtractMetadataHeaders(t *testing.T) {
	testCases := []struct {
		header     http.Header
		metadata   map[string]string
		shouldFail bool
	}{
		// Validate if there a known 'content-type'.
		{
			header: http.Header{
				"Content-Type": []string{"image/png"},
			},
			metadata: map[string]string{
				"content-type": "image/png",
			},
			shouldFail: false,
		},
		// Validate if there are no keys to extract.
		{
			header: http.Header{
				"Test-1": []string{"123"},
			},
			metadata:   map[string]string{},
			shouldFail: false,
		},
		// Validate that there are all headers extracted
		{
			header: http.Header{
				"X-Amz-Meta-Appid":   []string{"amz-meta"},
				"X-Minio-Meta-Appid": []string{"minio-meta"},
			},
			metadata: map[string]string{
				"X-Amz-Meta-Appid":   "amz-meta",
				"X-Minio-Meta-Appid": "minio-meta",
			},
			shouldFail: false,
		},
		// Fail if header key is not in canonicalized form
		{
			header: http.Header{
				"x-amz-meta-appid": []string{"amz-meta"},
			},
			metadata: map[string]string{
				"X-Amz-Meta-Appid": "amz-meta",
			},
			shouldFail: true,
		},
		// Empty header input returns empty metadata.
		{
			header:     nil,
			metadata:   nil,
			shouldFail: true,
		},
	}

	// Validate if the extracting headers.
	for i, testCase := range testCases {
		metadata, err := extractMetadataFromHeader(testCase.header)
		if err != nil && !testCase.shouldFail {
			t.Fatalf("Test %d failed to extract metadata: %v", i+1, err)
		}
		if err == nil && testCase.shouldFail {
			t.Fatalf("Test %d should fail, but it passed", i+1)
		}
		if err == nil && !reflect.DeepEqual(metadata, testCase.metadata) {
			t.Fatalf("Test %d failed: Expected \"%#v\", got \"%#v\"", i+1, testCase.metadata, metadata)
		}
	}
}

// Test getResource()
func TestGetResource(t *testing.T) {
	testCases := []struct {
		p                string
		host             string
		domain           string
		expectedResource string
	}{
		{"/a/b/c", "test.mydomain.com", "mydomain.com", "/test/a/b/c"},
		{"/a/b/c", "test.mydomain.com", "notmydomain.com", "/a/b/c"},
		{"/a/b/c", "test.mydomain.com", "", "/a/b/c"},
	}
	for i, test := range testCases {
		gotResource, err := getResource(test.p, test.host, test.domain)
		if err != nil {
			t.Fatal(err)
		}
		if gotResource != test.expectedResource {
			t.Fatalf("test %d: expected %s got %s", i+1, test.expectedResource, gotResource)
		}
	}
}
