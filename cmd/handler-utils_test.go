/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"reflect"
	"testing"
)

// Tests validate bucket LocationConstraint.
func TestIsValidLocationContraint(t *testing.T) {
	path, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(path)

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
		{"us-east-1", "us-east-1", ErrNone},
		// Test case - 2.
		// In case of empty request body ErrNone is returned.
		{"", "us-east-1", ErrNone},
		// Test case - 3.
		{"eu-central-1", "us-east-1", ErrInvalidRegion},
	}
	for i, testCase := range testCases {
		inputRequest, e := createExpectedRequest(&http.Request{}, testCase.locationForInputRequest)
		if e != nil {
			t.Fatalf("Test %d: Failed to Marshal bucket configuration", i+1)
		}
		serverConfig.SetRegion(testCase.serverConfigRegion)
		actualCode := isValidLocationConstraint(inputRequest)
		if testCase.expectedCode != actualCode {
			t.Errorf("Test %d: Expected the APIErrCode to be %d, but instead found %d", i+1, testCase.expectedCode, actualCode)
		}
	}
}

// Tests validate metadata extraction from http headers.
func TestExtractMetadataHeaders(t *testing.T) {
	testCases := []struct {
		header   http.Header
		metadata map[string]string
	}{
		// Validate if there a known 'content-type'.
		{
			header: http.Header{
				"Content-Type": []string{"image/png"},
			},
			metadata: map[string]string{
				"content-type": "image/png",
			},
		},
		// Validate if there are no keys to extract.
		{
			header: http.Header{
				"test-1": []string{"123"},
			},
			metadata: map[string]string{},
		},
	}

	// Validate if the extracting headers.
	for i, testCase := range testCases {
		metadata := extractMetadataFromHeader(testCase.header)
		if !reflect.DeepEqual(metadata, testCase.metadata) {
			t.Fatalf("Test %d failed: Expected \"%#v\", got \"%#v\"", i+1, testCase.metadata, metadata)
		}
	}
}
