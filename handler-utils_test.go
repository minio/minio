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

package main

import (
	"bytes"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"testing"
)

// Tests validate bucket LocationConstraint.
func TestIsValidLocationContraint(t *testing.T) {
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
		actualCode := isValidLocationContraint(inputRequest.Body, testCase.serverConfigRegion)
		if testCase.expectedCode != actualCode {
			t.Errorf("Test %d: Expected the APIErrCode to be %d, but instead found %d", i+1, testCase.expectedCode, actualCode)
		}
	}
}
