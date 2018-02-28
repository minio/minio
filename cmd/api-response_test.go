/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// Tests object location.
func TestObjectLocation(t *testing.T) {
	testCases := []struct {
		request          *http.Request
		bucket, object   string
		domain           string
		expectedLocation string
	}{
		// Server binding to localhost IP with https.
		{
			request: &http.Request{
				Host: "127.0.0.1:9000",
				Header: map[string][]string{
					"X-Forwarded-Scheme": {httpScheme},
				},
			},
			bucket:           "testbucket1",
			object:           "test/1.txt",
			expectedLocation: "http://127.0.0.1:9000/testbucket1/test/1.txt",
		},
		{
			request: &http.Request{
				Host: "127.0.0.1:9000",
				Header: map[string][]string{
					"X-Forwarded-Scheme": {httpsScheme},
				},
			},
			bucket:           "testbucket1",
			object:           "test/1.txt",
			expectedLocation: "https://127.0.0.1:9000/testbucket1/test/1.txt",
		},
		// Server binding to fqdn.
		{
			request: &http.Request{
				Host: "s3.mybucket.org",
				Header: map[string][]string{
					"X-Forwarded-Scheme": {httpScheme},
				},
			},
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "http://s3.mybucket.org/mybucket/test/1.txt",
		},
		// Server binding to fqdn.
		{
			request: &http.Request{
				Host:   "mys3.mybucket.org",
				Header: map[string][]string{},
			},
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "http://mys3.mybucket.org/mybucket/test/1.txt",
		},
		// Server with virtual domain name.
		{
			request: &http.Request{
				Host:   "mys3.bucket.org",
				Header: map[string][]string{},
			},
			domain:           "mys3.bucket.org",
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "http://mybucket.mys3.bucket.org/test/1.txt",
		},
		{
			request: &http.Request{
				Host: "mys3.bucket.org",
				Header: map[string][]string{
					"X-Forwarded-Scheme": {httpsScheme},
				},
			},
			domain:           "mys3.bucket.org",
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "https://mybucket.mys3.bucket.org/test/1.txt",
		},
	}
	for i, testCase := range testCases {
		gotLocation := getObjectLocation(testCase.request, testCase.domain, testCase.bucket, testCase.object)
		if testCase.expectedLocation != gotLocation {
			t.Errorf("Test %d: expected %s, got %s", i+1, testCase.expectedLocation, gotLocation)
		}
	}
}

// Tests getURLScheme function behavior.
func TestGetURLScheme(t *testing.T) {
	tls := false
	gotScheme := getURLScheme(tls)
	if gotScheme != httpScheme {
		t.Errorf("Expected %s, got %s", httpScheme, gotScheme)
	}
	tls = true
	gotScheme = getURLScheme(tls)
	if gotScheme != httpsScheme {
		t.Errorf("Expected %s, got %s", httpsScheme, gotScheme)
	}
}
