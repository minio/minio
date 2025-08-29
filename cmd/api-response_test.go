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
	"net/http"
	"testing"
)

// Tests object location.
func TestObjectLocation(t *testing.T) {
	testCases := []struct {
		request          *http.Request
		bucket, object   string
		domains          []string
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
				Host:   "mybucket.mys3.bucket.org",
				Header: map[string][]string{},
			},
			domains:          []string{"mys3.bucket.org"},
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "http://mybucket.mys3.bucket.org/test/1.txt",
		},
		{
			request: &http.Request{
				Host: "mybucket.mys3.bucket.org",
				Header: map[string][]string{
					"X-Forwarded-Scheme": {httpsScheme},
				},
			},
			domains:          []string{"mys3.bucket.org"},
			bucket:           "mybucket",
			object:           "test/1.txt",
			expectedLocation: "https://mybucket.mys3.bucket.org/test/1.txt",
		},
	}
	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			gotLocation := getObjectLocation(testCase.request, testCase.domains, testCase.bucket, testCase.object)
			if testCase.expectedLocation != gotLocation {
				t.Errorf("expected %s, got %s", testCase.expectedLocation, gotLocation)
			}
		})
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
