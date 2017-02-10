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
	"fmt"
	"testing"
)

func TestNewSetup(t *testing.T) {
	_, err := getHostIP4("myserver")
	testCases := []struct {
		serverAddr  string
		args        []string
		expectedErr error
	}{
		// FS setup.
		{"", []string{"d1"}, nil},
		{"localhost:10000", []string{"/d1"}, nil},
		// XL setup.
		{"", []string{"/d1", "/d2", "d3", "d4"}, nil},
		{"", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, nil},
		{":10000", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, nil},
		{":10000", []string{"http://localhost/d1", "http://localhost/d2", "http://127.0.0.1/d3", "http://localhost/d4"}, nil},
		// Distribute setup.
		{"", []string{"http://localhost/d1", "http://example.com/d1", "http://example.net/d1", "http://example.org/d1"}, nil},
		{"", []string{"http://localhost:20000/d1", "http://example.com/d1", "http://example.net/d1", "http://example.org/d1"}, nil},
		{"", []string{"http://localhost/d1", "http://localhost/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{"", []string{"http://localhost:10000/d1", "http://localhost:10000/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{":10000", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{":9000", []string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9003/d3", "http://localhost:9004/d4"}, nil},
		// Error setup.
		{"myserver:10000", []string{"d1"}, err},
		{"example.org:10000", []string{"d1"}, fmt.Errorf("host in server address should be this server")},
		{"", []string{"http://localhost/d1"}, fmt.Errorf("FS: Use Path style endpoint")},
		{"", []string{"https://example.org:9000/d1", "https://example.org:9001/d1", "https://example.com/d1", "https://example.com/d2"}, fmt.Errorf("Same path can not be served from different port")},
		{"localhost:10000", []string{"http://example.org/d1", "http://example.org/d2", "http://example.org/d3", "http://example.org/d4"}, fmt.Errorf("no endpoint found for this host")},
		{"", []string{"https://example.org/d1", "https://example.com/d2", "https://example.net/d3", "https://example.edu/d4"}, fmt.Errorf("no endpoint found for this host")},
		{":20000", []string{"http://localhost:10000/d1", "http://localhost:10000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("server address and endpoint have different ports")},
		{"", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("for more than one endpoints for local host with different port, server address must be provided")},
		{":30000", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("port in server address does not match with local endpoints")},
	}

	for _, testCase := range testCases {
		_, err := NewSetup(testCase.serverAddr, testCase.args...)
		if testCase.expectedErr == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}
