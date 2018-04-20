/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package net

import (
	"testing"
)

func TestPortString(t *testing.T) {
	testCases := []struct {
		port        Port
		expectedStr string
	}{
		{Port(0), "0"},
		{Port(9000), "9000"},
		{Port(65535), "65535"},
		{Port(1024), "1024"},
	}

	for i, testCase := range testCases {
		str := testCase.port.String()

		if str != testCase.expectedStr {
			t.Fatalf("test %v: error: port: %v, got: %v", i+1, testCase.expectedStr, str)
		}
	}
}

func TestParsePort(t *testing.T) {
	testCases := []struct {
		s            string
		expectedPort Port
		expectErr    bool
	}{
		{"0", Port(0), false},
		{"9000", Port(9000), false},
		{"65535", Port(65535), false},
		{"90000", Port(0), true},
		{"-10", Port(0), true},
		{"", Port(0), true},
		{"http", Port(0), true},
		{" 1024", Port(0), true},
	}

	for i, testCase := range testCases {
		port, err := ParsePort(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if port != testCase.expectedPort {
				t.Fatalf("test %v: error: port: %v, got: %v", i+1, testCase.expectedPort, port)
			}
		}
	}
}
