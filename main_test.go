/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"testing"
)

func TestCheckGoVersion(t *testing.T) {
	// Test success cases.
	testCases := []struct {
		version string
		success bool
	}{
		{minGoVersion, true},
		{"1.6.8", false},
		{"1.5", false},
		{"0.1", false},
		{".1", false},
		{"somejunk", false},
	}

	for _, testCase := range testCases {
		err := checkGoVersion(testCase.version)
		if err != nil && testCase.success {
			t.Fatalf("Test %v, expected: success, got: %v", testCase, err)
		}
		if err == nil && !testCase.success {
			t.Fatalf("Test %v, expected: failure, got: success", testCase)
		}
	}
}
