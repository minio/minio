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

package main

import (
	"fmt"
	"testing"
)

func TestCheckGoVersion(t *testing.T) {
	// Test success cases.
	testCases := []struct {
		version     string
		expectedErr error
	}{
		{minGoVersion, nil},
		{"1.6.8", fmt.Errorf("Minio is not compiled by Go >= 1.9.1.  Please recompile accordingly")},
		{"1.5", fmt.Errorf("Minio is not compiled by Go >= 1.9.1.  Please recompile accordingly")},
		{"0.1", fmt.Errorf("Minio is not compiled by Go >= 1.9.1.  Please recompile accordingly")},
		{".1", fmt.Errorf("Malformed version: .1")},
		{"somejunk", fmt.Errorf("Malformed version: somejunk")},
	}

	for _, testCase := range testCases {
		err := checkGoVersion(testCase.version)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("expected: %v, got: %v", testCase.expectedErr, err)
			}
		} else if err == nil {
			t.Fatalf("expected: %v, got: %v", testCase.expectedErr, err)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("expected: %v, got: %v", testCase.expectedErr, err)
		}
	}
}
