/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"os"
	"testing"
)

// Tests scenarios which can occur for hasExtendedHeader function.
func TestHasExtendedHeader(t *testing.T) {
	// All test cases concerning hasExtendedHeader function.
	testCases := []struct {
		metadata map[string]string
		has      bool
	}{
		// Verifies if X-Amz-Meta is present.
		{
			metadata: map[string]string{
				"X-Amz-Meta-1": "value",
			},
			has: true || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if X-Minio-Meta is present.
		{
			metadata: map[string]string{
				"X-Minio-Meta-1": "value",
			},
			has: true || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if extended header is not present.
		{
			metadata: map[string]string{
				"md5Sum": "value",
			},
			has: false || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if extended header is not present, but with an empty input.
		{
			metadata: nil,
			has:      false || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		has := hasExtendedHeader(testCase.metadata)
		if has != testCase.has {
			t.Fatalf("Test case %d: Expected \"%#v\", but got \"%#v\"", i+1, testCase.has, has)
		}
	}
}
