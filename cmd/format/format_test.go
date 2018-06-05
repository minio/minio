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

package format

import "testing"

func TestFormatV1Validate(t *testing.T) {
	testCases := []struct {
		formatV1   formatV1
		formatType string
		expectErr  bool
	}{
		{formatV1{V1, fsType}, fsType, false},
		{formatV1{V1, "xl"}, "xl", false},
		{formatV1{}, fsType, true},
		{formatV1{V2, fsType}, fsType, true},
	}

	for i, testCase := range testCases {
		err := testCase.formatV1.validate(testCase.formatType)
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestFormatV1ValidateFS(t *testing.T) {
	testCases := []struct {
		formatV1  formatV1
		expectErr bool
	}{
		{formatV1{V1, fsType}, false},
		{formatV1{}, true},
		{formatV1{V1, "xl"}, true},
		{formatV1{V2, fsType}, true},
	}

	for i, testCase := range testCases {
		err := testCase.formatV1.validateFS()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
