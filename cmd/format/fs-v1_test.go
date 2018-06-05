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

func TestFsv1validate(t *testing.T) {
	testCases := []struct {
		fsv1      fsv1
		expectErr bool
	}{
		{fsv1{V1}, false},
		{fsv1{}, true},
		{fsv1{V2}, true},
		{fsv1{"1.0"}, true},
	}

	for i, testCase := range testCases {
		err := testCase.fsv1.validate()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestFSV1Validate(t *testing.T) {
	testCases := []struct {
		f         FSV1
		expectErr bool
	}{
		{FSV1{formatV1{V1, fsType}, fsv1{V1}}, false},
		{FSV1{}, true},
		{FSV1{formatV1{V1, fsType}, fsv1{V2}}, true},
		{FSV1{formatV1{V1, fsType}, fsv1{"1.0"}}, true},
		{FSV1{formatV1{V2, fsType}, fsv1{V1}}, true},
		{FSV1{formatV1{V1, "xl"}, fsv1{V1}}, true},
	}

	for i, testCase := range testCases {
		err := testCase.f.Validate()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
