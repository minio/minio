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

func TestFsv2validate(t *testing.T) {
	testCases := []struct {
		fsv2      fsv2
		expectErr bool
	}{
		{fsv2{V2}, false},
		{fsv2{}, true},
		{fsv2{V1}, true},
		{fsv2{"2.0"}, true},
	}

	for i, testCase := range testCases {
		err := testCase.fsv2.validate()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestFSV2Validate(t *testing.T) {
	case1FSV2 := NewFSV2()
	testCases := []struct {
		f         FSV2
		expectErr bool
	}{
		{*case1FSV2, false},
		{FSV2{}, true},
		{FSV2{formatV1{V1, fsType}, fsv2{V1}}, true},
		{FSV2{formatV1{V1, fsType}, fsv2{"2.0"}}, true},
		{FSV2{formatV1{V2, fsType}, fsv2{V2}}, true},
		{FSV2{formatV1{V1, "xl"}, fsv2{V2}}, true},
	}

	for i, testCase := range testCases {
		err := testCase.f.Validate()
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
