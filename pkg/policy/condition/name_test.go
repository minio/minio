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

package condition

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNameIsValid(t *testing.T) {
	testCases := []struct {
		n              name
		expectedResult bool
	}{
		{stringEquals, true},
		{stringNotEquals, true},
		{stringLike, true},
		{stringNotLike, true},
		{ipAddress, true},
		{notIPAddress, true},
		{null, true},
		{name("foo"), false},
	}

	for i, testCase := range testCases {
		result := testCase.n.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameMarshalJSON(t *testing.T) {
	testCases := []struct {
		n              name
		expectedResult []byte
		expectErr      bool
	}{
		{stringEquals, []byte(`"StringEquals"`), false},
		{stringNotEquals, []byte(`"StringNotEquals"`), false},
		{stringLike, []byte(`"StringLike"`), false},
		{stringNotLike, []byte(`"StringNotLike"`), false},
		{ipAddress, []byte(`"IpAddress"`), false},
		{notIPAddress, []byte(`"NotIpAddress"`), false},
		{null, []byte(`"Null"`), false},
		{name("foo"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.n)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestNameUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult name
		expectErr      bool
	}{
		{[]byte(`"StringEquals"`), stringEquals, false},
		{[]byte(`"foo"`), name(""), true},
	}

	for i, testCase := range testCases {
		var result name
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if testCase.expectedResult != result {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
