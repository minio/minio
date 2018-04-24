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

package policy

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestIDIsValid(t *testing.T) {
	testCases := []struct {
		id             ID
		expectedResult bool
	}{
		{ID("DenyEncryptionSt1"), true},
		{ID(""), true},
	}

	for i, testCase := range testCases {
		result := testCase.id.IsValid()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestIDMarshalJSON(t *testing.T) {
	testCases := []struct {
		id             ID
		expectedResult []byte
		expectErr      bool
	}{
		{ID("foo"), []byte(`"foo"`), false},
		{ID("1234"), []byte(`"1234"`), false},
		{ID("DenyEncryptionSt1"), []byte(`"DenyEncryptionSt1"`), false},
		{ID(""), []byte(`""`), false},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.id)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestIDUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult ID
		expectErr      bool
	}{
		{[]byte(`"foo"`), ID("foo"), false},
		{[]byte(`"1234"`), ID("1234"), false},
		{[]byte(`"DenyEncryptionSt1"`), ID("DenyEncryptionSt1"), false},
		{[]byte(`""`), ID(""), false},
		{[]byte(`"foo bar"`), ID(""), true},
	}

	for i, testCase := range testCases {
		var result ID
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
