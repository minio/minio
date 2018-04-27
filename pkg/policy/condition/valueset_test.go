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

func TestValueSetAdd(t *testing.T) {
	testCases := []struct {
		value          Value
		expectedResult ValueSet
	}{
		{NewBoolValue(true), NewValueSet(NewBoolValue(true))},
		{NewIntValue(7), NewValueSet(NewIntValue(7))},
		{NewStringValue("foo"), NewValueSet(NewStringValue("foo"))},
	}

	for i, testCase := range testCases {
		result := NewValueSet()
		result.Add(testCase.value)

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestValueSetMarshalJSON(t *testing.T) {
	testCases := []struct {
		set            ValueSet
		expectedResult string
		expectErr      bool
	}{
		{NewValueSet(NewBoolValue(true)), `[true]`, false},
		{NewValueSet(NewIntValue(7)), `[7]`, false},
		{NewValueSet(NewStringValue("foo")), `["foo"]`, false},
		{NewValueSet(NewBoolValue(true)), `[true]`, false},
		{NewValueSet(NewStringValue("7")), `["7"]`, false},
		{NewValueSet(NewStringValue("foo")), `["foo"]`, false},
		{make(ValueSet), "", true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.set)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if string(result) != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, string(result))
			}
		}
	}
}

func TestValueSetUnmarshalJSON(t *testing.T) {
	set1 := NewValueSet(
		NewBoolValue(true),
		NewStringValue("false"),
		NewIntValue(7),
		NewStringValue("7"),
		NewStringValue("foo"),
		NewStringValue("192.168.1.100/24"),
	)

	testCases := []struct {
		data           []byte
		expectedResult ValueSet
		expectErr      bool
	}{
		{[]byte(`true`), NewValueSet(NewBoolValue(true)), false},
		{[]byte(`7`), NewValueSet(NewIntValue(7)), false},
		{[]byte(`"foo"`), NewValueSet(NewStringValue("foo")), false},
		{[]byte(`[true]`), NewValueSet(NewBoolValue(true)), false},
		{[]byte(`[7]`), NewValueSet(NewIntValue(7)), false},
		{[]byte(`["foo"]`), NewValueSet(NewStringValue("foo")), false},
		{[]byte(`[true, "false", 7, "7", "foo", "192.168.1.100/24"]`), set1, false},
		{[]byte(`{}`), nil, true},           // Unsupported data.
		{[]byte(`[]`), nil, true},           // Empty array.
		{[]byte(`[7, 7, true]`), nil, true}, // Duplicate value.
	}

	for i, testCase := range testCases {
		result := make(ValueSet)
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
