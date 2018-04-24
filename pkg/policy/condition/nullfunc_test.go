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
	"reflect"
	"testing"
)

func TestNullFuncEvaluate(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(true)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(false)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"prefix": {"true"}}, true},
		{case1Function, map[string][]string{"prefix": {"false"}}, true},
		{case1Function, map[string][]string{"prefix": {"mybucket/foo"}}, true},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},
		{case2Function, map[string][]string{"prefix": {"true"}}, false},
		{case2Function, map[string][]string{"prefix": {"false"}}, false},
		{case2Function, map[string][]string{"prefix": {"mybucket/foo"}}, false},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNullFuncKey(t *testing.T) {
	case1Function, err := newNullFunc(S3XAmzCopySource, NewValueSet(NewBoolValue(true)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3XAmzCopySource},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNullFuncToMap(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(true)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		S3Prefix: NewValueSet(NewBoolValue(true)),
	}

	case2Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(false)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		S3Prefix: NewValueSet(NewBoolValue(false)),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{&nullFunc{}, nil},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewNullFunc(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(true)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNullFunc(S3Prefix, NewValueSet(NewBoolValue(false)))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{S3Prefix, NewValueSet(NewBoolValue(true)), case1Function, false},
		{S3Prefix, NewValueSet(NewStringValue("false")), case2Function, false},
		// Multiple values error.
		{S3Prefix, NewValueSet(NewBoolValue(true), NewBoolValue(false)), nil, true},
		// Invalid boolean string error.
		{S3Prefix, NewValueSet(NewStringValue("foo")), nil, true},
		// Invalid value error.
		{S3Prefix, NewValueSet(NewIntValue(7)), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newNullFunc(testCase.key, testCase.values)
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
