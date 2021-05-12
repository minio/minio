// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package condition

import (
	"reflect"
	"testing"
)

func TestNullFuncEvaluate(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"prefix": {"true"}}, false},
		{case1Function, map[string][]string{"prefix": {"false"}}, false},
		{case1Function, map[string][]string{"prefix": {"mybucket/foo"}}, false},
		{case1Function, map[string][]string{}, true},
		{case1Function, map[string][]string{"delimiter": {"/"}}, true},
		{case2Function, map[string][]string{"prefix": {"true"}}, true},
		{case2Function, map[string][]string{"prefix": {"false"}}, true},
		{case2Function, map[string][]string{"prefix": {"mybucket/foo"}}, true},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Errorf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNullFuncKey(t *testing.T) {
	case1Function, err := newNullFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3XAmzCopySource.ToKey()},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNullFuncName(t *testing.T) {
	case1Function, err := newNullFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: null}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNullFuncToMap(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		S3Prefix.ToKey(): NewValueSet(NewBoolValue(true)),
	}

	case2Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		S3Prefix.ToKey(): NewValueSet(NewBoolValue(false)),
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

func TestNullFuncClone(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &nullFunc{
		k:     S3Prefix.ToKey(),
		value: true,
	}

	case2Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &nullFunc{
		k:     S3Prefix.ToKey(),
		value: false,
	}

	testCases := []struct {
		f              Function
		expectedResult Function
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
	}

	for i, testCase := range testCases {
		result := testCase.f.clone()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %+v, got: %+v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewNullFunc(t *testing.T) {
	case1Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNullFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), case1Function, false},
		{S3Prefix.ToKey(), NewValueSet(NewStringValue("false")), case2Function, false},
		// Multiple values error.
		{S3Prefix.ToKey(), NewValueSet(NewBoolValue(true), NewBoolValue(false)), nil, true},
		// Invalid boolean string error.
		{S3Prefix.ToKey(), NewValueSet(NewStringValue("foo")), nil, true},
		// Invalid value error.
		{S3Prefix.ToKey(), NewValueSet(NewIntValue(7)), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newNullFunc(testCase.key, testCase.values, "")
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
