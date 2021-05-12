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

func TestBooleanFuncEvaluate(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"SecureTransport": {"true"}}, true},
		{case2Function, map[string][]string{"SecureTransport": {"false"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Errorf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}

	if _, err := newBooleanFunc(S3Prefix.ToKey(), NewValueSet(NewBoolValue(true)), ""); err == nil {
		t.Errorf("error expected")
	}
}

func TestBooleanFuncKey(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, AWSSecureTransport.ToKey()},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestBooleanFuncName(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: boolean}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestBooleanFuncToMap(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		AWSSecureTransport.ToKey(): NewValueSet(NewStringValue("true")),
	}

	case2Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		AWSSecureTransport.ToKey(): NewValueSet(NewStringValue("false")),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestBooleanFuncClone(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &booleanFunc{k: AWSSecureTransport.ToKey(), value: "true"}

	case2Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &booleanFunc{k: AWSSecureTransport.ToKey(), value: "false"}

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
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewBooleanFunc(t *testing.T) {
	case1Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newBooleanFunc(AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(false)), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{AWSSecureTransport.ToKey(), NewValueSet(NewBoolValue(true)), case1Function, false},
		{AWSSecureTransport.ToKey(), NewValueSet(NewStringValue("false")), case2Function, false},
		// Multiple values error.
		{AWSSecureTransport.ToKey(), NewValueSet(NewStringValue("true"), NewStringValue("false")), nil, true},
		// Invalid boolean string error.
		{AWSSecureTransport.ToKey(), NewValueSet(NewStringValue("foo")), nil, true},
		// Invalid value error.
		{AWSSecureTransport.ToKey(), NewValueSet(NewIntValue(7)), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newBooleanFunc(testCase.key, testCase.values, "")
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
