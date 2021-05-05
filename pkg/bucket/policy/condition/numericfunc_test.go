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

func testNumericFuncEvaluate(t *testing.T, funcs ...Function) {
	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{funcs[0], map[string][]string{"max-keys": {"16"}}, true},
		{funcs[0], map[string][]string{"max-keys": {"61"}}, false},
		{funcs[1], map[string][]string{"max-keys": {"16"}}, false},
		{funcs[1], map[string][]string{"max-keys": {"61"}}, true},
		{funcs[2], map[string][]string{"max-keys": {"16"}}, false},
		{funcs[2], map[string][]string{"max-keys": {"6"}}, false},
		{funcs[2], map[string][]string{"max-keys": {"61"}}, true},
		{funcs[3], map[string][]string{"max-keys": {"16"}}, true},
		{funcs[3], map[string][]string{"max-keys": {"6"}}, false},
		{funcs[3], map[string][]string{"max-keys": {"61"}}, true},
		{funcs[4], map[string][]string{"max-keys": {"16"}}, false},
		{funcs[4], map[string][]string{"max-keys": {"6"}}, true},
		{funcs[4], map[string][]string{"max-keys": {"61"}}, false},
		{funcs[5], map[string][]string{"max-keys": {"16"}}, true},
		{funcs[5], map[string][]string{"max-keys": {"6"}}, true},
		{funcs[5], map[string][]string{"max-keys": {"61"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Errorf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNumericFuncEvaluate(t *testing.T) {
	valueSet := NewValueSet(NewIntValue(16))

	case1Function, err := newNumericEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNumericNotEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newNumericGreaterThanFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newNumericGreaterThanEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newNumericLessThanFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newNumericLessThanEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testNumericFuncEvaluate(t, case1Function, case2Function, case3Function, case4Function, case5Function, case6Function)

	if _, err := newNumericEqualsFunc(S3MaxKeys, NewValueSet(NewIntValue(16), NewStringValue("16")), ""); err == nil {
		t.Fatalf("error expected")
	}

	if _, err := newNumericEqualsFunc(S3MaxKeys, NewValueSet(NewStringValue("sixy one")), ""); err == nil {
		t.Fatalf("error expected")
	}

	if _, err := newNumericEqualsFunc(S3MaxKeys, NewValueSet(NewBoolValue(true)), ""); err == nil {
		t.Fatalf("error expected")
	}
}

func TestNewNumericFuncEvaluate(t *testing.T) {
	case1Function, err := NewNumericEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := NewNumericNotEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := NewNumericGreaterThanFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := NewNumericGreaterThanEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := NewNumericLessThanFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := NewNumericLessThanEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testNumericFuncEvaluate(t, case1Function, case2Function, case3Function, case4Function, case5Function, case6Function)
}

func TestNumericFuncKey(t *testing.T) {
	case1Function, err := newNumericEqualsFunc(S3MaxKeys, NewValueSet(NewStringValue("16")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3MaxKeys},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNumericFuncName(t *testing.T) {
	valueSet := NewValueSet(NewStringValue("16"))

	case1Function, err := newNumericEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newNumericNotEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newNumericGreaterThanFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newNumericGreaterThanEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newNumericLessThanFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newNumericLessThanEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: numericEquals}},
		{case2Function, name{name: numericNotEquals}},
		{case3Function, name{name: numericGreaterThan}},
		{case4Function, name{name: numericGreaterThanEquals}},
		{case5Function, name{name: numericLessThan}},
		{case6Function, name{name: numericLessThanEquals}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNumericFuncToMap(t *testing.T) {
	valueSet := NewValueSet(NewIntValue(16))
	case1Function, err := newNumericEqualsFunc(S3MaxKeys, valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{S3MaxKeys: valueSet}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNumericFuncClone(t *testing.T) {
	case1Function, err := NewNumericEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &numericFunc{
		n:     name{name: numericEquals},
		k:     S3MaxKeys,
		value: 16,
		c:     equals,
	}

	case2Function, err := NewNumericNotEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &numericFunc{
		n:     name{name: numericNotEquals},
		k:     S3MaxKeys,
		value: 16,
		c:     notEquals,
	}

	case3Function, err := NewNumericGreaterThanFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := &numericFunc{
		n:     name{name: numericGreaterThan},
		k:     S3MaxKeys,
		value: 16,
		c:     greaterThan,
	}

	case4Function, err := NewNumericGreaterThanEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := &numericFunc{
		n:     name{name: numericGreaterThanEquals},
		k:     S3MaxKeys,
		value: 16,
		c:     greaterThanEquals,
	}

	case5Function, err := NewNumericLessThanFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Result := &numericFunc{
		n:     name{name: numericLessThan},
		k:     S3MaxKeys,
		value: 16,
		c:     lessThan,
	}

	case6Function, err := NewNumericLessThanEqualsFunc(S3MaxKeys, 16)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Result := &numericFunc{
		n:     name{name: numericLessThanEquals},
		k:     S3MaxKeys,
		value: 16,
		c:     lessThanEquals,
	}

	testCases := []struct {
		function       Function
		expectedResult Function
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
		{case4Function, case4Result},
		{case5Function, case5Result},
		{case6Function, case6Result},
	}

	for i, testCase := range testCases {
		result := testCase.function.clone()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
