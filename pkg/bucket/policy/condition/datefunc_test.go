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
	"time"
)

func testDateFuncEvaluate(t *testing.T, funcs ...Function) {
	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{funcs[0], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, true},
		{funcs[0], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, false},
		{funcs[1], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, false},
		{funcs[1], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, true},
		{funcs[2], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, false},
		{funcs[2], map[string][]string{"object-lock-retain-until-date": {"2008-11-10T15:00:00Z"}}, false},
		{funcs[2], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, true},
		{funcs[3], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, true},
		{funcs[3], map[string][]string{"object-lock-retain-until-date": {"2008-11-10T15:00:00Z"}}, false},
		{funcs[3], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, true},
		{funcs[4], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, false},
		{funcs[4], map[string][]string{"object-lock-retain-until-date": {"2008-11-10T15:00:00Z"}}, true},
		{funcs[4], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, false},
		{funcs[5], map[string][]string{"object-lock-retain-until-date": {"2009-11-10T15:00:00Z"}}, true},
		{funcs[5], map[string][]string{"object-lock-retain-until-date": {"2008-11-10T15:00:00Z"}}, true},
		{funcs[5], map[string][]string{"object-lock-retain-until-date": {"2009-12-10T15:00:00Z"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Errorf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestDateFuncEvaluate(t *testing.T) {
	valueSet := NewValueSet(NewStringValue("2009-11-10T15:00:00Z"))

	case1Function, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newDateNotEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newDateGreaterThanFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newDateGreaterThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newDateLessThanFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newDateLessThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testDateFuncEvaluate(t, case1Function, case2Function, case3Function, case4Function, case5Function, case6Function)

	if _, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), NewValueSet(NewIntValue(20091110), NewStringValue("2009-11-10T15:00:00Z")), ""); err == nil {
		t.Fatalf("error expected")
	}

	if _, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), NewValueSet(NewStringValue("Mon, 02 Jan 2006 15:04:05 MST")), ""); err == nil {
		t.Fatalf("error expected")
	}

	if _, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), NewValueSet(NewIntValue(20091110)), ""); err == nil {
		t.Fatalf("error expected")
	}
}

func TestNewDateFuncEvaluate(t *testing.T) {
	dateValue := time.Date(2009, time.November, 10, 15, 0, 0, 0, time.UTC)

	case1Function, err := NewDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := NewDateNotEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := NewDateGreaterThanFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := NewDateGreaterThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := NewDateLessThanFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := NewDateLessThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testDateFuncEvaluate(t, case1Function, case2Function, case3Function, case4Function, case5Function, case6Function)
}

func TestDateFuncKey(t *testing.T) {
	case1Function, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), NewValueSet(NewStringValue("2009-11-10T15:00:00Z")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3ObjectLockRetainUntilDate.ToKey()},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestDateFuncName(t *testing.T) {
	valueSet := NewValueSet(NewStringValue("2009-11-10T15:00:00Z"))

	case1Function, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newDateNotEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newDateGreaterThanFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newDateGreaterThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newDateLessThanFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newDateLessThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: dateEquals}},
		{case2Function, name{name: dateNotEquals}},
		{case3Function, name{name: dateGreaterThan}},
		{case4Function, name{name: dateGreaterThanEquals}},
		{case5Function, name{name: dateLessThan}},
		{case6Function, name{name: dateLessThanEquals}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestDateFuncToMap(t *testing.T) {
	valueSet := NewValueSet(NewStringValue("2009-11-10T15:00:00Z"))
	case1Function, err := newDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), valueSet, "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{S3ObjectLockRetainUntilDate.ToKey(): valueSet}

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

func TestDateFuncClone(t *testing.T) {
	dateValue := time.Date(2009, time.November, 10, 15, 0, 0, 0, time.UTC)

	case1Function, err := NewDateEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &dateFunc{
		n:     name{name: dateEquals},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
		c:     equals,
	}

	case2Function, err := NewDateNotEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &dateFunc{
		n:     name{name: dateNotEquals},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
		c:     notEquals,
	}

	case3Function, err := NewDateGreaterThanFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := &dateFunc{
		n:     name{name: dateGreaterThan},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
		c:     greaterThan,
	}

	case4Function, err := NewDateGreaterThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := &dateFunc{
		n:     name{name: dateGreaterThanEquals},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
		c:     greaterThanEquals,
	}

	case5Function, err := NewDateLessThanFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Result := &dateFunc{
		n:     name{name: dateLessThan},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
		c:     lessThan,
	}

	case6Function, err := NewDateLessThanEqualsFunc(S3ObjectLockRetainUntilDate.ToKey(), dateValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Result := &dateFunc{
		n:     name{name: dateLessThanEquals},
		k:     S3ObjectLockRetainUntilDate.ToKey(),
		value: dateValue,
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
