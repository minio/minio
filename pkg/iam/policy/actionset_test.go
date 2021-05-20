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

package iampolicy

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestActionSetAdd(t *testing.T) {
	testCases := []struct {
		set            ActionSet
		action         Action
		expectedResult ActionSet
	}{
		{NewActionSet(), PutObjectAction, NewActionSet(PutObjectAction)},
		{NewActionSet(PutObjectAction), PutObjectAction, NewActionSet(PutObjectAction)},
	}

	for i, testCase := range testCases {
		testCase.set.Add(testCase.action)

		if !reflect.DeepEqual(testCase.expectedResult, testCase.set) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, testCase.set)
		}
	}
}

func TestActionSetMatches(t *testing.T) {
	testCases := []struct {
		set            ActionSet
		action         Action
		expectedResult bool
	}{
		{NewActionSet(AllActions), AbortMultipartUploadAction, true},
		{NewActionSet(PutObjectAction), PutObjectAction, true},
		{NewActionSet(PutObjectAction, GetObjectAction), PutObjectAction, true},
		{NewActionSet(PutObjectAction, GetObjectAction), AbortMultipartUploadAction, false},
	}

	for i, testCase := range testCases {
		result := testCase.set.Match(testCase.action)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestActionSetIntersection(t *testing.T) {
	testCases := []struct {
		set            ActionSet
		setToIntersect ActionSet
		expectedResult ActionSet
	}{
		{NewActionSet(), NewActionSet(PutObjectAction), NewActionSet()},
		{NewActionSet(PutObjectAction), NewActionSet(), NewActionSet()},
		{NewActionSet(PutObjectAction), NewActionSet(PutObjectAction, GetObjectAction), NewActionSet(PutObjectAction)},
	}

	for i, testCase := range testCases {
		result := testCase.set.Intersection(testCase.setToIntersect)

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, testCase.set)
		}
	}
}

func TestActionSetMarshalJSON(t *testing.T) {
	testCases := []struct {
		actionSet      ActionSet
		expectedResult []byte
		expectErr      bool
	}{
		{NewActionSet(PutObjectAction), []byte(`["s3:PutObject"]`), false},
		{NewActionSet(), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.actionSet)
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

func TestActionSetToSlice(t *testing.T) {
	testCases := []struct {
		actionSet      ActionSet
		expectedResult []Action
	}{
		{NewActionSet(PutObjectAction), []Action{PutObjectAction}},
		{NewActionSet(), []Action{}},
	}

	for i, testCase := range testCases {
		result := testCase.actionSet.ToSlice()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestActionSetUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data               []byte
		expectedResult     ActionSet
		expectUnmarshalErr bool
		expectValidateErr  bool
	}{
		{[]byte(`"s3:PutObject"`), NewActionSet(PutObjectAction), false, false},
		{[]byte(`["s3:PutObject"]`), NewActionSet(PutObjectAction), false, false},
		{[]byte(`["s3:PutObject", "s3:GetObject"]`), NewActionSet(PutObjectAction, GetObjectAction), false, false},
		{[]byte(`["s3:PutObject", "s3:GetObject", "s3:PutObject"]`), NewActionSet(PutObjectAction, GetObjectAction), false, false},
		{[]byte(`[]`), NewActionSet(), true, false},           // Empty array.
		{[]byte(`"foo"`), nil, false, true},                   // Invalid action.
		{[]byte(`["s3:PutObject", "foo"]`), nil, false, true}, // Invalid action.
	}

	for i, testCase := range testCases {
		result := make(ActionSet)
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectUnmarshalErr {
			t.Fatalf("case %v: error during unmarshal: expected: %v, got: %v\n", i+1, testCase.expectUnmarshalErr, expectErr)
		}

		err = result.Validate()
		expectErr = (err != nil)
		if expectErr != testCase.expectValidateErr {
			t.Fatalf("case %v: error during validation: expected: %v, got: %v\n", i+1, testCase.expectValidateErr, expectErr)
		}

		if !testCase.expectUnmarshalErr && !testCase.expectValidateErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
