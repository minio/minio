/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
