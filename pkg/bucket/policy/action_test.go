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

package policy

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestActionIsObjectAction(t *testing.T) {
	testCases := []struct {
		action         Action
		expectedResult bool
	}{
		{AbortMultipartUploadAction, true},
		{DeleteObjectAction, true},
		{GetObjectAction, true},
		{ListMultipartUploadPartsAction, true},
		{PutObjectAction, true},
		{CreateBucketAction, false},
	}

	for i, testCase := range testCases {
		result := testCase.action.isObjectAction()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestActionIsValid(t *testing.T) {
	testCases := []struct {
		action         Action
		expectedResult bool
	}{
		{AbortMultipartUploadAction, true},
		{Action("foo"), false},
	}

	for i, testCase := range testCases {
		result := testCase.action.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestActionMarshalJSON(t *testing.T) {
	testCases := []struct {
		action         Action
		expectedResult []byte
		expectErr      bool
	}{
		{PutObjectAction, []byte(`"s3:PutObject"`), false},
		{Action("foo"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.action)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestActionUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult Action
		expectErr      bool
	}{
		{[]byte(`"s3:PutObject"`), PutObjectAction, false},
		{[]byte(`"foo"`), Action(""), true},
	}

	for i, testCase := range testCases {
		var result Action
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
