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
