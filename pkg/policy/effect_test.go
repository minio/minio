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

func TestEffectIsAllowed(t *testing.T) {
	testCases := []struct {
		effect         Effect
		check          bool
		expectedResult bool
	}{
		{Allow, false, false},
		{Allow, true, true},
		{Deny, false, true},
		{Deny, true, false},
	}

	for i, testCase := range testCases {
		result := testCase.effect.IsAllowed(testCase.check)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}

}

func TestEffectIsValid(t *testing.T) {
	testCases := []struct {
		effect         Effect
		expectedResult bool
	}{
		{Allow, true},
		{Deny, true},
		{Effect(""), false},
		{Effect("foo"), false},
	}

	for i, testCase := range testCases {
		result := testCase.effect.IsValid()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestEffectMarshalJSON(t *testing.T) {
	testCases := []struct {
		effect         Effect
		expectedResult []byte
		expectErr      bool
	}{
		{Allow, []byte(`"Allow"`), false},
		{Deny, []byte(`"Deny"`), false},
		{Effect(""), nil, true},
		{Effect("foo"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.effect)
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

func TestEffectUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult Effect
		expectErr      bool
	}{
		{[]byte(`"Allow"`), Allow, false},
		{[]byte(`"Deny"`), Deny, false},
		{[]byte(`""`), Effect(""), true},
		{[]byte(`"foo"`), Effect(""), true},
	}

	for i, testCase := range testCases {
		var result Effect
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
