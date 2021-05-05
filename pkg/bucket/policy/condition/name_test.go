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
	"encoding/json"
	"reflect"
	"testing"
)

func TestNameIsValid(t *testing.T) {
	testCases := []struct {
		n              name
		expectedResult bool
	}{
		{name{name: stringEquals}, true},
		{name{name: stringNotEquals}, true},
		{name{name: stringLike}, true},
		{name{name: stringNotLike}, true},
		{name{name: ipAddress}, true},
		{name{name: notIPAddress}, true},
		{name{name: null}, true},
		{name{name: "foo"}, false},
		{name{qualifier: forAllValues, name: stringEquals}, true},
		{name{qualifier: forAnyValue, name: stringNotEquals}, true},
	}

	for i, testCase := range testCases {
		result := testCase.n.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameMarshalJSON(t *testing.T) {
	testCases := []struct {
		n              name
		expectedResult []byte
		expectErr      bool
	}{
		{name{name: stringEquals}, []byte(`"StringEquals"`), false},
		{name{name: stringNotEquals}, []byte(`"StringNotEquals"`), false},
		{name{name: stringLike}, []byte(`"StringLike"`), false},
		{name{name: stringNotLike}, []byte(`"StringNotLike"`), false},
		{name{name: ipAddress}, []byte(`"IpAddress"`), false},
		{name{name: notIPAddress}, []byte(`"NotIpAddress"`), false},
		{name{name: null}, []byte(`"Null"`), false},
		{name{name: "foo"}, nil, true},
		{name{qualifier: forAllValues, name: stringEquals}, []byte(`"ForAllValues:StringEquals"`), false},
		{name{qualifier: forAnyValue, name: stringNotEquals}, []byte(`"ForAnyValue:StringNotEquals"`), false},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.n)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestNameUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult name
		expectErr      bool
	}{
		{[]byte(`"StringEquals"`), name{name: stringEquals}, false},
		{[]byte(`"foo"`), name{name: ""}, true},
		{[]byte(`"ForAllValues:StringEquals"`), name{qualifier: forAllValues, name: stringEquals}, false},
		{[]byte(`"ForAnyValue:StringNotEquals"`), name{qualifier: forAnyValue, name: stringNotEquals}, false},
	}

	for i, testCase := range testCases {
		var result name
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
