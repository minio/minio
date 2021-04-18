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
		{stringEquals, true},
		{stringNotEquals, true},
		{stringLike, true},
		{stringNotLike, true},
		{ipAddress, true},
		{notIPAddress, true},
		{null, true},
		{name("foo"), false},
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
		{stringEquals, []byte(`"StringEquals"`), false},
		{stringNotEquals, []byte(`"StringNotEquals"`), false},
		{stringLike, []byte(`"StringLike"`), false},
		{stringNotLike, []byte(`"StringNotLike"`), false},
		{ipAddress, []byte(`"IpAddress"`), false},
		{notIPAddress, []byte(`"NotIpAddress"`), false},
		{null, []byte(`"Null"`), false},
		{name("foo"), nil, true},
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
		{[]byte(`"StringEquals"`), stringEquals, false},
		{[]byte(`"foo"`), name(""), true},
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
