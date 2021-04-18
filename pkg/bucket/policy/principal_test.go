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

	"github.com/minio/minio-go/v7/pkg/set"
)

func TestPrincipalIsValid(t *testing.T) {
	testCases := []struct {
		principal      Principal
		expectedResult bool
	}{
		{NewPrincipal("*"), true},
		{NewPrincipal("arn:aws:iam::AccountNumber:root"), true},
		{NewPrincipal(), false},
	}

	for i, testCase := range testCases {
		result := testCase.principal.IsValid()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPrincipalIntersection(t *testing.T) {
	testCases := []struct {
		principal            Principal
		principalToIntersect Principal
		expectedResult       set.StringSet
	}{
		{NewPrincipal("*"), NewPrincipal("*"), set.CreateStringSet("*")},
		{NewPrincipal("arn:aws:iam::AccountNumber:root"), NewPrincipal("arn:aws:iam::AccountNumber:myuser"), set.CreateStringSet()},
		{NewPrincipal(), NewPrincipal("*"), set.CreateStringSet()},
	}

	for i, testCase := range testCases {
		result := testCase.principal.Intersection(testCase.principalToIntersect)

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPrincipalMarshalJSON(t *testing.T) {
	testCases := []struct {
		principal      Principal
		expectedResult []byte
		expectErr      bool
	}{
		{NewPrincipal("*"), []byte(`{"AWS":["*"]}`), false},
		{NewPrincipal("arn:aws:iam::AccountNumber:*"), []byte(`{"AWS":["arn:aws:iam::AccountNumber:*"]}`), false},
		{NewPrincipal(), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.principal)
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

func TestPrincipalMatch(t *testing.T) {
	testCases := []struct {
		principals     Principal
		principal      string
		expectedResult bool
	}{
		{NewPrincipal("*"), "AccountNumber", true},
		{NewPrincipal("arn:aws:iam::*"), "arn:aws:iam::AccountNumber:root", true},
		{NewPrincipal("arn:aws:iam::AccountNumber:*"), "arn:aws:iam::TestAccountNumber:root", false},
	}

	for i, testCase := range testCases {
		result := testCase.principals.Match(testCase.principal)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPrincipalUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult Principal
		expectErr      bool
	}{
		{[]byte(`"*"`), NewPrincipal("*"), false},
		{[]byte(`{"AWS": "*"}`), NewPrincipal("*"), false},
		{[]byte(`{"AWS": "arn:aws:iam::AccountNumber:*"}`), NewPrincipal("arn:aws:iam::AccountNumber:*"), false},
		{[]byte(`"arn:aws:iam::AccountNumber:*"`), NewPrincipal(), true},
		{[]byte(`["arn:aws:iam::AccountNumber:*", "arn:aws:iam:AnotherAccount:*"]`), NewPrincipal(), true},
	}

	for i, testCase := range testCases {
		var result Principal
		err := json.Unmarshal(testCase.data, &result)
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
