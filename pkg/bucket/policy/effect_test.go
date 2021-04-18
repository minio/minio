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
