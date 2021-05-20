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

package sql

import (
	"testing"
)

func TestEvalSQLLike(t *testing.T) {
	dropCases := []struct {
		input, resultExpected string
		matchExpected         bool
	}{
		{"", "", false},
		{"a", "", true},
		{"ab", "b", true},
		{"தமிழ்", "மிழ்", true},
	}

	for i, tc := range dropCases {
		res, ok := dropRune(tc.input)
		if res != tc.resultExpected || ok != tc.matchExpected {
			t.Errorf("DropRune Case %d failed", i)
		}
	}

	matcherCases := []struct {
		iText, iPat        string
		iHasLeadingPercent bool
		resultExpected     string
		matchExpected      bool
	}{
		{"abcd", "bcd", false, "", false},
		{"abcd", "bcd", true, "", true},
		{"abcd", "abcd", false, "", true},
		{"abcd", "abcd", true, "", true},
		{"abcd", "ab", false, "cd", true},
		{"abcd", "ab", true, "cd", true},
		{"abcd", "bc", false, "", false},
		{"abcd", "bc", true, "d", true},
	}

	for i, tc := range matcherCases {
		res, ok := matcher(tc.iText, tc.iPat, tc.iHasLeadingPercent)
		if res != tc.resultExpected || ok != tc.matchExpected {
			t.Errorf("Matcher Case %d failed", i)
		}
	}

	evalCases := []struct {
		iText, iPat   string
		iEsc          rune
		matchExpected bool
		errExpected   error
	}{
		{"abcd", "abc", runeZero, false, nil},
		{"abcd", "abcd", runeZero, true, nil},
		{"abcd", "abc_", runeZero, true, nil},
		{"abcd", "_bdd", runeZero, false, nil},
		{"abcd", "_b_d", runeZero, true, nil},

		{"abcd", "____", runeZero, true, nil},
		{"abcd", "____%", runeZero, true, nil},
		{"abcd", "%____", runeZero, true, nil},
		{"abcd", "%__", runeZero, true, nil},
		{"abcd", "____", runeZero, true, nil},

		{"", "_", runeZero, false, nil},
		{"", "%", runeZero, true, nil},
		{"abcd", "%%%%%", runeZero, true, nil},
		{"abcd", "_____", runeZero, false, nil},
		{"abcd", "%%%%%", runeZero, true, nil},

		{"a%%d", `a\%\%d`, '\\', true, nil},
		{"a%%d", `a\%d`, '\\', false, nil},
		{`a%%\d`, `a\%\%\\d`, '\\', true, nil},
		{`a%%\`, `a\%\%\\`, '\\', true, nil},
		{`a%__%\`, `a\%\_\_\%\\`, '\\', true, nil},

		{`a%__%\`, `a\%\_\_\%_`, '\\', true, nil},
		{`a%__%\`, `a\%\_\__`, '\\', false, nil},
		{`a%__%\`, `a\%\_\_%`, '\\', true, nil},
		{`a%__%\`, `a?%?_?_?%\`, '?', true, nil},
	}

	for i, tc := range evalCases {
		// fmt.Println("Case:", i)
		res, err := evalSQLLike(tc.iText, tc.iPat, tc.iEsc)
		if res != tc.matchExpected || err != tc.errExpected {
			t.Errorf("Eval Case %d failed: %v %v", i, res, err)
		}
	}
}
