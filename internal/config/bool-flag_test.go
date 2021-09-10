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

package config

import (
	"testing"
)

// Test BoolFlag.String()
func TestBoolFlagString(t *testing.T) {
	var bf BoolFlag

	testCases := []struct {
		flag           BoolFlag
		expectedResult string
	}{
		{bf, "off"},
		{BoolFlag(true), "on"},
		{BoolFlag(false), "off"},
	}

	for _, testCase := range testCases {
		str := testCase.flag.String()
		if testCase.expectedResult != str {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, str)
		}
	}
}

// Test BoolFlag.MarshalJSON()
func TestBoolFlagMarshalJSON(t *testing.T) {
	var bf BoolFlag

	testCases := []struct {
		flag           BoolFlag
		expectedResult string
	}{
		{bf, `"off"`},
		{BoolFlag(true), `"on"`},
		{BoolFlag(false), `"off"`},
	}

	for _, testCase := range testCases {
		data, _ := testCase.flag.MarshalJSON()
		if testCase.expectedResult != string(data) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, string(data))
		}
	}
}

// Test BoolFlag.UnmarshalJSON()
func TestBoolFlagUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult BoolFlag
		expectedErr    bool
	}{
		{[]byte(`{}`), BoolFlag(false), true},
		{[]byte(`["on"]`), BoolFlag(false), true},
		{[]byte(`"junk"`), BoolFlag(false), true},
		{[]byte(`""`), BoolFlag(true), false},
		{[]byte(`"on"`), BoolFlag(true), false},
		{[]byte(`"off"`), BoolFlag(false), false},
		{[]byte(`"true"`), BoolFlag(true), false},
		{[]byte(`"false"`), BoolFlag(false), false},
		{[]byte(`"ON"`), BoolFlag(true), false},
		{[]byte(`"OFF"`), BoolFlag(false), false},
	}

	for _, testCase := range testCases {
		var flag BoolFlag
		err := (&flag).UnmarshalJSON(testCase.data)
		if !testCase.expectedErr && err != nil {
			t.Fatalf("error: expected = <nil>, got = %v", err)
		}
		if testCase.expectedErr && err == nil {
			t.Fatalf("error: expected error, got = <nil>")
		}
		if err == nil && testCase.expectedResult != flag {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, flag)
		}
	}
}

// Test ParseBoolFlag()
func TestParseBoolFlag(t *testing.T) {
	testCases := []struct {
		flagStr        string
		expectedResult BoolFlag
		expectedErr    bool
	}{
		{"", BoolFlag(false), true},
		{"junk", BoolFlag(false), true},
		{"true", BoolFlag(true), false},
		{"false", BoolFlag(false), false},
		{"ON", BoolFlag(true), false},
		{"OFF", BoolFlag(false), false},
		{"on", BoolFlag(true), false},
		{"off", BoolFlag(false), false},
	}

	for _, testCase := range testCases {
		bf, err := ParseBoolFlag(testCase.flagStr)
		if !testCase.expectedErr && err != nil {
			t.Fatalf("error: expected = <nil>, got = %v", err)
		}
		if testCase.expectedErr && err == nil {
			t.Fatalf("error: expected error, got = <nil>")
		}
		if err == nil && testCase.expectedResult != bf {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, bf)
		}
	}
}
