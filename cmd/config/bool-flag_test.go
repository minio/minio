/*
 * MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
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
