/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

package cmd

import (
	"errors"
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
		expectedErr    error
	}{
		{[]byte(`{}`), BoolFlag(false), errors.New("json: cannot unmarshal object into Go value of type string")},
		{[]byte(`["on"]`), BoolFlag(false), errors.New("json: cannot unmarshal array into Go value of type string")},
		{[]byte(`"junk"`), BoolFlag(false), errors.New("invalid value ‘junk’ for BoolFlag")},
		{[]byte(`"true"`), BoolFlag(false), errors.New("invalid value ‘true’ for BoolFlag")},
		{[]byte(`"false"`), BoolFlag(false), errors.New("invalid value ‘false’ for BoolFlag")},
		{[]byte(`"ON"`), BoolFlag(false), errors.New("invalid value ‘ON’ for BoolFlag")},
		{[]byte(`"OFF"`), BoolFlag(false), errors.New("invalid value ‘OFF’ for BoolFlag")},
		{[]byte(`""`), BoolFlag(true), nil},
		{[]byte(`"on"`), BoolFlag(true), nil},
		{[]byte(`"off"`), BoolFlag(false), nil},
	}

	for _, testCase := range testCases {
		var flag BoolFlag
		err := (&flag).UnmarshalJSON(testCase.data)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
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
		expectedErr    error
	}{
		{"", BoolFlag(false), errors.New("invalid value ‘’ for BoolFlag")},
		{"junk", BoolFlag(false), errors.New("invalid value ‘junk’ for BoolFlag")},
		{"true", BoolFlag(false), errors.New("invalid value ‘true’ for BoolFlag")},
		{"false", BoolFlag(false), errors.New("invalid value ‘false’ for BoolFlag")},
		{"ON", BoolFlag(false), errors.New("invalid value ‘ON’ for BoolFlag")},
		{"OFF", BoolFlag(false), errors.New("invalid value ‘OFF’ for BoolFlag")},
		{"on", BoolFlag(true), nil},
		{"off", BoolFlag(false), nil},
	}

	for _, testCase := range testCases {
		bf, err := ParseBoolFlag(testCase.flagStr)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if err == nil && testCase.expectedResult != bf {
			t.Fatalf("result: expected: %v, got: %v", testCase.expectedResult, bf)
		}
	}
}
