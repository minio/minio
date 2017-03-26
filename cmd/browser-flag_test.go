/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// Test BrowserFlag.String()
func TestBrowserFlagString(t *testing.T) {
	var bf BrowserFlag

	testCases := []struct {
		flag           BrowserFlag
		expectedResult string
	}{
		{bf, "off"},
		{BrowserFlag(true), "on"},
		{BrowserFlag(false), "off"},
	}

	for _, testCase := range testCases {
		str := testCase.flag.String()
		if testCase.expectedResult != str {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, str)
		}
	}
}

// Test BrowserFlag.MarshalJSON()
func TestBrowserFlagMarshalJSON(t *testing.T) {
	var bf BrowserFlag

	testCases := []struct {
		flag           BrowserFlag
		expectedResult string
	}{
		{bf, `"off"`},
		{BrowserFlag(true), `"on"`},
		{BrowserFlag(false), `"off"`},
	}

	for _, testCase := range testCases {
		data, _ := testCase.flag.MarshalJSON()
		if testCase.expectedResult != string(data) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, string(data))
		}
	}
}

// Test BrowserFlag.UnmarshalJSON()
func TestBrowserFlagUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult BrowserFlag
		expectedErr    error
	}{
		{[]byte(`{}`), BrowserFlag(false), errors.New("json: cannot unmarshal object into Go value of type string")},
		{[]byte(`["on"]`), BrowserFlag(false), errors.New("json: cannot unmarshal array into Go value of type string")},
		{[]byte(`"junk"`), BrowserFlag(false), errors.New("invalid value ‘junk’ for BrowserFlag")},
		{[]byte(`"true"`), BrowserFlag(false), errors.New("invalid value ‘true’ for BrowserFlag")},
		{[]byte(`"false"`), BrowserFlag(false), errors.New("invalid value ‘false’ for BrowserFlag")},
		{[]byte(`"ON"`), BrowserFlag(false), errors.New("invalid value ‘ON’ for BrowserFlag")},
		{[]byte(`"OFF"`), BrowserFlag(false), errors.New("invalid value ‘OFF’ for BrowserFlag")},
		{[]byte(`""`), BrowserFlag(true), nil},
		{[]byte(`"on"`), BrowserFlag(true), nil},
		{[]byte(`"off"`), BrowserFlag(false), nil},
	}

	for _, testCase := range testCases {
		var flag BrowserFlag
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

// Test ParseBrowserFlag()
func TestParseBrowserFlag(t *testing.T) {
	testCases := []struct {
		flagStr        string
		expectedResult BrowserFlag
		expectedErr    error
	}{
		{"", BrowserFlag(false), errors.New("invalid value ‘’ for BrowserFlag")},
		{"junk", BrowserFlag(false), errors.New("invalid value ‘junk’ for BrowserFlag")},
		{"true", BrowserFlag(false), errors.New("invalid value ‘true’ for BrowserFlag")},
		{"false", BrowserFlag(false), errors.New("invalid value ‘false’ for BrowserFlag")},
		{"ON", BrowserFlag(false), errors.New("invalid value ‘ON’ for BrowserFlag")},
		{"OFF", BrowserFlag(false), errors.New("invalid value ‘OFF’ for BrowserFlag")},
		{"on", BrowserFlag(true), nil},
		{"off", BrowserFlag(false), nil},
	}

	for _, testCase := range testCases {
		bf, err := ParseBrowserFlag(testCase.flagStr)
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
