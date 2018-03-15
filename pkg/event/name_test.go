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

package event

import (
	"encoding/json"
	"encoding/xml"
	"reflect"
	"testing"
)

func TestNameExpand(t *testing.T) {
	testCases := []struct {
		name           Name
		expectedResult []Name
	}{
		{ObjectAccessedAll, []Name{ObjectAccessedGet, ObjectAccessedHead}},
		{ObjectCreatedAll, []Name{ObjectCreatedCompleteMultipartUpload, ObjectCreatedCopy, ObjectCreatedPost, ObjectCreatedPut}},
		{ObjectRemovedAll, []Name{ObjectRemovedDelete}},
		{ObjectAccessedHead, []Name{ObjectAccessedHead}},
	}

	for i, testCase := range testCases {
		result := testCase.name.Expand()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameString(t *testing.T) {
	var blankName Name

	testCases := []struct {
		name           Name
		expectedResult string
	}{
		{ObjectAccessedAll, "s3:ObjectAccessed:*"},
		{ObjectAccessedGet, "s3:ObjectAccessed:Get"},
		{ObjectAccessedHead, "s3:ObjectAccessed:Head"},
		{ObjectCreatedAll, "s3:ObjectCreated:*"},
		{ObjectCreatedCompleteMultipartUpload, "s3:ObjectCreated:CompleteMultipartUpload"},
		{ObjectCreatedCopy, "s3:ObjectCreated:Copy"},
		{ObjectCreatedPost, "s3:ObjectCreated:Post"},
		{ObjectCreatedPut, "s3:ObjectCreated:Put"},
		{ObjectRemovedAll, "s3:ObjectRemoved:*"},
		{ObjectRemovedDelete, "s3:ObjectRemoved:Delete"},
		{blankName, ""},
	}

	for i, testCase := range testCases {
		result := testCase.name.String()

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameMarshalXML(t *testing.T) {
	var blankName Name

	testCases := []struct {
		name         Name
		expectedData []byte
		expectErr    bool
	}{
		{ObjectAccessedAll, []byte("<Name>s3:ObjectAccessed:*</Name>"), false},
		{ObjectRemovedDelete, []byte("<Name>s3:ObjectRemoved:Delete</Name>"), false},
		{blankName, []byte("<Name></Name>"), false},
	}

	for i, testCase := range testCases {
		data, err := xml.Marshal(testCase.name)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(data, testCase.expectedData) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, string(testCase.expectedData), string(data))
			}
		}
	}
}

func TestNameUnmarshalXML(t *testing.T) {
	var blankName Name

	testCases := []struct {
		data         []byte
		expectedName Name
		expectErr    bool
	}{
		{[]byte("<Name>s3:ObjectAccessed:*</Name>"), ObjectAccessedAll, false},
		{[]byte("<Name>s3:ObjectRemoved:Delete</Name>"), ObjectRemovedDelete, false},
		{[]byte("<Name></Name>"), blankName, true},
	}

	for i, testCase := range testCases {
		var name Name
		err := xml.Unmarshal(testCase.data, &name)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(name, testCase.expectedName) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedName, name)
			}
		}
	}
}

func TestNameMarshalJSON(t *testing.T) {
	var blankName Name

	testCases := []struct {
		name         Name
		expectedData []byte
		expectErr    bool
	}{
		{ObjectAccessedAll, []byte(`"s3:ObjectAccessed:*"`), false},
		{ObjectRemovedDelete, []byte(`"s3:ObjectRemoved:Delete"`), false},
		{blankName, []byte(`""`), false},
	}

	for i, testCase := range testCases {
		data, err := json.Marshal(testCase.name)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(data, testCase.expectedData) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, string(testCase.expectedData), string(data))
			}
		}
	}
}

func TestNameUnmarshalJSON(t *testing.T) {
	var blankName Name

	testCases := []struct {
		data         []byte
		expectedName Name
		expectErr    bool
	}{
		{[]byte(`"s3:ObjectAccessed:*"`), ObjectAccessedAll, false},
		{[]byte(`"s3:ObjectRemoved:Delete"`), ObjectRemovedDelete, false},
		{[]byte(`""`), blankName, true},
	}

	for i, testCase := range testCases {
		var name Name
		err := json.Unmarshal(testCase.data, &name)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(name, testCase.expectedName) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedName, name)
			}
		}
	}
}

func TestParseName(t *testing.T) {
	var blankName Name

	testCases := []struct {
		s            string
		expectedName Name
		expectErr    bool
	}{
		{"s3:ObjectAccessed:*", ObjectAccessedAll, false},
		{"s3:ObjectRemoved:Delete", ObjectRemovedDelete, false},
		{"", blankName, true},
	}

	for i, testCase := range testCases {
		name, err := ParseName(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(name, testCase.expectedName) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedName, name)
			}
		}
	}
}
