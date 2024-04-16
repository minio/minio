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
		{BucketCreated, []Name{BucketCreated}},
		{BucketRemoved, []Name{BucketRemoved}},
		{ObjectAccessedAll, []Name{ObjectAccessedGet, ObjectAccessedHead, ObjectAccessedGetRetention, ObjectAccessedGetLegalHold, ObjectAccessedAttributes}},
		{ObjectCreatedAll, []Name{
			ObjectCreatedCompleteMultipartUpload, ObjectCreatedCopy, ObjectCreatedPost, ObjectCreatedPut,
			ObjectCreatedPutRetention, ObjectCreatedPutLegalHold, ObjectCreatedPutTagging, ObjectCreatedDeleteTagging,
		}},
		{ObjectRemovedAll, []Name{ObjectRemovedDelete, ObjectRemovedDeleteMarkerCreated, ObjectRemovedNoOP, ObjectRemovedDeleteAllVersions}},
		{ObjectAccessedHead, []Name{ObjectAccessedHead}},
	}

	for i, testCase := range testCases {
		result := testCase.name.Expand()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Errorf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNameString(t *testing.T) {
	var blankName Name

	testCases := []struct {
		name           Name
		expectedResult string
	}{
		{BucketCreated, "s3:BucketCreated:*"},
		{BucketRemoved, "s3:BucketRemoved:*"},
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
		{ObjectRemovedDeleteAllVersions, "s3:ObjectRemoved:DeleteAllVersions"},
		{ILMDelMarkerExpirationDelete, "s3:LifecycleDelMarkerExpiration:Delete"},
		{ObjectRemovedNoOP, "s3:ObjectRemoved:NoOP"},
		{ObjectCreatedPutRetention, "s3:ObjectCreated:PutRetention"},
		{ObjectCreatedPutLegalHold, "s3:ObjectCreated:PutLegalHold"},
		{ObjectAccessedGetRetention, "s3:ObjectAccessed:GetRetention"},
		{ObjectAccessedGetLegalHold, "s3:ObjectAccessed:GetLegalHold"},

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
		{ObjectRemovedNoOP, []byte("<Name>s3:ObjectRemoved:NoOP</Name>"), false},
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
		{[]byte("<Name>s3:ObjectRemoved:NoOP</Name>"), ObjectRemovedNoOP, false},
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
		{ObjectRemovedNoOP, []byte(`"s3:ObjectRemoved:NoOP"`), false},
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
		{[]byte(`"s3:ObjectRemoved:NoOP"`), ObjectRemovedNoOP, false},
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
		{"s3:ObjectRemoved:NoOP", ObjectRemovedNoOP, false},
		{"s3:LifecycleDelMarkerExpiration:Delete", ILMDelMarkerExpirationDelete, false},
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
