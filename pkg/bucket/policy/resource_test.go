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
)

func TestResourceIsBucketPattern(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("mybucket?0", ""), true},
		{NewResource("", "*"), false},
		{NewResource("*", "*"), false},
		{NewResource("mybucket", "*"), false},
		{NewResource("mybucket*", "/myobject"), false},
		{NewResource("mybucket?0", "/2010/photos/*"), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.isBucketPattern()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceIsObjectPattern(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("", "*"), true},
		{NewResource("*", "*"), true},
		{NewResource("mybucket", "*"), true},
		{NewResource("mybucket*", "/myobject"), true},
		{NewResource("mybucket?0", "/2010/photos/*"), true},
		{NewResource("mybucket", ""), false},
		{NewResource("mybucket?0", ""), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.isObjectPattern()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceIsValid(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult bool
	}{
		{NewResource("*", ""), true},
		{NewResource("mybucket*", ""), true},
		{NewResource("*", "*"), true},
		{NewResource("mybucket", "*"), true},
		{NewResource("mybucket*", "/myobject"), true},
		{NewResource("mybucket?0", "/2010/photos/*"), true},
		{NewResource("mybucket", ""), true},
		{NewResource("mybucket?0", ""), true},
		{NewResource("", ""), false},
		{NewResource("", "*"), false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.IsValid()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceMatch(t *testing.T) {
	testCases := []struct {
		resource       Resource
		objectName     string
		expectedResult bool
	}{
		{NewResource("*", ""), "mybucket", true},
		{NewResource("*", ""), "mybucket/myobject", true},
		{NewResource("mybucket*", ""), "mybucket", true},
		{NewResource("mybucket*", ""), "mybucket/myobject", true},
		{NewResource("", "*"), "/myobject", true},
		{NewResource("*", "*"), "mybucket/myobject", true},
		{NewResource("mybucket", "*"), "mybucket/myobject", true},
		{NewResource("mybucket*", "/myobject"), "mybucket/myobject", true},
		{NewResource("mybucket*", "/myobject"), "mybucket100/myobject", true},
		{NewResource("mybucket?0", "/2010/photos/*"), "mybucket20/2010/photos/1.jpg", true},
		{NewResource("mybucket", ""), "mybucket", true},
		{NewResource("mybucket?0", ""), "mybucket30", true},
		{NewResource("", "*"), "mybucket/myobject", false},
		{NewResource("*", "*"), "mybucket", false},
		{NewResource("mybucket", "*"), "mybucket10/myobject", false},
		{NewResource("mybucket?0", "/2010/photos/*"), "mybucket0/2010/photos/1.jpg", false},
		{NewResource("mybucket", ""), "mybucket/myobject", false},
	}

	for i, testCase := range testCases {
		result := testCase.resource.Match(testCase.objectName, nil)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceMarshalJSON(t *testing.T) {
	testCases := []struct {
		resource       Resource
		expectedResult []byte
		expectErr      bool
	}{
		{NewResource("*", ""), []byte(`"arn:aws:s3:::*"`), false},
		{NewResource("mybucket*", ""), []byte(`"arn:aws:s3:::mybucket*"`), false},
		{NewResource("mybucket", ""), []byte(`"arn:aws:s3:::mybucket"`), false},
		{NewResource("*", "*"), []byte(`"arn:aws:s3:::*/*"`), false},
		{NewResource("mybucket", "*"), []byte(`"arn:aws:s3:::mybucket/*"`), false},
		{NewResource("mybucket*", "myobject"), []byte(`"arn:aws:s3:::mybucket*/myobject"`), false},
		{NewResource("mybucket?0", "/2010/photos/*"), []byte(`"arn:aws:s3:::mybucket?0/2010/photos/*"`), false},
		{Resource{}, nil, true},
		{NewResource("", "*"), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.resource)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestResourceUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult Resource
		expectErr      bool
	}{
		{[]byte(`"arn:aws:s3:::*"`), NewResource("*", ""), false},
		{[]byte(`"arn:aws:s3:::mybucket*"`), NewResource("mybucket*", ""), false},
		{[]byte(`"arn:aws:s3:::mybucket"`), NewResource("mybucket", ""), false},
		{[]byte(`"arn:aws:s3:::*/*"`), NewResource("*", "*"), false},
		{[]byte(`"arn:aws:s3:::mybucket/*"`), NewResource("mybucket", "*"), false},
		{[]byte(`"arn:aws:s3:::mybucket*/myobject"`), NewResource("mybucket*", "myobject"), false},
		{[]byte(`"arn:aws:s3:::mybucket?0/2010/photos/*"`), NewResource("mybucket?0", "/2010/photos/*"), false},
		{[]byte(`"mybucket/myobject*"`), Resource{}, true},
		{[]byte(`"arn:aws:s3:::/*"`), Resource{}, true},
	}

	for i, testCase := range testCases {
		var result Resource
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestResourceValidate(t *testing.T) {
	testCases := []struct {
		resource   Resource
		bucketName string
		expectErr  bool
	}{
		{NewResource("mybucket", "/myobject*"), "mybucket", false},
		{NewResource("", "/myobject*"), "yourbucket", true},
		{NewResource("mybucket", "/myobject*"), "yourbucket", true},
	}

	for i, testCase := range testCases {
		err := testCase.resource.Validate(testCase.bucketName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
