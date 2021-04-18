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

func TestResourceSetBucketResourceExists(t *testing.T) {
	testCases := []struct {
		resourceSet    ResourceSet
		expectedResult bool
	}{
		{NewResourceSet(NewResource("*", "")), true},
		{NewResourceSet(NewResource("mybucket", "")), true},
		{NewResourceSet(NewResource("mybucket*", "")), true},
		{NewResourceSet(NewResource("mybucket?0", "")), true},
		{NewResourceSet(NewResource("mybucket", "/2010/photos/*"), NewResource("mybucket", "")), true},
		{NewResourceSet(NewResource("", "*")), false},
		{NewResourceSet(NewResource("*", "*")), false},
		{NewResourceSet(NewResource("mybucket", "*")), false},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), false},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*")), false},
	}

	for i, testCase := range testCases {
		result := testCase.resourceSet.bucketResourceExists()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceSetObjectResourceExists(t *testing.T) {
	testCases := []struct {
		resourceSet    ResourceSet
		expectedResult bool
	}{
		{NewResourceSet(NewResource("*", "")), true},
		{NewResourceSet(NewResource("mybucket*", "")), true},
		{NewResourceSet(NewResource("", "*")), true},
		{NewResourceSet(NewResource("*", "*")), true},
		{NewResourceSet(NewResource("mybucket", "*")), true},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), true},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*")), true},
		{NewResourceSet(NewResource("mybucket", ""), NewResource("mybucket", "/2910/photos/*")), true},
		{NewResourceSet(NewResource("mybucket", "")), false},
		{NewResourceSet(NewResource("mybucket?0", "")), false},
	}

	for i, testCase := range testCases {
		result := testCase.resourceSet.objectResourceExists()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceSetAdd(t *testing.T) {
	testCases := []struct {
		resourceSet    ResourceSet
		resource       Resource
		expectedResult ResourceSet
	}{
		{NewResourceSet(), NewResource("mybucket", "/myobject*"),
			NewResourceSet(NewResource("mybucket", "/myobject*"))},
		{NewResourceSet(NewResource("mybucket", "/myobject*")),
			NewResource("mybucket", "/yourobject*"),
			NewResourceSet(NewResource("mybucket", "/myobject*"),
				NewResource("mybucket", "/yourobject*"))},
		{NewResourceSet(NewResource("mybucket", "/myobject*")),
			NewResource("mybucket", "/myobject*"),
			NewResourceSet(NewResource("mybucket", "/myobject*"))},
	}

	for i, testCase := range testCases {
		testCase.resourceSet.Add(testCase.resource)

		if !reflect.DeepEqual(testCase.resourceSet, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, testCase.resourceSet)
		}
	}
}

func TestResourceSetIntersection(t *testing.T) {
	testCases := []struct {
		set            ResourceSet
		setToIntersect ResourceSet
		expectedResult ResourceSet
	}{
		{NewResourceSet(), NewResourceSet(NewResource("mybucket", "/myobject*")), NewResourceSet()},
		{NewResourceSet(NewResource("mybucket", "/myobject*")), NewResourceSet(), NewResourceSet()},
		{NewResourceSet(NewResource("mybucket", "/myobject*")),
			NewResourceSet(NewResource("mybucket", "/myobject*"), NewResource("mybucket", "/yourobject*")),
			NewResourceSet(NewResource("mybucket", "/myobject*"))},
	}

	for i, testCase := range testCases {
		result := testCase.set.Intersection(testCase.setToIntersect)

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, testCase.set)
		}
	}
}

func TestResourceSetMarshalJSON(t *testing.T) {
	testCases := []struct {
		resoruceSet    ResourceSet
		expectedResult []byte
		expectErr      bool
	}{
		{NewResourceSet(NewResource("mybucket", "/myobject*")),
			[]byte(`["arn:aws:s3:::mybucket/myobject*"]`), false},
		{NewResourceSet(NewResource("mybucket", "/photos/myobject*")),
			[]byte(`["arn:aws:s3:::mybucket/photos/myobject*"]`), false},
		{NewResourceSet(), nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.resoruceSet)
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

func TestResourceSetMatch(t *testing.T) {
	testCases := []struct {
		resourceSet    ResourceSet
		resource       string
		expectedResult bool
	}{
		{NewResourceSet(NewResource("*", "")), "mybucket", true},
		{NewResourceSet(NewResource("*", "")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "")), "mybucket", true},
		{NewResourceSet(NewResource("mybucket*", "")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("", "*")), "/myobject", true},
		{NewResourceSet(NewResource("*", "*")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket", "*")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), "mybucket/myobject", true},
		{NewResourceSet(NewResource("mybucket*", "/myobject")), "mybucket100/myobject", true},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*")), "mybucket20/2010/photos/1.jpg", true},
		{NewResourceSet(NewResource("mybucket", "")), "mybucket", true},
		{NewResourceSet(NewResource("mybucket?0", "")), "mybucket30", true},
		{NewResourceSet(NewResource("mybucket?0", "/2010/photos/*"),
			NewResource("mybucket", "/2010/photos/*")), "mybucket/2010/photos/1.jpg", true},
		{NewResourceSet(NewResource("", "*")), "mybucket/myobject", false},
		{NewResourceSet(NewResource("*", "*")), "mybucket", false},
		{NewResourceSet(NewResource("mybucket", "*")), "mybucket10/myobject", false},
		{NewResourceSet(NewResource("mybucket", "")), "mybucket/myobject", false},
		{NewResourceSet(), "mybucket/myobject", false},
	}

	for i, testCase := range testCases {
		result := testCase.resourceSet.Match(testCase.resource, nil)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestResourceSetUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		data           []byte
		expectedResult ResourceSet
		expectErr      bool
	}{
		{[]byte(`"arn:aws:s3:::mybucket/myobject*"`),
			NewResourceSet(NewResource("mybucket", "/myobject*")), false},
		{[]byte(`"arn:aws:s3:::mybucket/photos/myobject*"`),
			NewResourceSet(NewResource("mybucket", "/photos/myobject*")), false},
		{[]byte(`"arn:aws:s3:::mybucket"`), NewResourceSet(NewResource("mybucket", "")), false},
		{[]byte(`"mybucket/myobject*"`), nil, true},
	}

	for i, testCase := range testCases {
		var result ResourceSet
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

func TestResourceSetValidate(t *testing.T) {
	testCases := []struct {
		resourceSet ResourceSet
		bucketName  string
		expectErr   bool
	}{
		{NewResourceSet(NewResource("mybucket", "/myobject*")), "mybucket", false},
		{NewResourceSet(NewResource("", "/myobject*")), "yourbucket", true},
		{NewResourceSet(NewResource("mybucket", "/myobject*")), "yourbucket", true},
	}

	for i, testCase := range testCases {
		err := testCase.resourceSet.Validate(testCase.bucketName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
