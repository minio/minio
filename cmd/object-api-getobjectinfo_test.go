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

package cmd

import (
	"bytes"
	"context"
	"testing"
)

// Wrapper for calling GetObjectInfo tests for both Erasure multiple disks and single node setup.
func TestGetObjectInfo(t *testing.T) {
	ExecObjectLayerTest(t, testGetObjectInfo)
}

// Testing GetObjectInfo().
func testGetObjectInfo(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// This bucket is used for testing getObjectInfo operations.
	err := obj.MakeBucket(context.Background(), "test-getobjectinfo", MakeBucketOptions{})
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	opts := ObjectOptions{}
	_, err = obj.PutObject(context.Background(), "test-getobjectinfo", "Asia/asiapics.jpg", mustGetPutObjReader(t, bytes.NewBufferString("asiapics"), int64(len("asiapics")), "", ""), opts)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Put an empty directory
	_, err = obj.PutObject(context.Background(), "test-getobjectinfo", "Asia/empty-dir/", mustGetPutObjReader(t, bytes.NewBufferString(""), int64(len("")), "", ""), opts)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	resultCases := []ObjectInfo{
		// ObjectInfo -1.
		// ObjectName set to a existing object in the test case (Test case 14).
		{Bucket: "test-getobjectinfo", Name: "Asia/asiapics.jpg", ContentType: "image/jpeg", IsDir: false},
		{Bucket: "test-getobjectinfo", Name: "Asia/empty-dir/", ContentType: "application/octet-stream", IsDir: true},
	}
	testCases := []struct {
		bucketName string
		objectName string

		// Expected output of GetObjectInfo.
		result ObjectInfo
		err    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4 ).
		{".test", "", ObjectInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"---", "", ObjectInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", ObjectInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases with valid but non-existing bucket names (Test number 5-6).
		{"abcdefgh", "abc", ObjectInfo{}, BucketNotFound{Bucket: "abcdefgh"}, false},
		{"ijklmnop", "efg", ObjectInfo{}, BucketNotFound{Bucket: "ijklmnop"}, false},
		// Test cases with valid but non-existing bucket names and invalid object name (Test number 7-8).
		{"test-getobjectinfo", "", ObjectInfo{}, ObjectNameInvalid{Bucket: "test-getobjectinfo", Object: ""}, false},
		{"test-getobjectinfo", "", ObjectInfo{}, ObjectNameInvalid{Bucket: "test-getobjectinfo", Object: ""}, false},
		// Test cases with non-existing object name with existing bucket (Test number 9-11).
		{"test-getobjectinfo", "Africa", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Africa"}, false},
		{"test-getobjectinfo", "Antartica", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Antartica"}, false},
		{"test-getobjectinfo", "Asia/myfile", ObjectInfo{}, ObjectNotFound{Bucket: "test-getobjectinfo", Object: "Asia/myfile"}, false},
		// Valid case with existing object (Test number 12).
		{"test-getobjectinfo", "Asia/asiapics.jpg", resultCases[0], nil, true},
		{"test-getobjectinfo", "Asia/empty-dir/", resultCases[1], nil, true},
	}
	for i, testCase := range testCases {
		result, err := obj.GetObjectInfo(context.Background(), testCase.bucketName, testCase.objectName, opts)
		if err != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
		}
		if err == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.err.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if err != nil && !testCase.shouldPass {
			if testCase.err.Error() != err.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.err.Error(), err.Error())
			}
		}

		// Test passes as expected, but the output values are verified for correctness here.
		if err == nil && testCase.shouldPass {
			if testCase.result.Bucket != result.Bucket {
				t.Fatalf("Test %d: %s: Expected Bucket name to be '%s', but found '%s' instead", i+1, instanceType, testCase.result.Bucket, result.Bucket)
			}
			if testCase.result.Name != result.Name {
				t.Errorf("Test %d: %s: Expected Object name to be %s, but instead found it to be %s", i+1, instanceType, testCase.result.Name, result.Name)
			}
			if testCase.result.ContentType != result.ContentType {
				t.Errorf("Test %d: %s: Expected Content Type of the object to be %v, but instead found it to be %v", i+1, instanceType, testCase.result.ContentType, result.ContentType)
			}
			if testCase.result.IsDir != result.IsDir {
				t.Errorf("Test %d: %s: Expected IsDir flag of the object to be %v, but instead found it to be %v", i+1, instanceType, testCase.result.IsDir, result.IsDir)
			}
		}
	}
}
