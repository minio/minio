/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"bytes"
	"testing"
)

// Wrapper for calling GetObjectInfo tests for both XL multiple disks and single node setup.
func TestGetObjectInfo(t *testing.T) {
	ExecObjectLayerTest(t, testGetObjectInfo)
}

// Testing GetObjectInfo().
func testGetObjectInfo(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// This bucket is used for testing getObjectInfo operations.
	err := obj.MakeBucketWithLocation("test-getobjectinfo", "")
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	_, err = obj.PutObject("test-getobjectinfo", "Asia/asiapics.jpg", mustGetHashReader(t, bytes.NewBufferString("asiapics"), int64(len("asiapics")), "", ""), nil)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	resultCases := []ObjectInfo{
		// ObjectInfo -1.
		// ObjectName set to a existing object in the test case (Test case 14).
		{Bucket: "test-getobjectinfo", Name: "Asia/asiapics.jpg", ContentType: "image/jpeg", IsDir: false},
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
		{"Test", "", ObjectInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
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
	}
	for i, testCase := range testCases {
		result, err := obj.GetObjectInfo(testCase.bucketName, testCase.objectName)
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
