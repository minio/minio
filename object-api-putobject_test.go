/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"bytes"
	"fmt"
	"testing"
)

// Wrapper for calling PutObject tests for both XL multiple disks and single node setup.
func TestObjectAPIPutObject(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIPutObject)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObject(obj ObjectLayer, instanceType string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket("unused-bucket")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	failCases := []struct {
		bucketName      string
		objName         string
		inputReaderData string
		inputMeta       map[string]string
		intputDataSize  int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Test case  1-4.
		// Cases with invalid bucket name.
		{".test", "obj", "", nil, 0, false, "", fmt.Errorf("%s", "Bucket name invalid: .test")},
		{"------", "obj", "", nil, 0, false, "", fmt.Errorf("%s", "Bucket name invalid: ------")},
		{"$this-is-not-valid-too", "obj", "", nil, 0, false, "",
			fmt.Errorf("%s", "Bucket name invalid: $this-is-not-valid-too")},
		{"a", "obj", "", nil, 0, false, "", fmt.Errorf("%s", "Bucket name invalid: a")},
		// Test case - 5.
		// Case with invalid object names.
		{bucket, "", "", nil, 0, false, "", fmt.Errorf("%s", "Object name invalid: minio-bucket#")},
		// Test case - 6.
		// Valid object and bucket names but non-existent bucket.
		{"abc", "def", "", nil, 0, false, "", fmt.Errorf("%s", "Bucket not found: abc")},
		// Test case - 7.
		// Input to replicate Md5 mismatch.
		{bucket, object, "", map[string]string{"md5Sum": "a35"}, 0, false, "",
			fmt.Errorf("%s", "Bad digest: Expected a35 is not valid with what we calculated "+"d41d8cd98f00b204e9800998ecf8427e")},
		// Test case - 8.
		// Input with size more than the size of actual data inside the reader.
		{bucket, object, "abcd", map[string]string{"md5Sum": "a35"}, int64(len("abcd") + 1), false, "",
			fmt.Errorf("%s", "Bad digest: Expected a35 is not valid with what we calculated e2fc714c4727ee9395f324cd2e7f331f")},
		// Test case - 9.
		// Input with size less than the size of actual data inside the reader.
		{bucket, object, "abcd", map[string]string{"md5Sum": "a35"}, int64(len("abcd") - 1), false, "",
			fmt.Errorf("%s", "Bad digest: Expected a35 is not valid with what we calculated e2fc714c4727ee9395f324cd2e7f331f")},
		// Test case - 10-13.
		// Validating for success cases.
		{bucket, object, "abcd", map[string]string{"md5Sum": "e2fc714c4727ee9395f324cd2e7f331f"}, int64(len("abcd")), true, "", nil},
		{bucket, object, "efgh", map[string]string{"md5Sum": "1f7690ebdd9b4caf8fab49ca1757bf27"}, int64(len("efgh")), true, "", nil},
		{bucket, object, "ijkl", map[string]string{"md5Sum": "09a0877d04abf8759f99adec02baf579"}, int64(len("ijkl")), true, "", nil},
		{bucket, object, "mnop", map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"}, int64(len("mnop")), true, "", nil},
	}

	for i, testCase := range failCases {
		actualMd5Hex, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewBufferString(testCase.inputReaderData), testCase.inputMeta)
		// All are test cases above are expected to fail.

		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedError.Error() != actualErr.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1,
					instanceType, testCase.expectedError.Error(), actualErr.Error())
			}
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil && testCase.shouldPass {
			// Asserting whether the md5 output is correct.
			if testCase.inputMeta["md5Sum"] != actualMd5Hex {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualMd5Hex)
			}
		}
	}
}

// Wrapper for calling PutObject tests for both XL multiple disks case
// when quorum is not available.
func TestObjectAPIPutObjectDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskNotFoundTest(t, testObjectAPIPutObjectDiskNotFOund)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObjectDiskNotFOund(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket("unused-bucket")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Take 6 disks down, one more we loose quorum on 16 disk node.
	for _, disk := range disks[:6] {
		removeAll(disk)
	}

	testCases := []struct {
		bucketName      string
		objName         string
		inputReaderData string
		inputMeta       map[string]string
		intputDataSize  int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Validating for success cases.
		{bucket, object, "abcd", map[string]string{"md5Sum": "e2fc714c4727ee9395f324cd2e7f331f"}, int64(len("abcd")), true, "", nil},
		{bucket, object, "efgh", map[string]string{"md5Sum": "1f7690ebdd9b4caf8fab49ca1757bf27"}, int64(len("efgh")), true, "", nil},
		{bucket, object, "ijkl", map[string]string{"md5Sum": "09a0877d04abf8759f99adec02baf579"}, int64(len("ijkl")), true, "", nil},
		{bucket, object, "mnop", map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"}, int64(len("mnop")), true, "", nil},
	}

	for i, testCase := range testCases {
		actualMd5Hex, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewBufferString(testCase.inputReaderData), testCase.inputMeta)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedError.Error() != actualErr.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1,
					instanceType, testCase.expectedError.Error(), actualErr.Error())
			}
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil && testCase.shouldPass {
			// Asserting whether the md5 output is correct.
			if testCase.inputMeta["md5Sum"] != actualMd5Hex {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualMd5Hex)
			}
		}
	}

	// This causes quorum failure verify.
	removeAll(disks[len(disks)-1])

	// Validate the last test.
	testCase := struct {
		bucketName      string
		objName         string
		inputReaderData string
		inputMeta       map[string]string
		intputDataSize  int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		bucket,
		object,
		"mnop",
		map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"},
		int64(len("mnop")),
		false,
		"",
		InsufficientWriteQuorum{},
	}
	_, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewBufferString(testCase.inputReaderData), testCase.inputMeta)
	if actualErr != nil && testCase.shouldPass {
		t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", len(testCases)+1, instanceType, actualErr.Error())
	}
	// Failed as expected, but does it fail for the expected reason.
	if actualErr != nil && !testCase.shouldPass {
		if testCase.expectedError.Error() != actualErr.Error() {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", len(testCases)+1, instanceType, testCase.expectedError.Error(), actualErr.Error())
		}
	}
}
