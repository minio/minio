/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"fmt"
	"testing"
)

// Tests validate Bucket policy resource matcher.
func TestBucketPolicyResourceMatch(t *testing.T) {

	// generates\ statement with given resource..
	generateStatement := func(resource string) policyStatement {
		statement := policyStatement{}
		statement.Resources = []string{resource}
		return statement
	}

	// generates resource prefix.
	generateResource := func(bucketName, objectName string) string {
		return AWSResourcePrefix + bucketName + "/" + objectName
	}

	testCases := []struct {
		resourceToMatch       string
		statement             policyStatement
		expectedResourceMatch bool
	}{
		// Test case 1-4.
		// Policy with resource ending with bucket/* allows access to all objects inside the given bucket.
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/*")), true},
		// Test case - 5.
		// Policy with resource ending with bucket/oo* should not allow access to bucket/output.txt.
		{generateResource("minio-bucket", "output.txt"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/oo*")), false},
		// Test case - 6.
		// Policy with resource ending with bucket/oo* should allow access to bucket/ootput.txt.
		{generateResource("minio-bucket", "ootput.txt"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/oo*")), true},
		// Test case - 7.
		// Policy with resource ending with bucket/oo* allows access to all subfolders starting with "oo" inside given bucket.
		{generateResource("minio-bucket", "oop-bucket/my-file"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/oo*")), true},
		// Test case - 8.
		{generateResource("minio-bucket", "Asia/India/1.pjg"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/Asia/Japan/*")), false},
		// Test case - 9.
		{generateResource("minio-bucket", "Asia/India/1.pjg"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix, "minio-bucket"+"/Asia/Japan/*")), false},
		// Test case - 10.
		// Proves that the name space is flat.
		{generateResource("minio-bucket", "Africa/Bihar/India/design_info.doc/Bihar"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix,
			"minio-bucket"+"/*/India/*/Bihar")), true},
		// Test case - 11.
		// Proves that the name space is flat.
		{generateResource("minio-bucket", "Asia/China/India/States/Bihar/output.txt"), generateStatement(fmt.Sprintf("%s%s", AWSResourcePrefix,
			"minio-bucket"+"/*/India/*/Bihar/*")), true},
	}
	for i, testCase := range testCases {
		actualResourceMatch := bucketPolicyResourceMatch(testCase.resourceToMatch, testCase.statement)
		if testCase.expectedResourceMatch != actualResourceMatch {
			t.Errorf("Test %d: Expected Resource match to be `%v`, but instead found it to be `%v`", i+1, testCase.expectedResourceMatch, actualResourceMatch)
		}
	}
}
