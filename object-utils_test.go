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
	"testing"
)

//Validating bucket name.
func ensureBucketName(t *testing.T, name string, testNum int, pass bool) {
	isValidBucketName := IsValidBucketName(name)
	if pass && !isValidBucketName {
		t.Errorf("Test case %d: Expected \"%s\" to be a valid bucket name", testNum, name)
	}
	if !pass && isValidBucketName {
		t.Errorf("Test case %d: Expected bucket name \"%s\" to be invalid", testNum, name)
	}
}

func TestIsValidBucketName(t *testing.T) {
	testCases := []struct {
		bucketName string
		shouldPass bool
	}{
		//cases which should pass the test
		//passing in valid bucket names
		{"lol", true},
		{"1-this-is-valid", true},
		{"1-this-too-is-valid-1", true},
		{"this.works.too.1", true},
		{"1234567", true},
		{"123", true},
		{"s3-eu-west-1.amazonaws.com", true},
		{"ideas-are-more-powerful-than-guns", true},
		{"testbucket", true},
		{"1bucket", true},
		{"bucket1", true},
		//cases for which test should fail
		//passing invalid bucket names
		{"------", false},
		{"$this-is-not-valid-too", false},
		{"contains-$-dollar", false},
		{"contains-^-carrot", false},
		{"contains-$-dollar", false},
		{"contains-$-dollar", false},
		{"......", false},
		{"", false},
		{"a", false},
		{"ab", false},
		{".starts-with-a-dot", false},
		{"ends-with-a-dot.", false},
		{"ends-with-a-dash-", false},
		{"-starts-with-a-dash", false},
		{"THIS-BEINGS-WITH-UPPERCASe", false},
		{"tHIS-ENDS-WITH-UPPERCASE", false},
		{"ThisBeginsAndEndsWithUpperCase", false},
		{"una ñina", false},
		{"lalalallalallalalalallalallalala-theString-size-is-greater-than-64", false},
	}

	for i, testCase := range testCases {
		ensureBucketName(t, testCase.bucketName, i+1, testCase.shouldPass)
	}
}

//Test for validating object name.
func ensureObjectName(t *testing.T, name string, testNum int, pass bool) {
	isValidObjectName := IsValidObjectName(name)
	if pass && !isValidObjectName {
		t.Errorf("Test case %d: Expected \"%s\" to be a valid object name", testNum, name)
	}
	if !pass && isValidObjectName {
		t.Errorf("Test case %d: Expected object name \"%s\" to be invalid", testNum, name)
	}

}

func TestIsValidObjectName(t *testing.T) {
	testCases := []struct {
		objectName string
		shouldPass bool
	}{
		//cases which should pass the test
		//passing in valid object name
		{"object", true},
		{"The Shining Script <v1>.pdf", true},
		{"Cost Benefit Analysis (2009-2010).pptx", true},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", true},
		{"SHØRT", true},
		{"There are far too many object names, and far too few bucket names!", true},
		//cases for which test should fail
		//passing invalid object names
		{"", false},
		{string([]byte{0xff, 0xfe, 0xfd}), false},
	}

	for i, testCase := range testCases {
		ensureObjectName(t, testCase.objectName, i+1, testCase.shouldPass)
	}
}
