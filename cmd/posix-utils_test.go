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

package cmd

import "testing"

// Tests validate volume name.
func TestIsValidVolname(t *testing.T) {
	testCases := []struct {
		volName    string
		shouldPass bool
	}{
		// Cases which should pass the test.
		// passing in valid bucket names.
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
		{"$this-is-not-valid-too", true},
		{"contains-$-dollar", true},
		{"contains-^-carrot", true},
		{"contains-$-dollar", true},
		{"contains-$-dollar", true},
		{".starts-with-a-dot", true},
		{"ends-with-a-dot.", true},
		{"ends-with-a-dash-", true},
		{"-starts-with-a-dash", true},
		{"THIS-BEINGS-WITH-UPPERCASe", true},
		{"tHIS-ENDS-WITH-UPPERCASE", true},
		{"ThisBeginsAndEndsWithUpperCase", true},
		{"una ñina", true},
		{"lalalallalallalalalallalallalala-theString-size-is-greater-than-64", true},
		// cases for which test should fail.
		// passing invalid bucket names.
		{"", false},
		{"/", false},
		{"a", false},
		{"ab", false},
		{"ab/", true},
		{"......", true},
	}

	for i, testCase := range testCases {
		isValidVolname := isValidVolname(testCase.volName)
		if testCase.shouldPass && !isValidVolname {
			t.Errorf("Test case %d: Expected \"%s\" to be a valid bucket name", i+1, testCase.volName)
		}
		if !testCase.shouldPass && isValidVolname {
			t.Errorf("Test case %d: Expected bucket name \"%s\" to be invalid", i+1, testCase.volName)
		}
	}
}
