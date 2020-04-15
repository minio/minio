/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"reflect"
	"testing"
)

// Tests cache exclude parsing.
func TestParseGatewaySSE(t *testing.T) {
	testCases := []struct {
		gwSSEStr string
		expected gatewaySSE
		success  bool
	}{
		// valid input
		{"c;S3", []string{"C", "S3"}, true},
		{"S3", []string{"S3"}, true},
		{"c,S3", []string{}, false},
		{"c;S3;KMS", []string{}, false},
		{"C;s3", []string{"C", "S3"}, true},
	}

	for i, testCase := range testCases {
		gwSSE, err := parseGatewaySSE(testCase.gwSSEStr)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if err == nil {
			if !reflect.DeepEqual(gwSSE, testCase.expected) {
				t.Errorf("Test %d: Expected %v, got %v", i+1, testCase.expected, gwSSE)
			}
		}
	}
}
