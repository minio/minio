/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"fmt"
	"testing"
)

func TestS3EncodeName(t *testing.T) {
	testCases := []struct {
		inputText, encodingType, expectedOutput string
	}{
		{"a b", "", "a b"},
		{"a b", "url", "a+b"},
		{"p- ", "url", "p-+"},
		{"p-%", "url", "p-%25"},
		{"p/", "url", "p/"},
		{"p/", "url", "p/"},
		{"~user", "url", "%7Euser"},
		{"*user", "url", "*user"},
		{"user+password", "url", "user%2Bpassword"},
		{"_user", "url", "_user"},
		{"firstname.lastname", "url", "firstname.lastname"},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			outputText := s3EncodeName(testCase.inputText, testCase.encodingType)
			if testCase.expectedOutput != outputText {
				t.Errorf("Expected `%s`, got `%s`", testCase.expectedOutput, outputText)
			}

		})
	}
}
