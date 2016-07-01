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

import "testing"

// Tests validate the URL path encoder.
func TestUrlEncodePath(t *testing.T) {
	testCases := []struct {
		// Input.
		inputStr string
		// Expected result.
		result string
	}{
		// % should be encoded as %25
		{"thisisthe%url", "thisisthe%25url"},
		// UTF-8 encoding.
		{"本語", "%E6%9C%AC%E8%AA%9E"},
		// UTF-8 encoding with ASCII.
		{"本語.1", "%E6%9C%AC%E8%AA%9E.1"},
		// Unusual ASCII characters.
		{">123", "%3E123"},
		// Fragment path characters.
		{"myurl#link", "myurl%23link"},
		// Space should be set to %20 not '+'.
		{"space in url", "space%20in%20url"},
		// '+' shouldn't be treated as space.
		{"url+path", "url%2Bpath"},
	}

	// Tests generated values from url encoded name.
	for i, testCase := range testCases {
		result := getURLEncodedName(testCase.inputStr)
		if testCase.result != result {
			t.Errorf("Test %d: Expected queryEncode result to be \"%s\", but found it to be \"%s\" instead", i+1, testCase.result, result)
		}
	}
}
