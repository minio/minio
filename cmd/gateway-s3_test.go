/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"testing"
)

// Tests extracting md5/sha256 bytes.
func TestGetMD5AndSha256Bytes(t *testing.T) {
	testCases := []struct {
		md5Hex    string
		sha256Hex string
		success   bool
	}{
		// Test 1: Hex encoding failure.
		{
			md5Hex:    "a",
			sha256Hex: "b",
			success:   false,
		},
		// Test 2: Hex encoding success.
		{
			md5Hex:    "91be0b892e47ede9de06aac14ca0369e",
			sha256Hex: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			success:   true,
		},
		// Test 3: hex values are empty should return success.
		{
			md5Hex:    "",
			sha256Hex: "",
			success:   true,
		},
	}
	for i, testCase := range testCases {
		_, _, err := getMD5AndSha256SumBytes(testCase.md5Hex, testCase.sha256Hex)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success, but got failure %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure, but got success", i+1)
		}
	}
}
