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

// Test for getChunkSize()
func TestGetChunkSize(t *testing.T) {
	// Refer to comments on getChunkSize() for details.
	testCases := []struct {
		blockSize  int64
		dataBlocks int
		chunkSize  int64
	}{
		{
			10,
			10,
			1,
		},
		{
			10,
			11,
			1,
		},
		{
			10,
			9,
			2,
		},
	}
	// Verify getChunkSize() for the test cases.
	for i, test := range testCases {
		expected := test.chunkSize
		got := getChunkSize(test.blockSize, test.dataBlocks)
		if expected != got {
			t.Errorf("Test %d : expected=%d got=%d", i+1, expected, got)
		}
	}
}
