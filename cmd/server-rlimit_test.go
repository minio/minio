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

import "testing"

func TestGetMaxCacheSize(t *testing.T) {
	testCases := []struct {
		curLimit       uint64
		totalRAM       uint64
		expectedResult uint64
	}{
		{uint64(0), uint64(0), uint64(0)},
		{minRAMSize, uint64(0), uint64(0)},
		{uint64(0), minRAMSize, uint64(0)},
		{uint64(18446744073709551615), uint64(8115998720), uint64(0)},
		{uint64(8115998720), uint64(16115998720), uint64(0)},
		{minRAMSize, minRAMSize, uint64(12884901888)},
		{minRAMSize, uint64(16115998720), uint64(0)},
		{uint64(18446744073709551615), uint64(10115998720), uint64(0)},
	}

	for i, testCase := range testCases {
		cacheSize := getMaxCacheSize(testCase.curLimit, testCase.totalRAM)
		if testCase.expectedResult != cacheSize {
			t.Fatalf("Test %d, Expected: %v, Got: %v", i+1, testCase.expectedResult, cacheSize)
		}
	}
}
