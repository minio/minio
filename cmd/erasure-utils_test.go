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

import (
	"crypto/rand"
	"testing"

	"github.com/minio/minio/pkg/bitrot"
)

// Tests validate the output of getChunkSize.
func TestGetChunkSize(t *testing.T) {
	// Refer to comments on getChunkSize() for details.
	testCases := []struct {
		blockSize  int64
		dataBlocks int
		// expected result.
		expectedChunkSize int64
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
	for i, testCase := range testCases {
		got := getChunkSize(testCase.blockSize, testCase.dataBlocks)
		if testCase.expectedChunkSize != got {
			t.Errorf("Test %d : expected=%d got=%d", i+1, testCase.expectedChunkSize, got)
		}
	}
}

// Checks whether all available algorithms can create implementation instances and
// ensures that the default bitrot algorithm is available.
func TestAvailableBitrotAlgorithms(t *testing.T) {
	for alg := bitrot.Algorithm(0); alg < bitrot.UnknownAlgorithm; alg++ {
		if alg.Available() {
			key, err := alg.GenerateKey(rand.Reader)
			if err != nil {
				t.Errorf("Algorithm %s: Failed to generate key", alg.String())
			}
			if _, err = alg.New(key, bitrot.Protect); err != nil {
				t.Errorf("Algorithm %s: failed to create instance with key", alg.String())
			}
		}
	}
	if !DefaultBitrotAlgorithm.Available() {
		t.Errorf("default bitrot algorithm %s is not available", DefaultBitrotAlgorithm)
	}
}
