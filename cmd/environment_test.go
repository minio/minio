// Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"testing"
)

// Tests create endpoints with ellipses and without.
func TestParseRateLimit(t *testing.T) {
	testCases := []struct {
		val           string
		expectedRate  float64
		expectedBurst int
		success       bool
	}{
		// Invalid input.
		{"", 0, 0, false},
		{"10.3", 10.3, 10, true},
		{"100:10", 100, 10, true},
		{"hello", 0, 0, false},
		{"100:[10]", 0, 0, false},
	}

	for i, testCase := range testCases {
		rateLim, burstLimit, err := parseRateLimit(testCase.val)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if rateLim != testCase.expectedRate {
			t.Errorf("Test %d: Expected rate to be %f but was %f", i, testCase.expectedRate, rateLim)
		}
		if burstLimit != testCase.expectedBurst {
			t.Errorf("Test %d: Expected rate to be %d but was %d", i, testCase.expectedBurst, burstLimit)
		}
	}
}
