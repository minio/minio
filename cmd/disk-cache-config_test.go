/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
func TestParseCacheExclude(t *testing.T) {
	testCases := []struct {
		excludeStr       string
		expectedPatterns []string
		success          bool
	}{
		// Empty input.
		{"", []string{}, true},
		// valid input
		{"/home/drive1;/home/drive2;/home/drive3", []string{}, false},
		{"bucket1/*;*.png;images/trip/barcelona/*", []string{"bucket1/*", "*.png", "images/trip/barcelona/*"}, true},
		{"bucket1", []string{"bucket1"}, true},
	}

	for i, testCase := range testCases {
		excludes, err := parseCacheExcludes(testCase.excludeStr)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if !reflect.DeepEqual(excludes, testCase.expectedPatterns) {
			t.Errorf("Expected %v, got %v", testCase.expectedPatterns, excludes)
		}
	}
}
