/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package compress

import (
	"reflect"
	"testing"
)

func TestParseCompressIncludes(t *testing.T) {
	testCases := []struct {
		str              string
		expectedPatterns []string
		success          bool
	}{
		// invalid input
		{",,,", []string{}, false},
		{"", []string{}, false},
		{",", []string{}, false},
		{"/", []string{}, false},
		{"text/*,/", []string{}, false},

		// valid input
		{".txt,.log", []string{".txt", ".log"}, true},
		{"text/*,application/json", []string{"text/*", "application/json"}, true},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.str, func(t *testing.T) {
			gotPatterns, err := parseCompressIncludes(testCase.str)
			if !testCase.success && err == nil {
				t.Error("expected failure but success instead")
			}
			if testCase.success && err != nil {
				t.Errorf("expected success but failed instead %s", err)
			}
			if testCase.success && !reflect.DeepEqual(testCase.expectedPatterns, gotPatterns) {
				t.Errorf("expected patterns %s but got %s", testCase.expectedPatterns, gotPatterns)
			}
		})
	}
}
