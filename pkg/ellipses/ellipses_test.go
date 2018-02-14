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

package ellipses

import (
	"fmt"
	"testing"
)

// Test tests args with ellipses.
func TestHasEllipses(t *testing.T) {
	testCases := []struct {
		args       []string
		expectedOk bool
	}{
		// Tests for all args without ellipses.
		{
			[]string{"64"},
			false,
		},
		// Found flower braces, still attempt to parse and throw an error.
		{
			[]string{"{1..64}"},
			true,
		},
		{
			[]string{"{1..2..}"},
			true,
		},
		// Test for valid input.
		{
			[]string{"1...64"},
			true,
		},
		{
			[]string{"{1...2O}"},
			true,
		},
		{
			[]string{"..."},
			true,
		},
		{
			[]string{"{-1...1}"},
			true,
		},
		{
			[]string{"{0...-1}"},
			true,
		},
		{
			[]string{"{1....4}"},
			true,
		},
		{
			[]string{"{1...64}"},
			true,
		},
		{
			[]string{"{...}"},
			true,
		},
		{
			[]string{"{1...64}", "{65...128}"},
			true,
		},
		{
			[]string{"http://minio{2...3}/export/set{1...64}"},
			true,
		},
		{
			[]string{
				"http://minio{2...3}/export/set{1...64}",
				"http://minio{2...3}/export/set{65...128}",
			},
			true,
		},
		{
			[]string{
				"mydisk-{a...z}{1...20}",
			},
			true,
		},
		{
			[]string{
				"mydisk-{1...4}{1..2.}",
			},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotOk := HasEllipses(testCase.args...)
			if gotOk != testCase.expectedOk {
				t.Errorf("Expected %t, got %t", testCase.expectedOk, gotOk)
			}
		})
	}
}

// Test tests find ellipses patterns.
func TestFindEllipsesPatterns(t *testing.T) {
	testCases := []struct {
		pattern       string
		success       bool
		expectedCount int
	}{
		// Tests for all invalid inputs
		{
			"{1..64}",
			false,
			0,
		},
		{
			"1...64",
			false,
			0,
		},
		{
			"...",
			false,
			0,
		},
		{
			"{1...",
			false,
			0,
		},
		{
			"...64}",
			false,
			0,
		},
		{
			"{...}",
			false,
			0,
		},
		{
			"{-1...1}",
			false,
			0,
		},
		{
			"{0...-1}",
			false,
			0,
		},
		{
			"{1...2O}",
			false,
			0,
		},
		{
			"{64...1}",
			false,
			0,
		},
		{
			"{1....4}",
			false,
			0,
		},
		{
			"mydisk-{a...z}{1...20}",
			false,
			0,
		},
		{
			"mydisk-{1...4}{1..2.}",
			false,
			0,
		},
		{
			"{1..2.}-mydisk-{1...4}",
			false,
			0,
		},
		{
			"{{1...4}}",
			false,
			0,
		},
		{
			"{4...02}",
			false,
			0,
		},
		// Test for valid input.
		{
			"{1...64}",
			true,
			64,
		},
		{
			"{1...64} {65...128}",
			true,
			4096,
		},
		{
			"{01...036}",
			true,
			36,
		},
		{
			"{001...036}",
			true,
			36,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			argP, err := FindEllipsesPatterns(testCase.pattern)
			if err != nil && testCase.success {
				t.Errorf("Expected success but failed instead %s", err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Expected failure but passed instead")
			}
			if err == nil {
				gotCount := len(argP.Expand())
				if gotCount != testCase.expectedCount {
					t.Errorf("Expected %d, got %d", testCase.expectedCount, gotCount)
				}
			}
		})
	}
}
