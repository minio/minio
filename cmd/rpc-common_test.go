/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, semVersion 2.0 (the "License");
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

// Tests version comparator.
func TestCompare(t *testing.T) {
	type compareTest struct {
		v1     semVersion
		v2     semVersion
		result int
	}

	var compareTests = []compareTest{
		{semVersion{1, 0, 0}, semVersion{1, 0, 0}, 0},
		{semVersion{2, 0, 0}, semVersion{1, 0, 0}, 1},
		{semVersion{0, 1, 0}, semVersion{0, 1, 0}, 0},
		{semVersion{0, 2, 0}, semVersion{0, 1, 0}, 1},
		{semVersion{0, 0, 1}, semVersion{0, 0, 1}, 0},
		{semVersion{0, 0, 2}, semVersion{0, 0, 1}, 1},
		{semVersion{1, 2, 3}, semVersion{1, 2, 3}, 0},
		{semVersion{2, 2, 4}, semVersion{1, 2, 4}, 1},
		{semVersion{1, 3, 3}, semVersion{1, 2, 3}, 1},
		{semVersion{1, 2, 4}, semVersion{1, 2, 3}, 1},

		// Spec Examples #11
		{semVersion{1, 0, 0}, semVersion{2, 0, 0}, -1},
		{semVersion{2, 0, 0}, semVersion{2, 1, 0}, -1},
		{semVersion{2, 1, 0}, semVersion{2, 1, 1}, -1},
	}

	for _, test := range compareTests {
		if res := test.v1.Compare(test.v2); res != test.result {
			t.Errorf("Comparing %q : %q, expected %d but got %d", test.v1, test.v2, test.result, res)
		}
		// Test if reverse is true as well.
		if res := test.v2.Compare(test.v1); res != -test.result {
			t.Errorf("Comparing %q : %q, expected %d but got %d", test.v2, test.v1, -test.result, res)
		}
	}
}
