/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import "testing"

func TestEvalSQLSubstring(t *testing.T) {
	evalCases := []struct {
		s           string
		startIdx    int
		length      int
		resExpected string
		errExpected error
	}{
		{"abcd", 1, 1, "a", nil},
		{"abcd", -1, 1, "a", nil},
		{"abcd", 999, 999, "", nil},
		{"", 999, 999, "", nil},
		{"测试abc", 1, 1, "测", nil},
		{"测试abc", 5, 5, "c", nil},
	}

	for i, tc := range evalCases {
		res, err := evalSQLSubstring(tc.s, tc.startIdx, tc.length)
		if res != tc.resExpected || err != tc.errExpected {
			t.Errorf("Eval Case %d failed: %v %v", i, res, err)
		}
	}
}
