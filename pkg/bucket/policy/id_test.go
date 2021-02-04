/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package policy

import (
	"testing"
)

func TestIDIsValid(t *testing.T) {
	testCases := []struct {
		id             ID
		expectedResult bool
	}{
		{ID("DenyEncryptionSt1"), true},
		{ID(""), true},
		{ID("aa\xe2"), false},
	}

	for i, testCase := range testCases {
		result := testCase.id.IsValid()

		if result != testCase.expectedResult {
			t.Errorf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}
