/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package encoding

import (
	"math"
	"reflect"
	"testing"
)

func TestVarIntToBytes(t *testing.T) {
	testCases := []struct {
		ui64           uint64
		expectedResult []byte
	}{
		{0, []byte{0}},
		{1, []byte{1}},
		{0x7F, []byte{127}},
		{0x80, []byte{128, 1}},
		{uint64(math.MaxUint64), []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
	}

	for i, testCase := range testCases {
		result := varIntEncode(testCase.ui64)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
