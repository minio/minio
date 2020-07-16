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
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func TestRLEEncodeInt32s(t *testing.T) {
	testCases := []struct {
		values         []int32
		bitWidth       int32
		dataType       parquet.Type
		expectedResult []byte
	}{
		{[]int32{3, 5, 7}, 1, parquet.Type_INT32, []byte{2, 3, 2, 5, 2, 7}},
		{[]int32{3, 3, 3}, 1, parquet.Type_INT32, []byte{6, 3}},
		{[]int32{2, 2, 3, 3, 3}, 1, parquet.Type_INT32, []byte{4, 2, 6, 3}},
	}

	for i, testCase := range testCases {
		result := rleEncodeInt32s(testCase.values, testCase.bitWidth)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
