// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
