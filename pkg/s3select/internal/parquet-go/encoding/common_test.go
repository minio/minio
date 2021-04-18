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
