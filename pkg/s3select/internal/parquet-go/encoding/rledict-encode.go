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
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/common"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

// RLEDictEncode encodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8 and returns dictionary page data and data page data.
//
// Dictionary page data contains PLAIN encodeed slice of uniquely fully defined non-nil values.
// Data page data contains RLE/Bit-Packed Hybrid encoded indices of fully defined non-nil values.
//
// Supported Types: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY
func RLEDictEncode(values []interface{}, parquetType parquet.Type, bitWidth int32) (dictPageData, dataPageData []byte, dictValueCount int32, indexBitWidth uint8) {
	var definedValues []interface{}
	var indices []int32

	valueIndexMap := make(map[interface{}]int32)
	j := 0
	for i := 0; i < len(values); i = j {
		for j = i; j < len(values); j++ {
			value := values[j]
			if value == nil {
				continue
			}

			index, found := valueIndexMap[value]
			if !found {
				index = int32(len(definedValues))
				definedValues = append(definedValues, value)
				valueIndexMap[value] = index
			}

			indices = append(indices, index)
		}
	}

	indexBitWidth = uint8(common.BitWidth(uint64(indices[len(indices)-1])))

	dictPageData = PlainEncode(common.ToSliceValue(definedValues, parquetType), parquetType)
	dataPageData = RLEBitPackedHybridEncode(indices, int32(indexBitWidth), parquet.Type_INT32)

	return dictPageData, dataPageData, int32(len(definedValues)), indexBitWidth
}
