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
