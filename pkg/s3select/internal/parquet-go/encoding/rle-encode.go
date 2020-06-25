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
	"fmt"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func rleEncodeInt32s(i32s []int32, bitWidth int32) (data []byte) {
	j := 0
	for i := 0; i < len(i32s); i = j {
		for j = i + 1; j < len(i32s) && i32s[i] == i32s[j]; j++ {
		}

		headerBytes := varIntEncode(uint64((j - i) << 1))
		data = append(data, headerBytes...)

		valBytes := plainEncodeInt32s([]int32{i32s[i]})
		byteCount := (bitWidth + 7) / 8
		data = append(data, valBytes[:byteCount]...)
	}

	return data
}

func rleEncodeInt64s(i64s []int64, bitWidth int32) (data []byte) {
	j := 0
	for i := 0; i < len(i64s); i = j {
		for j = i + 1; j < len(i64s) && i64s[i] == i64s[j]; j++ {
		}

		headerBytes := varIntEncode(uint64((j - i) << 1))
		data = append(data, headerBytes...)

		valBytes := plainEncodeInt64s([]int64{i64s[i]})
		byteCount := (bitWidth + 7) / 8
		data = append(data, valBytes[:byteCount]...)
	}

	return data
}

// RLEBitPackedHybridEncode encodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
//
// Supported Types: INT32, INT64
func RLEBitPackedHybridEncode(values interface{}, bitWidth int32, parquetType parquet.Type) []byte {
	var rleBytes []byte

	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}
		rleBytes = rleEncodeInt32s(i32s, bitWidth)
	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}
		rleBytes = rleEncodeInt64s(i64s, bitWidth)
	default:
		panic(fmt.Errorf("%v parquet type unsupported", parquetType))
	}

	lenBytes := plainEncodeInt32s([]int32{int32(len(rleBytes))})
	return append(lenBytes, rleBytes...)
}
