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
