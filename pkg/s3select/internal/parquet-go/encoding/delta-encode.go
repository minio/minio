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

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/common"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

const (
	blockSize      = 128
	miniBlockSize  = 32
	miniBlockCount = blockSize / miniBlockSize
)

var deltaEncodeHeaderBytes []byte

func init() {
	deltaEncodeHeaderBytes = varIntEncode(blockSize)
	deltaEncodeHeaderBytes = append(deltaEncodeHeaderBytes, varIntEncode(miniBlockCount)...)
}

// Supported Types: BOOLEAN, INT32, INT64
func bitPackedEncode(values interface{}, bitWidth uint64, withHeader bool, parquetType parquet.Type) []byte {
	var i64s []int64
	switch parquetType {
	case parquet.Type_BOOLEAN:
		bs, ok := values.([]bool)
		if !ok {
			panic(fmt.Errorf("expected slice of bool"))
		}

		i64s = make([]int64, len(bs))
		for i := range bs {
			if bs[i] {
				i64s[i] = 1
			}
		}
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			i64s[i] = int64(i32s[i])
		}
	case parquet.Type_INT64:
		var ok bool
		i64s, ok = values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}
	default:
		panic(fmt.Errorf("%v parquet type unsupported", parquetType))
	}

	if len(i64s) == 0 {
		return nil
	}

	var valueByte byte
	bitsSet := uint64(0)
	bitsNeeded := uint64(8)
	bitsToSet := bitWidth
	value := i64s[0]

	valueBytes := []byte{}
	for i := 0; i < len(i64s); {
		if bitsToSet >= bitsNeeded {
			valueByte |= byte(((value >> bitsSet) & ((1 << bitsNeeded) - 1)) << (8 - bitsNeeded))
			valueBytes = append(valueBytes, valueByte)
			bitsToSet -= bitsNeeded
			bitsSet += bitsNeeded

			bitsNeeded = 8
			valueByte = 0

			if bitsToSet <= 0 && (i+1) < len(i64s) {
				i++
				value = i64s[i]
				bitsToSet = bitWidth
				bitsSet = 0
			}
		} else {
			valueByte |= byte((value >> bitsSet) << (8 - bitsNeeded))
			i++

			if i < len(i64s) {
				value = i64s[i]
			}

			bitsNeeded -= bitsToSet
			bitsToSet = bitWidth
			bitsSet = 0
		}
	}

	if withHeader {
		header := uint64(((len(i64s) / 8) << 1) | 1)
		headerBytes := varIntEncode(header)
		return append(headerBytes, valueBytes...)
	}

	return valueBytes
}

func deltaEncodeInt32s(i32s []int32) (data []byte) {
	getValue := func(i32 int32) uint64 {
		return uint64((i32 >> 31) ^ (i32 << 1))
	}

	data = append(data, deltaEncodeHeaderBytes...)
	data = append(data, varIntEncode(uint64(len(i32s)))...)
	data = append(data, varIntEncode(getValue(i32s[0]))...)

	for i := 1; i < len(i32s); {
		block := []int32{}
		minDelta := int32(0x7FFFFFFF)

		for ; i < len(i32s) && len(block) < blockSize; i++ {
			delta := i32s[i] - i32s[i-1]
			block = append(block, delta)
			if delta < minDelta {
				minDelta = delta
			}
		}

		for len(block) < blockSize {
			block = append(block, minDelta)
		}

		bitWidths := make([]byte, miniBlockCount)
		for j := 0; j < miniBlockCount; j++ {
			maxValue := int32(0)
			for k := j * miniBlockSize; k < (j+1)*miniBlockSize; k++ {
				block[k] -= minDelta
				if block[k] > maxValue {
					maxValue = block[k]
				}
			}

			bitWidths[j] = byte(common.BitWidth(uint64(maxValue)))
		}

		minDeltaZigZag := getValue(minDelta)
		data = append(data, varIntEncode(minDeltaZigZag)...)
		data = append(data, bitWidths...)

		for j := 0; j < miniBlockCount; j++ {
			bitPacked := bitPackedEncode(
				block[j*miniBlockSize:(j+1)*miniBlockSize],
				uint64(bitWidths[j]),
				false,
				parquet.Type_INT32,
			)
			data = append(data, bitPacked...)
		}
	}

	return data
}

func deltaEncodeInt64s(i64s []int64) (data []byte) {
	getValue := func(i64 int64) uint64 {
		return uint64((i64 >> 63) ^ (i64 << 1))
	}

	data = append(data, deltaEncodeHeaderBytes...)
	data = append(data, varIntEncode(uint64(len(i64s)))...)
	data = append(data, varIntEncode(getValue(i64s[0]))...)

	for i := 1; i < len(i64s); {
		block := []int64{}
		minDelta := int64(0x7FFFFFFFFFFFFFFF)

		for ; i < len(i64s) && len(block) < blockSize; i++ {
			delta := i64s[i] - i64s[i-1]
			block = append(block, delta)
			if delta < minDelta {
				minDelta = delta
			}
		}

		for len(block) < blockSize {
			block = append(block, minDelta)
		}

		bitWidths := make([]byte, miniBlockCount)
		for j := 0; j < miniBlockCount; j++ {
			maxValue := int64(0)
			for k := j * miniBlockSize; k < (j+1)*miniBlockSize; k++ {
				block[k] -= minDelta
				if block[k] > maxValue {
					maxValue = block[k]
				}
			}

			bitWidths[j] = byte(common.BitWidth(uint64(maxValue)))
		}

		minDeltaZigZag := getValue(minDelta)
		data = append(data, varIntEncode(minDeltaZigZag)...)
		data = append(data, bitWidths...)

		for j := 0; j < miniBlockCount; j++ {
			bitPacked := bitPackedEncode(
				block[j*miniBlockSize:(j+1)*miniBlockSize],
				uint64(bitWidths[j]),
				false,
				parquet.Type_INT64,
			)
			data = append(data, bitPacked...)
		}
	}

	return data
}

// DeltaEncode encodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5
//
// Supported Types: INT32, INT64.
func DeltaEncode(values interface{}, parquetType parquet.Type) []byte {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}
		return deltaEncodeInt32s(i32s)
	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}
		return deltaEncodeInt64s(i64s)
	}

	panic(fmt.Errorf("%v parquet type unsupported", parquetType))
}

// DeltaLengthByteArrayEncode encodes bytes slices specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6
//
// Supported Types: BYTE_ARRAY
func DeltaLengthByteArrayEncode(bytesSlices [][]byte) (data []byte) {
	lengths := make([]int32, len(bytesSlices))
	for i, bytes := range bytesSlices {
		lengths[i] = int32(len(bytes))
	}

	data = deltaEncodeInt32s(lengths)
	for _, bytes := range bytesSlices {
		data = append(data, []byte(bytes)...)
	}

	return data
}

// DeltaByteArrayEncode encodes sequence of strings values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7
//
// Supported Types: BYTE_ARRAY
func DeltaByteArrayEncode(bytesSlices [][]byte) (data []byte) {
	prefixLengths := make([]int32, len(bytesSlices))
	suffixes := make([][]byte, len(bytesSlices))

	var i, j int
	for i = 1; i < len(bytesSlices); i++ {
		for j = 0; j < len(bytesSlices[i-1]) && j < len(bytesSlices[i]); j++ {
			if bytesSlices[i-1][j] != bytesSlices[i][j] {
				break
			}
		}

		prefixLengths[i] = int32(j)
		suffixes[i] = bytesSlices[i][j:]
	}

	data = deltaEncodeInt32s(prefixLengths)
	return append(data, DeltaLengthByteArrayEncode(suffixes)...)
}
