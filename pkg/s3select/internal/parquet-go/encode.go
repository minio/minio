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

package parquet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func boolsToBytes(bs []bool) []byte {
	size := (len(bs) + 7) / 8
	result := make([]byte, size)
	for i := range bs {
		if bs[i] {
			result[i/8] |= 1 << uint32(i%8)
		}
	}

	return result
}

func int32sToBytes(i32s []int32) []byte {
	buf := make([]byte, 4*len(i32s))
	for i, i32 := range i32s {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(i32))
	}
	return buf
}

func int64sToBytes(i64s []int64) []byte {
	buf := make([]byte, 8*len(i64s))
	for i, i64 := range i64s {
		binary.LittleEndian.PutUint64(buf[i*8:], uint64(i64))
	}
	return buf
}

func float32sToBytes(f32s []float32) []byte {
	buf := make([]byte, 4*len(f32s))
	for i, f32 := range f32s {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f32))
	}
	return buf
}

func float64sToBytes(f64s []float64) []byte {
	buf := make([]byte, 8*len(f64s))
	for i, f64 := range f64s {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(f64))
	}
	return buf
}

func byteSlicesToBytes(byteSlices [][]byte) []byte {
	buf := new(bytes.Buffer)
	for _, s := range byteSlices {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
			panic(err)
		}

		if _, err := buf.Write(s); err != nil {
			panic(err)
		}
	}

	return buf.Bytes()
}

func byteArraysToBytes(arrayList [][]byte) []byte {
	buf := new(bytes.Buffer)
	arrayLen := -1
	for _, array := range arrayList {
		if arrayLen != -1 && len(array) != arrayLen {
			panic(errors.New("array list does not have same length"))
		}

		arrayLen = len(array)
		if _, err := buf.Write(array); err != nil {
			panic(err)
		}
	}

	return buf.Bytes()
}

func int96sToBytes(i96s [][]byte) []byte {
	return byteArraysToBytes(i96s)
}

func valuesToBytes(values interface{}, dataType parquet.Type) []byte {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return boolsToBytes(values.([]bool))
	case parquet.Type_INT32:
		return int32sToBytes(values.([]int32))
	case parquet.Type_INT64:
		return int64sToBytes(values.([]int64))
	case parquet.Type_INT96:
		return int96sToBytes(values.([][]byte))
	case parquet.Type_FLOAT:
		return float32sToBytes(values.([]float32))
	case parquet.Type_DOUBLE:
		return float64sToBytes(values.([]float64))
	case parquet.Type_BYTE_ARRAY:
		return byteSlicesToBytes(values.([][]byte))
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return byteArraysToBytes(values.([][]byte))
	}

	return []byte{}
}

func valueToBytes(value interface{}, dataType parquet.Type) []byte {
	var values interface{}
	switch dataType {
	case parquet.Type_BOOLEAN:
		values = []bool{value.(bool)}
	case parquet.Type_INT32:
		values = []int32{value.(int32)}
	case parquet.Type_INT64:
		values = []int64{value.(int64)}
	case parquet.Type_INT96:
		values = [][]byte{value.([]byte)}
	case parquet.Type_FLOAT:
		values = []float32{value.(float32)}
	case parquet.Type_DOUBLE:
		values = []float64{value.(float64)}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		values = [][]byte{value.([]byte)}
	}

	return valuesToBytes(values, dataType)
}

func unsignedVarIntToBytes(ui64 uint64) []byte {
	size := (getBitWidth(ui64) + 6) / 7
	if size == 0 {
		return []byte{0}
	}

	buf := make([]byte, size)
	for i := uint64(0); i < size; i++ {
		buf[i] = byte(ui64&0x7F) | 0x80
		ui64 >>= 7
	}
	buf[size-1] &= 0x7F

	return buf
}

func valuesToRLEBytes(values interface{}, bitWidth int32, valueType parquet.Type) []byte {
	vals := valuesToInterfaces(values, valueType)
	result := []byte{}
	j := 0
	for i := 0; i < len(vals); i = j {
		for j = i + 1; j < len(vals) && vals[i] == vals[j]; j++ {
		}
		headerBytes := unsignedVarIntToBytes(uint64((j - i) << 1))
		result = append(result, headerBytes...)

		valBytes := valueToBytes(vals[i], valueType)
		byteCount := (bitWidth + 7) / 8
		result = append(result, valBytes[:byteCount]...)
	}

	return result
}

func valuesToRLEBitPackedHybridBytes(values interface{}, bitWidth int32, dataType parquet.Type) []byte {
	rleBytes := valuesToRLEBytes(values, bitWidth, dataType)
	lenBytes := valueToBytes(int32(len(rleBytes)), parquet.Type_INT32)
	return append(lenBytes, rleBytes...)
}

func valuesToBitPackedBytes(values interface{}, bitWidth int64, withHeader bool, dataType parquet.Type) []byte {
	var i64s []int64
	switch dataType {
	case parquet.Type_BOOLEAN:
		bs := values.([]bool)
		i64s = make([]int64, len(bs))
		for i := range bs {
			if bs[i] {
				i64s[i] = 1
			}
		}
	case parquet.Type_INT32:
		i32s := values.([]int32)
		i64s = make([]int64, len(i32s))
		for i := range i32s {
			i64s[i] = int64(i32s[i])
		}
	case parquet.Type_INT64:
		i64s = values.([]int64)
	default:
		panic(fmt.Errorf("data type %v is not supported for bit packing", dataType))
	}

	if len(i64s) == 0 {
		return nil
	}

	var valueByte byte
	bitsSet := uint64(0)
	bitsNeeded := uint64(8)
	bitsToSet := uint64(bitWidth)
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
				bitsToSet = uint64(bitWidth)
				bitsSet = 0
			}
		} else {
			valueByte |= byte((value >> bitsSet) << (8 - bitsNeeded))
			i++

			if i < len(i64s) {
				value = i64s[i]
			}

			bitsNeeded -= bitsToSet
			bitsToSet = uint64(bitWidth)
			bitsSet = 0
		}
	}

	if withHeader {
		header := uint64(((len(i64s) / 8) << 1) | 1)
		headerBytes := unsignedVarIntToBytes(header)
		return append(headerBytes, valueBytes...)
	}

	return valueBytes
}

const (
	blockSize     = 128
	subBlockSize  = 32
	subBlockCount = blockSize / subBlockSize
)

var (
	blockSizeBytes     = unsignedVarIntToBytes(blockSize)
	subBlockCountBytes = unsignedVarIntToBytes(subBlockCount)
)

func int32ToDeltaBytes(i32s []int32) []byte {
	getValue := func(i32 int32) uint64 {
		return uint64((i32 >> 31) ^ (i32 << 1))
	}

	result := append([]byte{}, blockSizeBytes...)
	result = append(result, subBlockCountBytes...)
	result = append(result, unsignedVarIntToBytes(uint64(len(i32s)))...)
	result = append(result, unsignedVarIntToBytes(getValue(i32s[0]))...)

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

		bitWidths := make([]byte, subBlockCount)
		for j := 0; j < subBlockCount; j++ {
			maxValue := int32(0)
			for k := j * subBlockSize; k < (j+1)*subBlockSize; k++ {
				block[k] -= minDelta
				if block[k] > maxValue {
					maxValue = block[k]
				}
			}

			bitWidths[j] = byte(getBitWidth(uint64(maxValue)))
		}

		minDeltaZigZag := getValue(minDelta)
		result = append(result, unsignedVarIntToBytes(minDeltaZigZag)...)
		result = append(result, bitWidths...)

		for j := 0; j < subBlockCount; j++ {
			bitPacked := valuesToBitPackedBytes(
				block[j*subBlockSize:(j+1)*subBlockSize],
				int64(bitWidths[j]),
				false,
				parquet.Type_INT32,
			)
			result = append(result, bitPacked...)
		}
	}

	return result
}

func int64ToDeltaBytes(i64s []int64) []byte {
	getValue := func(i64 int64) uint64 {
		return uint64((i64 >> 63) ^ (i64 << 1))
	}

	result := append([]byte{}, blockSizeBytes...)
	result = append(result, subBlockCountBytes...)
	result = append(result, unsignedVarIntToBytes(uint64(len(i64s)))...)
	result = append(result, unsignedVarIntToBytes(getValue(i64s[0]))...)

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

		bitWidths := make([]byte, subBlockCount)
		for j := 0; j < subBlockCount; j++ {
			maxValue := int64(0)
			for k := j * subBlockSize; k < (j+1)*subBlockSize; k++ {
				block[k] -= minDelta
				if block[k] > maxValue {
					maxValue = block[k]
				}
			}

			bitWidths[j] = byte(getBitWidth(uint64(maxValue)))
		}

		minDeltaZigZag := getValue(minDelta)
		result = append(result, unsignedVarIntToBytes(minDeltaZigZag)...)
		result = append(result, bitWidths...)

		for j := 0; j < subBlockCount; j++ {
			bitPacked := valuesToBitPackedBytes(
				block[j*subBlockSize:(j+1)*subBlockSize],
				int64(bitWidths[j]),
				false,
				parquet.Type_INT64,
			)
			result = append(result, bitPacked...)
		}
	}

	return result
}

func valuesToDeltaBytes(values interface{}, dataType parquet.Type) []byte {
	switch dataType {
	case parquet.Type_INT32:
		return int32ToDeltaBytes(values.([]int32))
	case parquet.Type_INT64:
		return int64ToDeltaBytes(values.([]int64))
	}

	return nil
}

func stringsToDeltaLengthByteArrayBytes(strs []string) []byte {
	lengths := make([]int32, len(strs))
	for i, s := range strs {
		lengths[i] = int32(len(s))
	}

	result := int32ToDeltaBytes(lengths)
	for _, s := range strs {
		result = append(result, []byte(s)...)
	}

	return result
}

func stringsToDeltaByteArrayBytes(strs []string) []byte {
	prefixLengths := make([]int32, len(strs))
	suffixes := make([]string, len(strs))

	var i, j int
	for i = 1; i < len(strs); i++ {
		for j = 0; j < len(strs[i-1]) && j < len(strs[i]); j++ {
			if strs[i-1][j] != strs[i][j] {
				break
			}
		}

		prefixLengths[i] = int32(j)
		suffixes[i] = strs[i][j:]
	}

	result := int32ToDeltaBytes(prefixLengths)
	return append(result, stringsToDeltaLengthByteArrayBytes(suffixes)...)
}

func encodeValues(values interface{}, dataType parquet.Type, encoding parquet.Encoding, bitWidth int32) []byte {
	switch encoding {
	case parquet.Encoding_RLE:
		return valuesToRLEBitPackedHybridBytes(values, bitWidth, dataType)
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return valuesToDeltaBytes(values, dataType)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return stringsToDeltaByteArrayBytes(values.([]string))
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return stringsToDeltaLengthByteArrayBytes(values.([]string))
	}

	return valuesToBytes(values, dataType)
}
