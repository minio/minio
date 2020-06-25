/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"fmt"
	"math"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func i64sToi32s(i64s []int64) (i32s []int32) {
	i32s = make([]int32, len(i64s))
	for i := range i64s {
		i32s[i] = int32(i64s[i])
	}

	return i32s
}

func readBitPacked(reader *bytes.Reader, header, bitWidth uint64) (result []int64, err error) {
	count := header * 8

	if count == 0 {
		return result, nil
	}

	if bitWidth == 0 {
		return make([]int64, count), nil
	}

	data := make([]byte, header*bitWidth)
	if _, err = reader.Read(data); err != nil {
		return nil, err
	}

	var val, used, left, b uint64

	valNeedBits := bitWidth
	i := -1
	for {
		if left <= 0 {
			i++
			if i >= len(data) {
				break
			}

			b = uint64(data[i])
			left = 8
			used = 0
		}

		if left >= valNeedBits {
			val |= ((b >> used) & ((1 << valNeedBits) - 1)) << (bitWidth - valNeedBits)
			result = append(result, int64(val))
			val = 0
			left -= valNeedBits
			used += valNeedBits
			valNeedBits = bitWidth
		} else {
			val |= (b >> used) << (bitWidth - valNeedBits)
			valNeedBits -= left
			left = 0
		}
	}

	return result, nil
}

func readBools(reader *bytes.Reader, count uint64) (result []bool, err error) {
	i64s, err := readBitPacked(reader, count, 1)
	if err != nil {
		return nil, err
	}

	var i uint64
	for i = 0; i < count; i++ {
		result = append(result, i64s[i] > 0)
	}

	return result, nil
}

func readInt32s(reader *bytes.Reader, count uint64) (result []int32, err error) {
	buf := make([]byte, 4)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, int32(bytesToUint32(buf)))
	}

	return result, nil
}

func readInt64s(reader *bytes.Reader, count uint64) (result []int64, err error) {
	buf := make([]byte, 8)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, int64(bytesToUint64(buf)))
	}

	return result, nil
}

func readInt96s(reader *bytes.Reader, count uint64) (result [][]byte, err error) {
	var i uint64
	for i = 0; i < count; i++ {
		buf := make([]byte, 12)

		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, buf)
	}

	return result, nil
}

func readFloats(reader *bytes.Reader, count uint64) (result []float32, err error) {
	buf := make([]byte, 4)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, math.Float32frombits(bytesToUint32(buf)))
	}

	return result, nil
}

func readDoubles(reader *bytes.Reader, count uint64) (result []float64, err error) {
	buf := make([]byte, 8)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, math.Float64frombits(bytesToUint64(buf)))
	}

	return result, nil
}

func readByteArrays(reader *bytes.Reader, count uint64) (result [][]byte, err error) {
	buf := make([]byte, 4)
	var length uint32
	var data []byte

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		length = bytesToUint32(buf)
		data = make([]byte, length)
		if length > 0 {
			if _, err = reader.Read(data); err != nil {
				return nil, err
			}
		}

		result = append(result, data)
	}

	return result, nil
}

func readFixedLenByteArrays(reader *bytes.Reader, count, length uint64) (result [][]byte, err error) {
	var i uint64
	for i = 0; i < count; i++ {
		data := make([]byte, length)
		if _, err = reader.Read(data); err != nil {
			return nil, err
		}

		result = append(result, data)
	}

	return result, nil
}

func readValues(reader *bytes.Reader, dataType parquet.Type, count, length uint64) (interface{}, error) {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return readBools(reader, count)
	case parquet.Type_INT32:
		return readInt32s(reader, count)
	case parquet.Type_INT64:
		return readInt64s(reader, count)
	case parquet.Type_INT96:
		return readInt96s(reader, count)
	case parquet.Type_FLOAT:
		return readFloats(reader, count)
	case parquet.Type_DOUBLE:
		return readDoubles(reader, count)
	case parquet.Type_BYTE_ARRAY:
		return readByteArrays(reader, count)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return readFixedLenByteArrays(reader, count, length)
	}

	return nil, fmt.Errorf("unknown parquet type %v", dataType)
}

func readUnsignedVarInt(reader *bytes.Reader) (v uint64, err error) {
	var b byte
	var shift uint64

	for {
		if b, err = reader.ReadByte(); err != nil {
			return 0, err
		}

		if v |= ((uint64(b) & 0x7F) << shift); b&0x80 == 0 {
			break
		}

		shift += 7
	}

	return v, nil
}

func readRLE(reader *bytes.Reader, header, bitWidth uint64) (result []int64, err error) {
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = reader.Read(data); err != nil {
			return nil, err
		}
	}

	if width < 4 {
		data = append(data, make([]byte, 4-width)...)
	}

	val := int64(bytesToUint32(data))

	count := header >> 1
	result = make([]int64, count)
	for i := range result {
		result[i] = val
	}

	return result, nil
}

func readRLEBitPackedHybrid(reader *bytes.Reader, length, bitWidth uint64) (result []int64, err error) {
	if length <= 0 {
		var i32s []int32
		i32s, err = readInt32s(reader, 1)
		if err != nil {
			return nil, err
		}
		length = uint64(i32s[0])
	}

	buf := make([]byte, length)
	if _, err = reader.Read(buf); err != nil {
		return nil, err
	}

	reader = bytes.NewReader(buf)
	for reader.Len() > 0 {
		header, err := readUnsignedVarInt(reader)
		if err != nil {
			return nil, err
		}

		var i64s []int64
		if header&1 == 0 {
			i64s, err = readRLE(reader, header, bitWidth)
		} else {
			i64s, err = readBitPacked(reader, header>>1, bitWidth)
		}

		if err != nil {
			return nil, err
		}

		result = append(result, i64s...)
	}

	return result, nil
}

func readDeltaBinaryPackedInt(reader *bytes.Reader) (result []int64, err error) {
	blockSize, err := readUnsignedVarInt(reader)
	if err != nil {
		return nil, err
	}

	numMiniblocksInBlock, err := readUnsignedVarInt(reader)
	if err != nil {
		return nil, err
	}

	numValues, err := readUnsignedVarInt(reader)
	if err != nil {
		return nil, err
	}

	firstValueZigZag, err := readUnsignedVarInt(reader)
	if err != nil {
		return nil, err
	}

	v := int64(firstValueZigZag>>1) ^ (-int64(firstValueZigZag & 1))
	result = append(result, v)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	bitWidths := make([]uint64, numMiniblocksInBlock)
	for uint64(len(result)) < numValues {
		minDeltaZigZag, err := readUnsignedVarInt(reader)
		if err != nil {
			return nil, err
		}

		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			bitWidths[i] = uint64(b)
		}

		minDelta := int64(minDeltaZigZag>>1) ^ (-int64(minDeltaZigZag & 1))
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			i64s, err := readBitPacked(reader, numValuesInMiniBlock/8, bitWidths[i])
			if err != nil {
				return nil, err
			}

			for j := range i64s {
				v += i64s[j] + minDelta
				result = append(result, v)
			}
		}
	}

	return result[:numValues], nil
}

func readDeltaLengthByteArrays(reader *bytes.Reader) (result [][]byte, err error) {
	i64s, err := readDeltaBinaryPackedInt(reader)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(i64s); i++ {
		arrays, err := readFixedLenByteArrays(reader, 1, uint64(i64s[i]))
		if err != nil {
			return nil, err
		}

		result = append(result, arrays[0])
	}

	return result, nil
}

func readDeltaByteArrays(reader *bytes.Reader) (result [][]byte, err error) {
	i64s, err := readDeltaBinaryPackedInt(reader)
	if err != nil {
		return nil, err
	}

	suffixes, err := readDeltaLengthByteArrays(reader)
	if err != nil {
		return nil, err
	}

	result = append(result, suffixes[0])
	for i := 1; i < len(i64s); i++ {
		prefixLength := i64s[i]
		val := append([]byte{}, result[i-1][:prefixLength]...)
		val = append(val, suffixes[i]...)
		result = append(result, val)
	}

	return result, nil
}

func readDataPageValues(
	bytesReader *bytes.Reader,
	encoding parquet.Encoding,
	dataType parquet.Type,
	convertedType parquet.ConvertedType,
	count, bitWidth uint64,
) (result interface{}, resultDataType parquet.Type, err error) {
	switch encoding {
	case parquet.Encoding_PLAIN:
		result, err = readValues(bytesReader, dataType, count, bitWidth)
		return result, dataType, err

	case parquet.Encoding_PLAIN_DICTIONARY:
		b, err := bytesReader.ReadByte()
		if err != nil {
			return nil, -1, err
		}

		i64s, err := readRLEBitPackedHybrid(bytesReader, uint64(bytesReader.Len()), uint64(b))
		if err != nil {
			return nil, -1, err
		}

		return i64s[:count], parquet.Type_INT64, nil

	case parquet.Encoding_RLE:
		i64s, err := readRLEBitPackedHybrid(bytesReader, 0, bitWidth)
		if err != nil {
			return nil, -1, err
		}

		i64s = i64s[:count]

		if dataType == parquet.Type_INT32 {
			return i64sToi32s(i64s), parquet.Type_INT32, nil
		}

		return i64s, parquet.Type_INT64, nil

	case parquet.Encoding_BIT_PACKED:
		return nil, -1, fmt.Errorf("deprecated parquet encoding %v", parquet.Encoding_BIT_PACKED)

	case parquet.Encoding_DELTA_BINARY_PACKED:
		i64s, err := readDeltaBinaryPackedInt(bytesReader)
		if err != nil {
			return nil, -1, err
		}

		i64s = i64s[:count]

		if dataType == parquet.Type_INT32 {
			return i64sToi32s(i64s), parquet.Type_INT32, nil
		}

		return i64s, parquet.Type_INT64, nil

	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		byteSlices, err := readDeltaLengthByteArrays(bytesReader)
		if err != nil {
			return nil, -1, err
		}

		return byteSlices[:count], parquet.Type_FIXED_LEN_BYTE_ARRAY, nil

	case parquet.Encoding_DELTA_BYTE_ARRAY:
		byteSlices, err := readDeltaByteArrays(bytesReader)
		if err != nil {
			return nil, -1, err
		}

		return byteSlices[:count], parquet.Type_FIXED_LEN_BYTE_ARRAY, nil
	}

	return nil, -1, fmt.Errorf("unsupported parquet encoding %v", encoding)
}
