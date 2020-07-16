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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func plainEncodeBools(bs []bool) []byte {
	data := make([]byte, (len(bs)+7)/8)

	for i := range bs {
		if bs[i] {
			data[i/8] |= 1 << uint(i%8)
		}
	}

	return data
}

func plainEncodeInt32s(i32s []int32) []byte {
	data := make([]byte, len(i32s)*4)

	for i, i32 := range i32s {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(i32))
	}

	return data
}

func plainEncodeInt64s(i64s []int64) []byte {
	data := make([]byte, len(i64s)*8)

	for i, i64 := range i64s {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(i64))
	}

	return data
}

func plainEncodeFloat32s(f32s []float32) []byte {
	data := make([]byte, len(f32s)*4)

	for i, f32 := range f32s {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(f32))
	}

	return data
}

func plainEncodeFloat64s(f64s []float64) []byte {
	data := make([]byte, len(f64s)*8)

	for i, f64 := range f64s {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(f64))
	}

	return data
}

func plainEncodeBytesSlices(bytesSlices [][]byte) []byte {
	buf := new(bytes.Buffer)

	for _, s := range bytesSlices {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
			panic(err)
		}

		if _, err := buf.Write(s); err != nil {
			panic(err)
		}
	}

	return buf.Bytes()
}

// PlainEncode encodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
//
// Supported Types: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY
func PlainEncode(values interface{}, parquetType parquet.Type) []byte {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		bs, ok := values.([]bool)
		if !ok {
			panic(fmt.Errorf("expected slice of bool"))
		}
		return plainEncodeBools(bs)
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}
		return plainEncodeInt32s(i32s)
	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}
		return plainEncodeInt64s(i64s)
	case parquet.Type_FLOAT:
		f32s, ok := values.([]float32)
		if !ok {
			panic(fmt.Errorf("expected slice of float32"))
		}
		return plainEncodeFloat32s(f32s)
	case parquet.Type_DOUBLE:
		f64s, ok := values.([]float64)
		if !ok {
			panic(fmt.Errorf("expected slice of float64"))
		}
		return plainEncodeFloat64s(f64s)
	case parquet.Type_BYTE_ARRAY:
		bytesSlices, ok := values.([][]byte)
		if !ok {
			panic(fmt.Errorf("expected slice of byte array"))
		}
		return plainEncodeBytesSlices(bytesSlices)
	}

	panic(fmt.Errorf("%v parquet type unsupported", parquetType))
}
