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

package common

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"github.com/klauspost/compress/snappy"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/pierrec/lz4"
)

// ToSliceValue converts values to a slice value.
func ToSliceValue(values []interface{}, parquetType parquet.Type) interface{} {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		bs := make([]bool, len(values))
		for i := range values {
			bs[i] = values[i].(bool)
		}
		return bs
	case parquet.Type_INT32:
		i32s := make([]int32, len(values))
		for i := range values {
			i32s[i] = values[i].(int32)
		}
		return i32s
	case parquet.Type_INT64:
		i64s := make([]int64, len(values))
		for i := range values {
			i64s[i] = values[i].(int64)
		}
		return i64s
	case parquet.Type_FLOAT:
		f32s := make([]float32, len(values))
		for i := range values {
			f32s[i] = values[i].(float32)
		}
		return f32s
	case parquet.Type_DOUBLE:
		f64s := make([]float64, len(values))
		for i := range values {
			f64s[i] = values[i].(float64)
		}
		return f64s
	case parquet.Type_BYTE_ARRAY:
		array := make([][]byte, len(values))
		for i := range values {
			array[i] = values[i].([]byte)
		}
		return array
	}

	return nil
}

// BitWidth returns bits count required to accommodate given value.
func BitWidth(ui64 uint64) (width int32) {
	for ; ui64 != 0; ui64 >>= 1 {
		width++
	}

	return width
}

// Compress compresses given data.
func Compress(compressionType parquet.CompressionCodec, data []byte) ([]byte, error) {
	switch compressionType {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return data, nil

	case parquet.CompressionCodec_SNAPPY:
		return snappy.Encode(nil, data), nil

	case parquet.CompressionCodec_GZIP:
		buf := new(bytes.Buffer)
		writer := gzip.NewWriter(buf)
		n, err := writer.Write(data)
		if err != nil {
			return nil, err
		}
		if n != len(data) {
			return nil, fmt.Errorf("short writes")
		}

		if err = writer.Flush(); err != nil {
			return nil, err
		}

		if err = writer.Close(); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil

	case parquet.CompressionCodec_LZ4:
		buf := new(bytes.Buffer)
		writer := lz4.NewWriter(buf)
		n, err := writer.Write(data)
		if err != nil {
			return nil, err
		}
		if n != len(data) {
			return nil, fmt.Errorf("short writes")
		}

		if err = writer.Flush(); err != nil {
			return nil, err
		}

		if err = writer.Close(); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unsupported compression codec %v", compressionType)
}

// Uncompress uncompresses given data.
func Uncompress(compressionType parquet.CompressionCodec, data []byte) ([]byte, error) {
	switch compressionType {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return data, nil

	case parquet.CompressionCodec_SNAPPY:
		return snappy.Decode(nil, data)

	case parquet.CompressionCodec_GZIP:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return ioutil.ReadAll(reader)

	case parquet.CompressionCodec_LZ4:
		return ioutil.ReadAll(lz4.NewReader(bytes.NewReader(data)))
	}

	return nil, fmt.Errorf("unsupported compression codec %v", compressionType)
}
