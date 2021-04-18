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

package parquet

import (
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func valuesToInterfaces(values interface{}, valueType parquet.Type) (tableValues []interface{}) {
	switch valueType {
	case parquet.Type_BOOLEAN:
		for _, v := range values.([]bool) {
			tableValues = append(tableValues, v)
		}
	case parquet.Type_INT32:
		for _, v := range values.([]int32) {
			tableValues = append(tableValues, v)
		}
	case parquet.Type_INT64:
		for _, v := range values.([]int64) {
			tableValues = append(tableValues, v)
		}
	case parquet.Type_FLOAT:
		for _, v := range values.([]float32) {
			tableValues = append(tableValues, v)
		}
	case parquet.Type_DOUBLE:
		for _, v := range values.([]float64) {
			tableValues = append(tableValues, v)
		}
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		for _, v := range values.([][]byte) {
			tableValues = append(tableValues, v)
		}
	}

	return tableValues
}

func interfacesToValues(values []interface{}, valueType parquet.Type) interface{} {
	switch valueType {
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
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		array := make([][]byte, len(values))
		for i := range values {
			array[i] = values[i].([]byte)
		}
		return array
	}

	return nil
}
