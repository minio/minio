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
