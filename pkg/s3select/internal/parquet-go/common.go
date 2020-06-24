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
	"reflect"

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

// sizeOf - get the size of a parquet value
func sizeOf(value interface{}) (size int32) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if v.IsNil() {
			return size
		}
	}

	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Bool:
		size = 1
	case reflect.Int32, reflect.Float32:
		size = 4
	case reflect.Int64, reflect.Float64:
		size = 8
	case reflect.Slice:
		size = int32(v.Len())
	}

	return size
}

func lessThanBytes(a, b []byte, littleEndianOrder, signed bool) bool {
	alen, blen := len(a), len(b)

	if littleEndianOrder {
		// Reverse a
		for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
			a[i], a[j] = a[j], a[i]
		}

		// Reverse b
		for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
			b[i], b[j] = b[j], b[i]
		}
	}

	// Make a and b are equal sized array.
	if alen < blen {
		preBytes := make([]byte, blen-alen)
		if signed && a[0]&0x80 == 0x80 {
			for i := range preBytes {
				preBytes[i] = 0xFF
			}
		}

		a = append(preBytes, a...)
	}

	if alen > blen {
		preBytes := make([]byte, alen-blen)
		if signed && b[0]&0x80 == 0x80 {
			for i := range preBytes {
				preBytes[i] = 0xFF
			}
		}

		b = append(preBytes, b...)
	}

	if signed {
		// If ((BYTE & 0x80) = 0x80) means, BYTE is negative.  Hence negative logic is used.
		if a[0]&0x80 > b[0]&0x80 {
			return true
		}

		if a[0]&0x80 < b[0]&0x80 {
			return false
		}
	}

	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return true
		}

		if a[i] > b[i] {
			return false
		}
	}

	return false
}

// lessThan - returns whether a is less than b.
func lessThan(a, b interface{}, dataType *parquet.Type, convertedType *parquet.ConvertedType) bool {
	if a == nil {
		if b == nil {
			return false
		}

		return true
	}

	if b == nil {
		return false
	}

	switch *dataType {
	case parquet.Type_BOOLEAN:
		return !a.(bool) && b.(bool)

	case parquet.Type_INT32:
		if convertedType != nil {
			switch *convertedType {
			case parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16, parquet.ConvertedType_UINT_32:
				return uint32(a.(int32)) < uint32(b.(int32))
			}
		}
		return a.(int32) < b.(int32)

	case parquet.Type_INT64:
		if convertedType != nil && *convertedType == parquet.ConvertedType_UINT_64 {
			return uint64(a.(int64)) < uint64(b.(int64))
		}
		return a.(int64) < b.(int64)

	case parquet.Type_INT96:
		ab := a.([]byte)
		bb := b.([]byte)

		// If ((BYTE & 0x80) = 0x80) means, BYTE is negative.  Hence negative logic is used.
		if ab[11]&0x80 > bb[11]&0x80 {
			return true
		}

		if ab[11]&0x80 < bb[11]&0x80 {
			return false
		}

		for i := 11; i >= 0; i-- {
			if ab[i] < bb[i] {
				return true
			}

			if ab[i] > bb[i] {
				return false
			}
		}

		return false

	case parquet.Type_FLOAT:
		return a.(float32) < b.(float32)

	case parquet.Type_DOUBLE:
		return a.(float64) < b.(float64)

	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return bytes.Compare(a.([]byte), b.([]byte)) == -1
	}

	return false
}

func min(a, b interface{}, dataType *parquet.Type, convertedType *parquet.ConvertedType) interface{} {
	if a == nil {
		return b
	}

	if b == nil {
		return a
	}

	if lessThan(a, b, dataType, convertedType) {
		return a
	}

	return b
}

func max(a, b interface{}, dataType *parquet.Type, convertedType *parquet.ConvertedType) interface{} {
	if a == nil {
		return b
	}

	if b == nil {
		return a
	}

	if lessThan(a, b, dataType, convertedType) {
		return b
	}

	return a
}
