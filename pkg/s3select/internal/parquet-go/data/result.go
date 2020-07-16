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

package data

import (
	"fmt"
	"math"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/tidwall/gjson"
)

func resultToBool(result gjson.Result) (value interface{}, err error) {
	switch result.Type {
	case gjson.False, gjson.True:
		return result.Bool(), nil
	}

	return nil, fmt.Errorf("result is not Bool but %v", result.Type)
}

func resultToInt32(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToInt64(result); err != nil {
		return nil, err
	}

	if value.(int64) < math.MinInt32 || value.(int64) > math.MaxInt32 {
		return nil, fmt.Errorf("int32 overflow")
	}

	return int32(value.(int64)), nil
}

func resultToInt64(result gjson.Result) (value interface{}, err error) {
	if result.Type == gjson.Number {
		return result.Int(), nil
	}

	return nil, fmt.Errorf("result is not Number but %v", result.Type)
}

func resultToFloat(result gjson.Result) (value interface{}, err error) {
	if result.Type == gjson.Number {
		return float32(result.Float()), nil
	}

	return nil, fmt.Errorf("result is not float32 but %v", result.Type)
}

func resultToDouble(result gjson.Result) (value interface{}, err error) {
	if result.Type == gjson.Number {
		return result.Float(), nil
	}

	return nil, fmt.Errorf("result is not float64 but %v", result.Type)
}

func resultToBytes(result gjson.Result) (interface{}, error) {
	if result.Type != gjson.JSON || !result.IsArray() {
		return nil, fmt.Errorf("result is not byte array but %v", result.Type)
	}

	data := []byte{}
	for i, r := range result.Array() {
		if r.Type != gjson.Number {
			return nil, fmt.Errorf("result[%v] is not byte but %v", i, r.Type)
		}

		value := r.Uint()
		if value > math.MaxUint8 {
			return nil, fmt.Errorf("byte overflow in result[%v]", i)
		}

		data = append(data, byte(value))
	}

	return data, nil
}

func resultToString(result gjson.Result) (value interface{}, err error) {
	if result.Type == gjson.String {
		return result.String(), nil
	}

	return nil, fmt.Errorf("result is not String but %v", result.Type)
}

func resultToUint8(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToUint64(result); err != nil {
		return nil, err
	}

	if value.(uint64) > math.MaxUint8 {
		return nil, fmt.Errorf("uint8 overflow")
	}

	return uint8(value.(uint64)), nil
}

func resultToUint16(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToUint64(result); err != nil {
		return nil, err
	}

	if value.(uint64) > math.MaxUint16 {
		return nil, fmt.Errorf("uint16 overflow")
	}

	return uint16(value.(uint64)), nil
}

func resultToUint32(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToUint64(result); err != nil {
		return nil, err
	}

	if value.(uint64) > math.MaxUint32 {
		return nil, fmt.Errorf("uint32 overflow")
	}

	return uint32(value.(uint64)), nil
}

func resultToUint64(result gjson.Result) (value interface{}, err error) {
	if result.Type == gjson.Number {
		return result.Uint(), nil
	}

	return nil, fmt.Errorf("result is not Number but %v", result.Type)
}

func resultToInt8(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToInt64(result); err != nil {
		return nil, err
	}

	if value.(int64) < math.MinInt8 || value.(int64) > math.MaxInt8 {
		return nil, fmt.Errorf("int8 overflow")
	}

	return int8(value.(int64)), nil
}

func resultToInt16(result gjson.Result) (value interface{}, err error) {
	if value, err = resultToInt64(result); err != nil {
		return nil, err
	}

	if value.(int64) < math.MinInt16 || value.(int64) > math.MaxInt16 {
		return nil, fmt.Errorf("int16 overflow")
	}

	return int16(value.(int64)), nil
}

func stringToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return []byte(value.(string)), nil
	}

	return nil, fmt.Errorf("string cannot be converted to parquet type %v", parquetType)
}

func uint8ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(uint8)), nil
	case parquet.Type_INT64:
		return int64(value.(uint8)), nil
	}

	return nil, fmt.Errorf("uint8 cannot be converted to parquet type %v", parquetType)
}

func uint16ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(uint16)), nil
	case parquet.Type_INT64:
		return int64(value.(uint16)), nil
	}

	return nil, fmt.Errorf("uint16 cannot be converted to parquet type %v", parquetType)
}

func uint32ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(uint32)), nil
	case parquet.Type_INT64:
		return int64(value.(uint32)), nil
	}

	return nil, fmt.Errorf("uint32 cannot be converted to parquet type %v", parquetType)
}

func uint64ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(uint64)), nil
	case parquet.Type_INT64:
		return int64(value.(uint64)), nil
	}

	return nil, fmt.Errorf("uint64 cannot be converted to parquet type %v", parquetType)
}

func int8ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(int8)), nil
	case parquet.Type_INT64:
		return int64(value.(int8)), nil
	}

	return nil, fmt.Errorf("int8 cannot be converted to parquet type %v", parquetType)
}

func int16ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(int16)), nil
	case parquet.Type_INT64:
		return int64(value.(int16)), nil
	}

	return nil, fmt.Errorf("int16 cannot be converted to parquet type %v", parquetType)
}

func int32ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return value.(int32), nil
	case parquet.Type_INT64:
		return int64(value.(int32)), nil
	}

	return nil, fmt.Errorf("int32 cannot be converted to parquet type %v", parquetType)
}

func int64ToParquetValue(value interface{}, parquetType parquet.Type) (interface{}, error) {
	switch parquetType {
	case parquet.Type_INT32:
		return int32(value.(int64)), nil
	case parquet.Type_INT64:
		return value.(int64), nil
	}

	return nil, fmt.Errorf("int64 cannot be converted to parquet type %v", parquetType)
}

func resultToParquetValueByConvertedValue(result gjson.Result, convertedType parquet.ConvertedType, parquetType parquet.Type) (value interface{}, err error) {
	if result.Type == gjson.Null {
		return nil, nil
	}

	switch convertedType {
	case parquet.ConvertedType_UTF8:
		if value, err = resultToString(result); err != nil {
			return nil, err
		}
		return stringToParquetValue(value, parquetType)
	case parquet.ConvertedType_UINT_8:
		if value, err = resultToUint8(result); err != nil {
			return nil, err
		}
		return uint8ToParquetValue(value, parquetType)
	case parquet.ConvertedType_UINT_16:
		if value, err = resultToUint16(result); err != nil {
			return nil, err
		}
		return uint16ToParquetValue(value, parquetType)
	case parquet.ConvertedType_UINT_32:
		if value, err = resultToUint32(result); err != nil {
			return nil, err
		}
		return uint32ToParquetValue(value, parquetType)
	case parquet.ConvertedType_UINT_64:
		if value, err = resultToUint64(result); err != nil {
			return nil, err
		}
		return uint64ToParquetValue(value, parquetType)
	case parquet.ConvertedType_INT_8:
		if value, err = resultToInt8(result); err != nil {
			return nil, err
		}
		return int8ToParquetValue(value, parquetType)
	case parquet.ConvertedType_INT_16:
		if value, err = resultToInt16(result); err != nil {
			return nil, err
		}
		return int16ToParquetValue(value, parquetType)
	case parquet.ConvertedType_INT_32:
		if value, err = resultToInt32(result); err != nil {
			return nil, err
		}
		return int32ToParquetValue(value, parquetType)
	case parquet.ConvertedType_INT_64:
		if value, err = resultToInt64(result); err != nil {
			return nil, err
		}
		return int64ToParquetValue(value, parquetType)
	}

	return nil, fmt.Errorf("unsupported converted type %v", convertedType)
}

func resultToParquetValue(result gjson.Result, parquetType parquet.Type, convertedType *parquet.ConvertedType) (interface{}, error) {
	if convertedType != nil {
		return resultToParquetValueByConvertedValue(result, *convertedType, parquetType)
	}

	if result.Type == gjson.Null {
		return nil, nil
	}

	switch parquetType {
	case parquet.Type_BOOLEAN:
		return resultToBool(result)
	case parquet.Type_INT32:
		return resultToInt32(result)
	case parquet.Type_INT64:
		return resultToInt64(result)
	case parquet.Type_FLOAT:
		return resultToFloat(result)
	case parquet.Type_DOUBLE:
		return resultToDouble(result)
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return resultToBytes(result)
	}

	return nil, fmt.Errorf("unknown parquet type %v", parquetType)
}

func resultToArray(result gjson.Result) ([]gjson.Result, error) {
	if result.Type == gjson.Null {
		return nil, nil
	}

	if result.Type != gjson.JSON || !result.IsArray() {
		return nil, fmt.Errorf("result is not Array but %v", result.Type)
	}

	return result.Array(), nil
}
