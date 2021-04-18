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

package data

import (
	"fmt"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/tidwall/gjson"
)

type jsonValue struct {
	result *gjson.Result
	path   *string
}

func (v *jsonValue) String() string {
	if v.result == nil {
		return "<nil>"
	}

	return fmt.Sprintf("%v", *v.result)
}

func (v *jsonValue) IsNull() bool {
	return v.result == nil || v.result.Type == gjson.Null
}

func (v *jsonValue) Get(path string) *jsonValue {
	if v.path != nil {
		var result *gjson.Result
		if *v.path == path {
			result = v.result
		}

		return resultToJSONValue(result)
	}

	if v.result == nil {
		return resultToJSONValue(nil)
	}

	result := v.result.Get(path)
	if !result.Exists() {
		return resultToJSONValue(nil)
	}

	return resultToJSONValue(&result)
}

func (v *jsonValue) GetValue(parquetType parquet.Type, convertedType *parquet.ConvertedType) (interface{}, error) {
	if v.result == nil {
		return nil, nil
	}

	return resultToParquetValue(*v.result, parquetType, convertedType)
}

func (v *jsonValue) GetArray() ([]gjson.Result, error) {
	if v.result == nil {
		return nil, nil
	}

	return resultToArray(*v.result)
}

func (v *jsonValue) Range(iterator func(key, value gjson.Result) bool) error {
	if v.result == nil || v.result.Type == gjson.Null {
		return nil
	}

	if v.result.Type != gjson.JSON || !v.result.IsObject() {
		return fmt.Errorf("result is not Map but %v", v.result.Type)
	}

	v.result.ForEach(iterator)
	return nil
}

func resultToJSONValue(result *gjson.Result) *jsonValue {
	return &jsonValue{
		result: result,
	}
}

func bytesToJSONValue(data []byte) (*jsonValue, error) {
	if !gjson.ValidBytes(data) {
		return nil, fmt.Errorf("invalid JSON data")
	}

	result := gjson.ParseBytes(data)
	return resultToJSONValue(&result), nil
}
