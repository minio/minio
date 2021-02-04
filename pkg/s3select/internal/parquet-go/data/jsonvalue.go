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
