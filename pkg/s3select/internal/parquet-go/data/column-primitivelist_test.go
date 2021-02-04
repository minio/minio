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
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
)

func TestPopulatePrimitiveList(t *testing.T) {
	requiredList1 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredList1.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = requiredList1.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = requiredList1.Set("col.list.element", requiredElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredList1.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	requiredList2 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredList2.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = requiredList2.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = requiredList2.Set("col.list.element", optionalElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredList2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList1 := schema.NewTree()
	{
		optionalCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList1.Set("col", optionalCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list.element", requiredElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList1.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList2 := schema.NewTree()
	{
		optionalCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList2.Set("col", optionalCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list.element", optionalElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	result1 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{1},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result2 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30},
			definitionLevels: []int64{1, 1, 1},
			repetitionLevels: []int64{0, 1, 1},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	result3 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{1},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result4 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result5 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30},
			definitionLevels: []int64{2, 2, 2},
			repetitionLevels: []int64{0, 1, 1},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	result6 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result7 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result8 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{3},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result9 := map[string]*Column{
		"col.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30},
			definitionLevels: []int64{3, 3, 3},
			repetitionLevels: []int64{0, 1, 1},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	testCases := []struct {
		schemaTree     *schema.Tree
		data           string
		expectedResult map[string]*Column
		expectErr      bool
	}{
		{requiredList1, `{}`, nil, true},              // err: col: nil value for required field
		{requiredList1, `{"col": null}`, nil, true},   // err: col: nil value for required field
		{requiredList1, `{"col": [null]}`, nil, true}, // err: col.list.element: nil value for required field
		{requiredList1, `{"col": [10]}`, result1, false},
		{requiredList1, `{"col": [10, 20, 30]}`, result2, false},
		{requiredList2, `{}`, nil, true},            // err: col: nil value for required field
		{requiredList2, `{"col": null}`, nil, true}, // err: col: nil value for required field
		{requiredList2, `{"col": [null]}`, result3, false},
		{requiredList2, `{"col": [10]}`, result4, false},
		{requiredList2, `{"col": [10, 20, 30]}`, result5, false},
		{optionalList1, `{}`, result6, false},
		{optionalList1, `{"col": null}`, result6, false},
		{optionalList1, `{"col": [null]}`, nil, true}, // err: col.list.element: nil value for required field
		{optionalList1, `{"col": [10]}`, result4, false},
		{optionalList1, `{"col": [10, 20, 30]}`, result5, false},
		{optionalList2, `{}`, result6, false},
		{optionalList2, `{"col": null}`, result6, false},
		{optionalList2, `{"col": [null]}`, result7, false},
		{optionalList2, `{"col": [10]}`, result8, false},
		{optionalList2, `{"col": [10, 20, 30]}`, result9, false},
	}

	for i, testCase := range testCases {
		result, err := UnmarshalJSON([]byte(testCase.data), testCase.schemaTree)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
