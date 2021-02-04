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

func TestPopulateListOfList(t *testing.T) {
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
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
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
		if err = requiredList1.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = requiredList1.Set("col.list.element.list.element", requiredSubElement); err != nil {
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

		requiredElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), nil,
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
		if err = requiredList2.Set("col.list.element", requiredElement); err != nil {
			t.Fatal(err)
		}
		if err = requiredList2.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = requiredList2.Set("col.list.element.list.element", optionalSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredList2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	requiredList3 := schema.NewTree()
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

		optioonalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredList3.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = requiredList3.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = requiredList3.Set("col.list.element", optioonalElement); err != nil {
			t.Fatal(err)
		}
		if err = requiredList3.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = requiredList3.Set("col.list.element.list.element", requiredSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredList3.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	requiredList4 := schema.NewTree()
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

		optioonalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredList4.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = requiredList4.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = requiredList4.Set("col.list.element", optioonalElement); err != nil {
			t.Fatal(err)
		}
		if err = requiredList4.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = requiredList4.Set("col.list.element.list.element", optionalSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredList4.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList1 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
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
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList1.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list.element", requiredElement); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = optionalList1.Set("col.list.element.list.element", requiredSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList1.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList2 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
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
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList2.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list.element", requiredElement); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = optionalList2.Set("col.list.element.list.element", optionalSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList3 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
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

		optioonalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList3.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList3.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList3.Set("col.list.element", optioonalElement); err != nil {
			t.Fatal(err)
		}
		if err = optionalList3.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = optionalList3.Set("col.list.element.list.element", requiredSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList3.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalList4 := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
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

		optioonalElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		subList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalSubElement, err := schema.NewElement("element", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalList4.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}
		if err = optionalList4.Set("col.list", list); err != nil {
			t.Fatal(err)
		}
		if err = optionalList4.Set("col.list.element", optioonalElement); err != nil {
			t.Fatal(err)
		}
		if err = optionalList4.Set("col.list.element.list", subList); err != nil {
			t.Fatal(err)
		}
		if err = optionalList4.Set("col.list.element.list.element", optionalSubElement); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalList4.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	result1 := map[string]*Column{
		"col.list.element.list.element": {
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

	result2 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30, v10, v20, v10, v30},
			definitionLevels: []int64{2, 2, 2, 2, 2, 2, 2},
			repetitionLevels: []int64{0, 2, 1, 2, 1, 2, 2},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	result3 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result4 := map[string]*Column{
		"col.list.element.list.element": {
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

	result5 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30, v10, v20, v10, v30},
			definitionLevels: []int64{3, 3, 3, 3, 3, 3, 3},
			repetitionLevels: []int64{0, 2, 1, 2, 1, 2, 2},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	result6 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{3},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result7 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{4},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result8 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30, v10, v20, v10, v30},
			definitionLevels: []int64{4, 4, 4, 4, 4, 4, 4},
			repetitionLevels: []int64{0, 2, 1, 2, 1, 2, 2},
			rowCount:         1,
			maxBitWidth:      5,
			minValue:         v10,
			maxValue:         v30,
		},
	}

	result9 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result10 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{4},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result11 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{5},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result12 := map[string]*Column{
		"col.list.element.list.element": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10, v20, v30, v10, v20, v10, v30},
			definitionLevels: []int64{5, 5, 5, 5, 5, 5, 5},
			repetitionLevels: []int64{0, 2, 1, 2, 1, 2, 2},
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
		{requiredList1, `{}`, nil, true},                // err: col: nil value for required field
		{requiredList1, `{"col": null}`, nil, true},     // err: col: nil value for required field
		{requiredList1, `{"col": [[null]]}`, nil, true}, // err: col.list.element.list.element: nil value for required field
		{requiredList1, `{"col": [[10]]}`, result1, false},
		{requiredList1, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result2, false},
		{requiredList2, `{}`, nil, true},            // err: col: nil value for required field
		{requiredList2, `{"col": null}`, nil, true}, // err: col: nil value for required field
		{requiredList2, `{"col": [[null]]}`, result3, false},
		{requiredList2, `{"col": [[10]]}`, result4, false},
		{requiredList2, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result5, false},
		{requiredList3, `{}`, nil, true},                // err: col: nil value for required field
		{requiredList3, `{"col": null}`, nil, true},     // err: col: nil value for required field
		{requiredList3, `{"col": [[null]]}`, nil, true}, // err: col.list.element.list.element: nil value for required field
		{requiredList3, `{"col": [[10]]}`, result4, false},
		{requiredList3, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result5, false},
		{requiredList4, `{}`, nil, true},            // err: col: nil value for required field
		{requiredList4, `{"col": null}`, nil, true}, // err: col: nil value for required field
		{requiredList4, `{"col": [[null]]}`, result6, false},
		{requiredList4, `{"col": [[10]]}`, result7, false},
		{requiredList4, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result8, false},
		{optionalList1, `{}`, result9, false},
		{optionalList1, `{"col": null}`, result9, false},
		{optionalList1, `{"col": [[null]]}`, nil, true}, // err: col.list.element.list.element: nil value for required field
		{optionalList1, `{"col": [[10]]}`, result4, false},
		{optionalList1, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result5, false},
		{optionalList2, `{}`, result9, false},
		{optionalList2, `{"col": null}`, result9, false},
		{optionalList2, `{"col": [[null]]}`, result6, false},
		{optionalList2, `{"col": [[10]]}`, result7, false},
		{optionalList2, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result8, false},
		{optionalList3, `{}`, result9, false},
		{optionalList3, `{"col": null}`, result9, false},
		{optionalList3, `{"col": [[null]]}`, nil, true}, // err: col.list.element.list.element: nil value for required field
		{optionalList3, `{"col": [[10]]}`, result7, false},
		{optionalList3, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result8, false},
		{optionalList4, `{}`, result9, false},
		{optionalList4, `{"col": null}`, result9, false},
		{optionalList4, `{"col": [[null]]}`, result10, false},
		{optionalList4, `{"col": [[10]]}`, result11, false},
		{optionalList4, `{"col": [[10, 20], [30, 10], [20, 10, 30]]}`, result12, false},
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
