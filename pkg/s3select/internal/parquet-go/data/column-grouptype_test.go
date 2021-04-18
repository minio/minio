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
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
)

func TestPopulateGroupType(t *testing.T) {
	requiredGroup1 := schema.NewTree()
	{
		requiredGroup, err := schema.NewElement("group", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredGroup1.Set("group", requiredGroup); err != nil {
			t.Fatal(err)
		}
		if err = requiredGroup1.Set("group.col", requiredCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err := requiredGroup1.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	requiredGroup2 := schema.NewTree()
	{
		requiredGroup, err := schema.NewElement("group", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredGroup2.Set("group", requiredGroup); err != nil {
			t.Fatal(err)
		}
		if err = requiredGroup2.Set("group.col", optionalCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err := requiredGroup2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalGroup1 := schema.NewTree()
	{
		optionalGroup, err := schema.NewElement("group", parquet.FieldRepetitionType_OPTIONAL,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalGroup1.Set("group", optionalGroup); err != nil {
			t.Fatal(err)
		}
		if err = optionalGroup1.Set("group.col", requiredCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err := optionalGroup1.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalGroup2 := schema.NewTree()
	{
		optionalGroup, err := schema.NewElement("group", parquet.FieldRepetitionType_OPTIONAL,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		optionalCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalGroup2.Set("group", optionalGroup); err != nil {
			t.Fatal(err)
		}
		if err = optionalGroup2.Set("group.col", optionalCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err := optionalGroup2.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	result1 := map[string]*Column{
		"group.col": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{v10},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
			rowCount:         1,
			maxBitWidth:      4,
			minValue:         v10,
			maxValue:         v10,
		},
	}

	result2 := map[string]*Column{
		"group.col": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result3 := map[string]*Column{
		"group.col": {
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

	result4 := map[string]*Column{
		"group.col": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{1},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result5 := map[string]*Column{
		"group.col": {
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

	testCases := []struct {
		schemaTree     *schema.Tree
		data           string
		expectedResult map[string]*Column
		expectErr      bool
	}{
		{requiredGroup1, `{}`, nil, true},                       // err: group: nil value for required field
		{requiredGroup1, `{"group": null}`, nil, true},          // err: group: nil value for required field
		{requiredGroup1, `{"group": {"col": null}}`, nil, true}, // err: group.col: nil value for required field
		{requiredGroup1, `{"group": {"col": 10}}`, result1, false},
		{requiredGroup2, `{}`, nil, true},              // err: group: nil value for required field
		{requiredGroup2, `{"group": null}`, nil, true}, // err: group: nil value for required field
		{requiredGroup2, `{"group": {"col": null}}`, result2, false},
		{requiredGroup2, `{"group": {"col": 10}}`, result3, false},
		{optionalGroup1, `{}`, result2, false},
		{optionalGroup1, `{"group": null}`, result2, false},
		{optionalGroup1, `{"group": {"col": null}}`, nil, true}, // err: group.col: nil value for required field
		{optionalGroup1, `{"group": {"col": 10}}`, result3, false},
		{optionalGroup2, `{}`, result2, false},
		{optionalGroup2, `{"group": null}`, result2, false},
		{optionalGroup2, `{"group": {"col": null}}`, result4, false},
		{optionalGroup2, `{"group": {"col": 10}}`, result5, false},
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
