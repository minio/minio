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

func TestPopulatePrimitiveType(t *testing.T) {
	requiredField := schema.NewTree()
	{
		requiredCol, err := schema.NewElement("col", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = requiredField.Set("col", requiredCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err = requiredField.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	optionalField := schema.NewTree()
	{
		optionalCol, err := schema.NewElement("col", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err = optionalField.Set("col", optionalCol); err != nil {
			t.Fatal(err)
		}

		if _, _, err = optionalField.ToParquetSchema(); err != nil {
			t.Fatal(err)
		}
	}

	result1 := map[string]*Column{
		"col": {
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
		"col": {
			parquetType:      parquet.Type_INT32,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
			rowCount:         1,
		},
	}

	result3 := map[string]*Column{
		"col": {
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

	testCases := []struct {
		schemaTree     *schema.Tree
		data           string
		expectedResult map[string]*Column
		expectErr      bool
	}{
		{requiredField, `{}`, nil, true},
		{requiredField, `{"col": null}`, nil, true}, // err: col: nil value for required field
		{requiredField, `{"col": 10}`, result1, false},
		{optionalField, `{}`, result2, false},
		{optionalField, `{"col": null}`, result2, false},
		{optionalField, `{"col": 10}`, result3, false},
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
