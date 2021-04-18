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

import "github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"

func getTableValues(values interface{}, valueType parquet.Type) (tableValues []interface{}) {
	return valuesToInterfaces(values, valueType)
}

type table struct {
	RepetitionType     parquet.FieldRepetitionType
	Type               parquet.Type
	MaxDefinitionLevel int32
	MaxRepetitionLevel int32
	Path               []string      // Path of this column
	Values             []interface{} // Parquet values
	DefinitionLevels   []int32       // Definition Levels slice
	RepetitionLevels   []int32       // Repetition Levels slice
	ConvertedType      parquet.ConvertedType
	Encoding           parquet.Encoding
	BitWidth           int32
}

func newTableFromTable(srcTable *table) *table {
	if srcTable == nil {
		return nil
	}

	return &table{
		Type: srcTable.Type,
		Path: append([]string{}, srcTable.Path...),
	}
}

func (table *table) Merge(tables ...*table) {
	for i := 0; i < len(tables); i++ {
		if tables[i] == nil {
			continue
		}

		table.Values = append(table.Values, tables[i].Values...)
		table.RepetitionLevels = append(table.RepetitionLevels, tables[i].RepetitionLevels...)
		table.DefinitionLevels = append(table.DefinitionLevels, tables[i].DefinitionLevels...)

		if table.MaxDefinitionLevel < tables[i].MaxDefinitionLevel {
			table.MaxDefinitionLevel = tables[i].MaxDefinitionLevel
		}

		if table.MaxRepetitionLevel < tables[i].MaxRepetitionLevel {
			table.MaxRepetitionLevel = tables[i].MaxRepetitionLevel
		}
	}
}

func (table *table) Pop(numRows int64) *table {
	result := newTableFromTable(table)
	var i, num int64
	for i = int64(0); i < int64(len(table.Values)); i++ {
		if table.RepetitionLevels[i] == 0 {
			if num >= numRows {
				break
			}

			num++
		}

		if result.MaxRepetitionLevel < table.RepetitionLevels[i] {
			result.MaxRepetitionLevel = table.RepetitionLevels[i]
		}

		if result.MaxDefinitionLevel < table.DefinitionLevels[i] {
			result.MaxDefinitionLevel = table.DefinitionLevels[i]
		}
	}

	result.RepetitionLevels = table.RepetitionLevels[:i]
	result.DefinitionLevels = table.DefinitionLevels[:i]
	result.Values = table.Values[:i]

	table.RepetitionLevels = table.RepetitionLevels[i:]
	table.DefinitionLevels = table.DefinitionLevels[i:]
	table.Values = table.Values[i:]

	return result
}
