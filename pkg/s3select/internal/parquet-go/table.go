/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
