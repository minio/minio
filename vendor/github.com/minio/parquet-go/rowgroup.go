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
	"strings"

	"github.com/minio/parquet-go/gen-go/parquet"
)

type rowGroup struct {
	Chunks         []*columnChunk
	RowGroupHeader *parquet.RowGroup
}

func newRowGroup() *rowGroup {
	return &rowGroup{
		RowGroupHeader: parquet.NewRowGroup(),
	}
}

func (rg *rowGroup) rowGroupToTableMap() map[string]*table {
	tableMap := make(map[string]*table)
	for _, chunk := range rg.Chunks {
		columnPath := ""
		for _, page := range chunk.Pages {
			if columnPath == "" {
				columnPath = strings.Join(page.DataTable.Path, ".")
			}

			if _, ok := tableMap[columnPath]; !ok {
				tableMap[columnPath] = newTableFromTable(page.DataTable)
			}

			tableMap[columnPath].Merge(page.DataTable)
		}
	}

	return tableMap
}
