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
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

// ColumnChunk ...
type ColumnChunk struct {
	parquet.ColumnChunk
	isDictPage  bool
	dictPageLen int64
	dataPageLen int64
	dataLen     int64
	data        []byte
}

// Data returns the data.
func (chunk *ColumnChunk) Data() []byte {
	return chunk.data
}

// DataLen returns the length of the data.
func (chunk *ColumnChunk) DataLen() int64 {
	return chunk.dataLen
}

// NewRowGroup creates a new row group.
func NewRowGroup(chunks []*ColumnChunk, numRows, offset int64) *parquet.RowGroup {
	rows := parquet.NewRowGroup()
	rows.NumRows = numRows

	for _, chunk := range chunks {
		rows.Columns = append(rows.Columns, &chunk.ColumnChunk)
		rows.TotalByteSize += chunk.dataLen

		chunk.ColumnChunk.FileOffset = offset

		if chunk.isDictPage {
			dictPageOffset := offset
			chunk.ColumnChunk.MetaData.DictionaryPageOffset = &dictPageOffset
			offset += chunk.dictPageLen
		}

		chunk.ColumnChunk.MetaData.DataPageOffset = offset
		offset += chunk.dataPageLen
	}

	return rows
}
