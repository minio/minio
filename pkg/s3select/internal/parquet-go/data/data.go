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
