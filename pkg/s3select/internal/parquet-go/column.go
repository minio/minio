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

import (
	"errors"
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func getColumns(
	rowGroup *parquet.RowGroup,
	columnNames set.StringSet,
	schemaElements []*parquet.SchemaElement,
	getReaderFunc GetReaderFunc,
) (nameColumnMap map[string]*column, err error) {
	nameIndexMap := make(map[string]int)
	for colIndex, columnChunk := range rowGroup.GetColumns() {
		meta := columnChunk.GetMetaData()
		if meta == nil {
			return nil, errors.New("parquet: column metadata missing")
		}
		columnName := strings.Join(meta.GetPathInSchema(), ".")
		if columnNames != nil && !columnNames.Contains(columnName) {
			continue
		}

		// Ignore column spanning into another file.
		if columnChunk.GetFilePath() != "" {
			continue
		}

		offset := meta.GetDataPageOffset()
		if meta.DictionaryPageOffset != nil {
			offset = meta.GetDictionaryPageOffset()
		}

		size := meta.GetTotalCompressedSize()
		if size < 0 {
			return nil, errors.New("parquet: negative compressed size")
		}
		rc, err := getReaderFunc(offset, size)
		if err != nil {
			return nil, err
		}

		thriftReader := thrift.NewTBufferedTransport(thrift.NewStreamTransportR(rc), int(size))

		if nameColumnMap == nil {
			nameColumnMap = make(map[string]*column)
		}
		var se *parquet.SchemaElement
		for _, schema := range schemaElements {
			if schema != nil && schema.Name == columnName {
				se = schema
				break
			}
		}

		nameColumnMap[columnName] = &column{
			name:           columnName,
			metadata:       meta,
			schema:         se,
			schemaElements: schemaElements,
			rc:             rc,
			thriftReader:   thriftReader,
			valueType:      meta.GetType(),
		}

		// First element of []*parquet.SchemaElement from parquet file metadata is 'schema'
		// which is always skipped, hence colIndex + 1 is valid.
		nameIndexMap[columnName] = colIndex + 1
	}

	for name := range nameColumnMap {
		nameColumnMap[name].nameIndexMap = nameIndexMap
	}

	return nameColumnMap, nil
}

type column struct {
	name           string
	endOfValues    bool
	valueIndex     int
	valueType      parquet.Type
	metadata       *parquet.ColumnMetaData
	schema         *parquet.SchemaElement
	schemaElements []*parquet.SchemaElement
	nameIndexMap   map[string]int
	dictPage       *page
	dataTable      *table
	rc             io.ReadCloser
	thriftReader   *thrift.TBufferedTransport
}

func (column *column) close() (err error) {
	if column.rc != nil {
		err = column.rc.Close()
		column.rc = nil
	}

	return err
}

func (column *column) readPage() {
	page, _, _, err := readPage(
		column.thriftReader,
		column.metadata,
		column.nameIndexMap,
		column.schemaElements,
	)

	if err != nil {
		column.endOfValues = true
		return
	}

	if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		column.dictPage = page
		column.readPage()
		return
	}

	page.decode(column.dictPage)

	if column.dataTable == nil {
		column.dataTable = newTableFromTable(page.DataTable)
	}

	column.dataTable.Merge(page.DataTable)
}

func (column *column) read() (value interface{}, valueType parquet.Type, cnv *parquet.SchemaElement) {
	if column.dataTable == nil {
		column.readPage()
		column.valueIndex = 0
	}

	if column.endOfValues {
		return nil, column.metadata.GetType(), column.schema
	}

	value = column.dataTable.Values[column.valueIndex]
	column.valueIndex++
	if len(column.dataTable.Values) == column.valueIndex {
		column.dataTable = nil
	}

	return value, column.metadata.GetType(), column.schema
}
