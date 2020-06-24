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

import (
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio-go/v6/pkg/set"
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

		rc, err := getReaderFunc(offset, size)
		if err != nil {
			return nil, err
		}

		thriftReader := thrift.NewTBufferedTransport(thrift.NewStreamTransportR(rc), int(size))

		if nameColumnMap == nil {
			nameColumnMap = make(map[string]*column)
		}

		nameColumnMap[columnName] = &column{
			name:           columnName,
			metadata:       meta,
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

func (column *column) read() (value interface{}, valueType parquet.Type) {
	if column.dataTable == nil {
		column.readPage()
		column.valueIndex = 0
	}

	if column.endOfValues {
		return nil, column.metadata.GetType()
	}

	value = column.dataTable.Values[column.valueIndex]
	column.valueIndex++
	if len(column.dataTable.Values) == column.valueIndex {
		column.dataTable = nil
	}

	return value, column.metadata.GetType()
}
