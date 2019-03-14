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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/gen-go/parquet"
)

const (
	defaultPageSize     = 8 * 1024          // 8 KiB
	defaultRowGroupSize = 128 * 1024 * 1024 // 128 MiB
)

// Writer - represents parquet writer.
type Writer struct {
	PageSize        int64
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec

	writeCloser    io.WriteCloser
	size           int64
	numRows        int64
	offset         int64
	pagesMapBuf    map[string][]*page
	dictRecs       map[string]*dictRec
	footer         *parquet.FileMetaData
	schemaElements []*parquet.SchemaElement
	rowGroupCount  int
	records        []map[string]*Value
}

func (writer *Writer) writeRecords() (err error) {
	if len(writer.records) == 0 {
		return nil
	}

	tableMap := make(map[string]*table)
	for _, schema := range writer.schemaElements {
		if schema.GetNumChildren() != 0 {
			continue
		}

		table := new(table)
		table.Path = strings.Split(schema.Name, ".")
		table.MaxDefinitionLevel = 0
		table.MaxRepetitionLevel = 0
		table.RepetitionType = schema.GetRepetitionType()
		table.Type = schema.GetType()
		table.ConvertedType = -1

		for _, record := range writer.records {
			value := record[schema.Name]
			if *schema.Type != value.Type {
				return fmt.Errorf("schema.Type and value.Type are not same")
			}

			switch value.Type {
			case parquet.Type_BOOLEAN:
				table.Values = append(table.Values, value.Value.(bool))
			case parquet.Type_INT32:
				table.Values = append(table.Values, value.Value.(int32))
			case parquet.Type_INT64:
				table.Values = append(table.Values, value.Value.(int64))
			case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
				table.Values = append(table.Values, value.Value.([]byte))
			case parquet.Type_FLOAT:
				table.Values = append(table.Values, value.Value.(float32))
			case parquet.Type_DOUBLE:
				table.Values = append(table.Values, value.Value.(float64))
			default:
				return fmt.Errorf("unknown parquet type %v", value.Type)
			}

			table.DefinitionLevels = append(table.DefinitionLevels, 0)
			table.RepetitionLevels = append(table.RepetitionLevels, 0)
		}

		tableMap[schema.Name] = table
	}

	pagesMap := make(map[string][]*page)
	for name, table := range tableMap {
		if table.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
			if _, ok := writer.dictRecs[name]; !ok {
				writer.dictRecs[name] = newDictRec(table.Type)
			}
			pagesMap[name], _ = tableToDictDataPages(writer.dictRecs[name], table, int32(writer.PageSize), 32, writer.CompressionType)
		} else {
			pagesMap[name], _ = tableToDataPages(table, int32(writer.PageSize), writer.CompressionType)
		}
	}

	recordsSize := int64(0)
	for name, pages := range pagesMap {
		if _, ok := writer.pagesMapBuf[name]; !ok {
			writer.pagesMapBuf[name] = pages
		} else {
			writer.pagesMapBuf[name] = append(writer.pagesMapBuf[name], pages...)
		}
		for _, page := range pages {
			recordsSize += int64(len(page.RawData))
			writer.size += int64(len(page.RawData))
			// As we got raw data, we don't need data table here after
			// page.DataTable = nil
		}
	}

	writer.numRows += int64(len(writer.records))

	// if len(writer.pagesMapBuf) > 0 && writer.size+recordsSize >= writer.RowGroupSize {
	if len(writer.pagesMapBuf) > 0 {
		//pages -> chunk
		chunkMap := make(map[string]*columnChunk)
		for name, pages := range writer.pagesMapBuf {
			// FIXME: add page encoding support.
			// if len(pages) > 0 && pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY {
			// 	dictPage, _ := dictoRecToDictPage(writer.dictRecs[name], int32(writer.PageSize), writer.CompressionType)
			// 	tmp := append([]*page{dictPage}, pages...)
			// 	chunkMap[name] = pagesToDictColumnChunk(tmp)
			// } else {
			// 	chunkMap[name] = pagesToColumnChunk(pages)
			// }

			chunkMap[name] = pagesToColumnChunk(pages)
		}

		writer.dictRecs = make(map[string]*dictRec)

		//chunks -> rowGroup
		rowGroup := newRowGroup()
		rowGroup.RowGroupHeader.Columns = []*parquet.ColumnChunk{}

		for k := 0; k < len(writer.schemaElements); k++ {
			//for _, chunk := range chunkMap {
			schema := writer.schemaElements[k]
			if schema.GetNumChildren() > 0 {
				continue
			}
			chunk := chunkMap[schema.Name]
			if chunk == nil {
				continue
			}
			rowGroup.Chunks = append(rowGroup.Chunks, chunk)
			rowGroup.RowGroupHeader.TotalByteSize += chunk.chunkHeader.MetaData.TotalCompressedSize
			rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.chunkHeader)
		}
		rowGroup.RowGroupHeader.NumRows = writer.numRows
		writer.numRows = 0

		for k := 0; k < len(rowGroup.Chunks); k++ {
			rowGroup.Chunks[k].chunkHeader.MetaData.DataPageOffset = -1
			rowGroup.Chunks[k].chunkHeader.FileOffset = writer.offset

			for l := 0; l < len(rowGroup.Chunks[k].Pages); l++ {
				switch {
				case rowGroup.Chunks[k].Pages[l].Header.Type == parquet.PageType_DICTIONARY_PAGE:
					offset := writer.offset
					rowGroup.Chunks[k].chunkHeader.MetaData.DictionaryPageOffset = &offset
				case rowGroup.Chunks[k].chunkHeader.MetaData.DataPageOffset <= 0:
					rowGroup.Chunks[k].chunkHeader.MetaData.DataPageOffset = writer.offset
				}

				data := rowGroup.Chunks[k].Pages[l].RawData
				if _, err = writer.writeCloser.Write(data); err != nil {
					return err
				}

				writer.offset += int64(len(data))
			}
		}

		writer.footer.RowGroups = append(writer.footer.RowGroups, rowGroup.RowGroupHeader)
		writer.size = 0
		writer.pagesMapBuf = make(map[string][]*page)
	}

	writer.footer.NumRows += int64(len(writer.records))
	writer.records = writer.records[:0]

	return nil
}

// Write - writes a single record. The actual binary data write happens once rowGroupCount records are cached.
func (writer *Writer) Write(record map[string]*Value) (err error) {
	writer.records = append(writer.records, record)
	if len(writer.records) != writer.rowGroupCount {
		return nil
	}

	return writer.writeRecords()
}

func (writer *Writer) finalize() (err error) {
	if err = writer.writeRecords(); err != nil {
		return err
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	footerBuf, err := ts.Write(context.TODO(), writer.footer)
	if err != nil {
		return err
	}

	if _, err = writer.writeCloser.Write(footerBuf); err != nil {
		return err
	}

	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = writer.writeCloser.Write(footerSizeBuf); err != nil {
		return err
	}

	_, err = writer.writeCloser.Write([]byte("PAR1"))
	return err
}

// Close - finalizes and closes writer. If any pending records are available, they are written here.
func (writer *Writer) Close() (err error) {
	if err = writer.finalize(); err != nil {
		return err
	}

	return writer.writeCloser.Close()
}

// NewWriter - creates new parquet writer. Binary data of rowGroupCount records are written to writeCloser.
func NewWriter(writeCloser io.WriteCloser, schemaElements []*parquet.SchemaElement, rowGroupCount int) (*Writer, error) {
	if _, err := writeCloser.Write([]byte("PAR1")); err != nil {
		return nil, err
	}

	footer := parquet.NewFileMetaData()
	footer.Version = 1
	numChildren := int32(len(schemaElements))
	footer.Schema = append(footer.Schema, &parquet.SchemaElement{
		Name:           "schema",
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		NumChildren:    &numChildren,
	})
	footer.Schema = append(footer.Schema, schemaElements...)

	return &Writer{
		PageSize:        defaultPageSize,
		RowGroupSize:    defaultRowGroupSize,
		CompressionType: parquet.CompressionCodec_SNAPPY,

		writeCloser:    writeCloser,
		offset:         4,
		pagesMapBuf:    make(map[string][]*page),
		dictRecs:       make(map[string]*dictRec),
		footer:         footer,
		schemaElements: schemaElements,
		rowGroupCount:  rowGroupCount,
	}, nil
}
