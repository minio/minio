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

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/data"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
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

	writeCloser   io.WriteCloser
	numRows       int64
	offset        int64
	footer        *parquet.FileMetaData
	schemaTree    *schema.Tree
	valueElements []*schema.Element
	columnDataMap map[string]*data.Column
	rowGroupCount int
}

func (writer *Writer) writeData() (err error) {
	if writer.numRows == 0 {
		return nil
	}

	var chunks []*data.ColumnChunk
	for _, element := range writer.valueElements {
		name := element.PathInTree
		columnData, found := writer.columnDataMap[name]
		if !found {
			continue
		}

		columnChunk := columnData.Encode(element)
		chunks = append(chunks, columnChunk)
	}

	rowGroup := data.NewRowGroup(chunks, writer.numRows, writer.offset)

	for _, chunk := range chunks {
		if _, err = writer.writeCloser.Write(chunk.Data()); err != nil {
			return err
		}

		writer.offset += chunk.DataLen()
	}

	writer.footer.RowGroups = append(writer.footer.RowGroups, rowGroup)
	writer.footer.NumRows += writer.numRows

	writer.numRows = 0
	writer.columnDataMap = nil
	return nil
}

// WriteJSON - writes a record represented in JSON.
func (writer *Writer) WriteJSON(recordData []byte) (err error) {
	columnDataMap, err := data.UnmarshalJSON(recordData, writer.schemaTree)
	if err != nil {
		return err
	}

	return writer.Write(columnDataMap)
}

// Write - writes a record represented in map.
func (writer *Writer) Write(record map[string]*data.Column) (err error) {
	if writer.columnDataMap == nil {
		writer.columnDataMap = record
	} else {
		for name, columnData := range record {
			var found bool
			var element *schema.Element
			for _, element = range writer.valueElements {
				if element.PathInTree == name {
					found = true
					break
				}
			}

			if !found {
				return fmt.Errorf("%v is not value column", name)
			}

			writer.columnDataMap[name].Merge(columnData)
		}
	}

	writer.numRows++
	if writer.numRows == int64(writer.rowGroupCount) {
		return writer.writeData()
	}

	return nil
}

func (writer *Writer) finalize() (err error) {
	if err = writer.writeData(); err != nil {
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
func NewWriter(writeCloser io.WriteCloser, schemaTree *schema.Tree, rowGroupCount int) (*Writer, error) {
	if _, err := writeCloser.Write([]byte("PAR1")); err != nil {
		return nil, err
	}

	schemaList, valueElements, err := schemaTree.ToParquetSchema()
	if err != nil {
		return nil, err
	}

	footer := parquet.NewFileMetaData()
	footer.Version = 1
	footer.Schema = schemaList

	return &Writer{
		PageSize:        defaultPageSize,
		RowGroupSize:    defaultRowGroupSize,
		CompressionType: parquet.CompressionCodec_SNAPPY,

		writeCloser:   writeCloser,
		offset:        4,
		footer:        footer,
		schemaTree:    schemaTree,
		valueElements: valueElements,
		rowGroupCount: rowGroupCount,
	}, nil
}
