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
	"encoding/binary"
	"encoding/json"
	"io"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

// GetReaderFunc - function type returning io.ReadCloser for requested offset/length.
type GetReaderFunc func(offset, length int64) (io.ReadCloser, error)

func footerSize(getReaderFunc GetReaderFunc) (size int64, err error) {
	rc, err := getReaderFunc(-8, 4)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	buf := make([]byte, 4)
	if _, err = io.ReadFull(rc, buf); err != nil {
		return 0, err
	}

	size = int64(binary.LittleEndian.Uint32(buf))

	return size, nil
}

func fileMetadata(getReaderFunc GetReaderFunc) (*parquet.FileMetaData, error) {
	size, err := footerSize(getReaderFunc)
	if err != nil {
		return nil, err
	}

	rc, err := getReaderFunc(-(8 + size), size)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	fileMeta := parquet.NewFileMetaData()

	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(rc))
	err = fileMeta.Read(protocol)
	if err != nil {
		return nil, err
	}

	return fileMeta, nil
}

// Value - denotes column value
type Value struct {
	Value  interface{}
	Type   parquet.Type
	Schema *parquet.SchemaElement
}

// MarshalJSON - encodes to JSON data
func (value Value) MarshalJSON() (data []byte, err error) {
	return json.Marshal(value.Value)
}

// Reader - denotes parquet file.
type Reader struct {
	getReaderFunc  GetReaderFunc
	schemaElements []*parquet.SchemaElement
	rowGroups      []*parquet.RowGroup
	rowGroupIndex  int

	nameList    []string
	columnNames set.StringSet
	columns     map[string]*column
	rowIndex    int64
}

// NewReader - creates new parquet reader. Reader calls getReaderFunc to get required data range for given columnNames. If columnNames is empty, all columns are used.
func NewReader(getReaderFunc GetReaderFunc, columnNames set.StringSet) (*Reader, error) {
	fileMeta, err := fileMetadata(getReaderFunc)
	if err != nil {
		return nil, err
	}

	nameList := []string{}
	schemaElements := fileMeta.GetSchema()
	for _, element := range schemaElements {
		nameList = append(nameList, element.Name)
	}

	return &Reader{
		getReaderFunc:  getReaderFunc,
		rowGroups:      fileMeta.GetRowGroups(),
		schemaElements: schemaElements,
		nameList:       nameList,
		columnNames:    columnNames,
	}, nil
}

// Read - reads single record.
func (reader *Reader) Read() (record *Record, err error) {
	if reader.rowGroupIndex >= len(reader.rowGroups) {
		return nil, io.EOF
	}

	if reader.columns == nil {
		reader.columns, err = getColumns(
			reader.rowGroups[reader.rowGroupIndex],
			reader.columnNames,
			reader.schemaElements,
			reader.getReaderFunc,
		)
		if err != nil {
			return nil, err
		}

		reader.rowIndex = 0
	}

	if reader.rowIndex >= reader.rowGroups[reader.rowGroupIndex].GetNumRows() {
		reader.rowGroupIndex++
		reader.Close()
		return reader.Read()
	}

	record = newRecord(reader.nameList)
	for name := range reader.columns {
		col := reader.columns[name]
		value, valueType, schema := col.read()
		record.set(name, Value{Value: value, Type: valueType, Schema: schema})
	}

	reader.rowIndex++

	return record, nil
}

// Close - closes underneath readers.
func (reader *Reader) Close() (err error) {
	for _, column := range reader.columns {
		column.close()
	}

	reader.columns = nil
	reader.rowIndex = 0

	return nil
}
