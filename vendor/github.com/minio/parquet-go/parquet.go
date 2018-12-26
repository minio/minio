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
	"encoding/binary"
	"encoding/json"
	"io"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/parquet-go/gen-go/parquet"
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
	Value interface{}
	Type  parquet.Type
}

// MarshalJSON - encodes to JSON data
func (value Value) MarshalJSON() (data []byte, err error) {
	return json.Marshal(value.Value)
}

// File - denotes parquet file.
type File struct {
	getReaderFunc  GetReaderFunc
	schemaElements []*parquet.SchemaElement
	rowGroups      []*parquet.RowGroup
	rowGroupIndex  int

	columnNames set.StringSet
	columns     map[string]*column
	rowIndex    int64
}

// Open - opens parquet file with given column names.
func Open(getReaderFunc GetReaderFunc, columnNames set.StringSet) (*File, error) {
	fileMeta, err := fileMetadata(getReaderFunc)
	if err != nil {
		return nil, err
	}

	return &File{
		getReaderFunc:  getReaderFunc,
		rowGroups:      fileMeta.GetRowGroups(),
		schemaElements: fileMeta.GetSchema(),
		columnNames:    columnNames,
	}, nil
}

// Read - reads single record.
func (file *File) Read() (record map[string]Value, err error) {
	if file.rowGroupIndex >= len(file.rowGroups) {
		return nil, io.EOF
	}

	if file.columns == nil {
		file.columns, err = getColumns(
			file.rowGroups[file.rowGroupIndex],
			file.columnNames,
			file.schemaElements,
			file.getReaderFunc,
		)
		if err != nil {
			return nil, err
		}

		file.rowIndex = 0
	}

	if file.rowIndex >= file.rowGroups[file.rowGroupIndex].GetNumRows() {
		file.rowGroupIndex++
		file.Close()
		return file.Read()
	}

	record = make(map[string]Value)
	for name := range file.columns {
		value, valueType := file.columns[name].read()
		record[name] = Value{value, valueType}
	}

	file.rowIndex++

	return record, nil
}

// Close - closes underneath readers.
func (file *File) Close() (err error) {
	if file.columns != nil {
		return nil
	}

	for _, column := range file.columns {
		column.close()
	}

	file.columns = nil
	file.rowIndex = 0

	return nil
}
