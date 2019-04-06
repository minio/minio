/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package csv

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bcicen/jstream"
	"github.com/minio/minio/pkg/s3select/sql"
)

// Record - is CSV record.
type Record struct {
	columnNames  []string
	csvRecord    []string
	nameIndexMap map[string]int64
}

// Get - gets the value for a column name. CSV fields do not have any
// defined type (other than the default string). So this function
// always returns fields using sql.FromBytes so that the type
// specified/implied by the query can be used, or can be automatically
// converted based on the query.
func (r *Record) Get(name string) (*sql.Value, error) {
	index, found := r.nameIndexMap[name]
	if !found {
		return nil, fmt.Errorf("column %v not found", name)
	}

	if index >= int64(len(r.csvRecord)) {
		// No value found for column 'name', hence return null
		// value
		return sql.FromNull(), nil
	}

	return sql.FromBytes([]byte(r.csvRecord[index])), nil
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) error {
	r.columnNames = append(r.columnNames, name)
	r.csvRecord = append(r.csvRecord, value.CSVString())
	return nil
}

// MarshalCSV - encodes to CSV data.
func (r *Record) MarshalCSV(fieldDelimiter rune) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)
	w.Comma = fieldDelimiter
	if err := w.Write(r.csvRecord); err != nil {
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	data := buf.Bytes()
	return data[:len(data)-1], nil
}

// MarshalJSON - encodes to JSON data.
func (r *Record) MarshalJSON() ([]byte, error) {
	var kvs jstream.KVS = make([]jstream.KV, len(r.columnNames))
	for i := 0; i < len(r.columnNames); i++ {
		kvs[i] = jstream.KV{Key: r.columnNames[i], Value: r.csvRecord[i]}
	}
	return json.Marshal(kvs)
}

// Raw - returns the underlying data with format info.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	return sql.SelectFmtCSV, r
}

// Replace - is not supported for CSV
func (r *Record) Replace(_ jstream.KVS) error {
	return errors.New("Replace is not supported for CSV")
}

// NewRecord - creates new CSV record.
func NewRecord() *Record {
	return &Record{}
}
