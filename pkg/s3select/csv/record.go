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
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bcicen/jstream"
	csv "github.com/minio/minio/pkg/csvparser"
	"github.com/minio/minio/pkg/s3select/sql"
)

// Record - is a CSV record.
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
func (r *Record) Set(name string, value *sql.Value) (sql.Record, error) {
	r.columnNames = append(r.columnNames, name)
	r.csvRecord = append(r.csvRecord, value.CSVString())
	return r, nil
}

// Reset data in record.
func (r *Record) Reset() {
	if len(r.columnNames) > 0 {
		r.columnNames = r.columnNames[:0]
	}
	if len(r.csvRecord) > 0 {
		r.csvRecord = r.csvRecord[:0]
	}
	for k := range r.nameIndexMap {
		delete(r.nameIndexMap, k)
	}
}

// Clone the record.
func (r *Record) Clone(dst sql.Record) sql.Record {
	other, ok := dst.(*Record)
	if !ok {
		other = &Record{}
	}
	if len(other.columnNames) > 0 {
		other.columnNames = other.columnNames[:0]
	}
	if len(other.csvRecord) > 0 {
		other.csvRecord = other.csvRecord[:0]
	}
	other.columnNames = append(other.columnNames, r.columnNames...)
	other.csvRecord = append(other.csvRecord, r.csvRecord...)
	return other
}

// WriteCSV - encodes to CSV data.
func (r *Record) WriteCSV(writer io.Writer, opts sql.WriteCSVOpts) error {
	w := csv.NewWriter(writer)
	w.Comma = opts.FieldDelimiter
	w.AlwaysQuote = opts.AlwaysQuote
	w.Quote = opts.Quote
	w.QuoteEscape = opts.QuoteEscape
	if err := w.Write(r.csvRecord); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

// WriteJSON - encodes to JSON data.
func (r *Record) WriteJSON(writer io.Writer) error {
	var kvs jstream.KVS = make([]jstream.KV, len(r.columnNames))
	for i := 0; i < len(r.columnNames); i++ {
		kvs[i] = jstream.KV{Key: r.columnNames[i], Value: r.csvRecord[i]}
	}
	return json.NewEncoder(writer).Encode(kvs)
}

// Raw - returns the underlying data with format info.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	return sql.SelectFmtCSV, r
}

// Replace - is not supported for CSV
func (r *Record) Replace(_ interface{}) error {
	return errors.New("Replace is not supported for CSV")
}

// NewRecord - creates new CSV record.
func NewRecord() *Record {
	return &Record{}
}
