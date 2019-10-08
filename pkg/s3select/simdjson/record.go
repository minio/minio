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

package simdjson

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/bcicen/jstream"
	"github.com/minio/minio/pkg/s3select/sql"
)

// Record - is JSON record.
type Record struct {
	// Used in Set(), Marshal*()
	j ParsedJson

	// field name to Tape index lookup
	rootFields map[string]int

	SelectFormat sql.SelectObjectFormat
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*sql.Value, error) {
	if len(r.rootFields) == 0 {
		r.indexFields()
	}
	// FIXME:
	return nil, nil
}

func (r *Record) indexFields() {
	i := 0
	for i < len(r.j.Tape) {

	}
	r.rootFields["indexed___"] = -1
}

// Reset the record.
func (r *Record) Reset() {
	for k := range r.rootFields {
		delete(r.rootFields, k)
	}
	r.j.Reset()
}

// Clone the record and if possible use the destination provided.
func (r *Record) Clone(dst sql.Record) sql.Record {
	other, ok := dst.(*Record)
	if !ok {
		other = &Record{}
	}
	/*
		if len(other.KVS) > 0 {
			other.KVS = other.KVS[:0]
		}
		other.KVS = append(other.KVS, r.KVS...)
	*/
	return other
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) error {
	var v interface{}
	if b, ok := value.ToBool(); ok {
		v = b
	} else if f, ok := value.ToFloat(); ok {
		v = f
	} else if i, ok := value.ToInt(); ok {
		v = i
	} else if t, ok := value.ToTimestamp(); ok {
		v = sql.FormatSQLTimestamp(t)
	} else if s, ok := value.ToString(); ok {
		v = s
	} else if value.IsNull() {
		v = nil
	} else if b, ok := value.ToBytes(); ok {
		v = b
		//} else if arr, ok := value.ToArray(); ok {
		//	v = arr
	} else {
		return fmt.Errorf("unsupported sql value %v and type %v", value, value.GetTypeString())
	}

	name = strings.Replace(name, "*", "__ALL__", -1)
	_ = v
	//r.KVS = append(r.KVS, jstream.KV{Key: name, Value: v})
	return nil
}

// WriteCSV - encodes to CSV data.
func (r *Record) WriteCSV(writer io.Writer, fieldDelimiter rune) error {
	var csvRecord []string
	/*
		for _, kv := range r.KVS {
			var columnValue string
			switch val := kv.Value.(type) {
			case bool, float64, int64, string:
				columnValue = fmt.Sprintf("%v", val)
			case nil:
				columnValue = ""
			case RawJSON:
				columnValue = string([]byte(val))
			case []interface{}:
				b, err := json.Marshal(val)
				if err != nil {
					return err
				}
				columnValue = string(b)
			default:
				return fmt.Errorf("Cannot marshal unhandled type: %T", kv.Value)
			}
			csvRecord = append(csvRecord, columnValue)
		}
	*/
	w := csv.NewWriter(writer)
	w.Comma = fieldDelimiter
	if err := w.Write(csvRecord); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	return nil
}

// Raw - returns the underlying representation.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	// FIXME:
	return r.SelectFormat, nil
}

// WriteJSON - encodes to JSON data.
func (r *Record) WriteJSON(writer io.Writer) error {
	// FIXME:
	return json.NewEncoder(writer).Encode(nil)
}

// Replace the underlying buffer of json data.
func (r *Record) Replace(k jstream.KVS) error {
	// FIXME:
	return nil
}

// NewRecord - creates new empty JSON record.
func NewRecord(f sql.SelectObjectFormat) *Record {
	return &Record{
		//KVS:          jstream.KVS{},
		SelectFormat: f,
	}
}
