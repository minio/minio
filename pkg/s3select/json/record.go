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

package json

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bcicen/jstream"
	"github.com/minio/minio/pkg/s3select/sql"
)

// RawJSON is a byte-slice that contains valid JSON
type RawJSON []byte

// MarshalJSON instance for []byte that assumes that byte-slice is
// already serialized JSON
func (b RawJSON) MarshalJSON() ([]byte, error) {
	return b, nil
}

// Record - is JSON record.
type Record struct {
	// Used in Set(), Marshal*()
	KVS jstream.KVS

	SelectFormat sql.SelectObjectFormat
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*sql.Value, error) {
	// Get is implemented directly in the sql package.
	return nil, errors.New("not implemented here")
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
		v = RawJSON(b)
	} else {
		return fmt.Errorf("unsupported sql value %v and type %v", value, value.GetTypeString())
	}

	name = strings.Replace(name, "*", "__ALL__", -1)
	r.KVS = append(r.KVS, jstream.KV{Key: name, Value: v})
	return nil
}

// MarshalCSV - encodes to CSV data.
func (r *Record) MarshalCSV(fieldDelimiter rune) ([]byte, error) {
	var csvRecord []string
	for _, kv := range r.KVS {
		var columnValue string
		switch val := kv.Value.(type) {
		case bool, float64, int64, string:
			columnValue = fmt.Sprintf("%v", val)
		case nil:
			columnValue = ""
		case RawJSON:
			columnValue = string([]byte(val))
		default:
			return nil, errors.New("Cannot marshal unhandled type")
		}
		csvRecord = append(csvRecord, columnValue)
	}

	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)
	w.Comma = fieldDelimiter
	if err := w.Write(csvRecord); err != nil {
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}

	data := buf.Bytes()
	return data[:len(data)-1], nil
}

// Raw - returns the underlying representation.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	return r.SelectFormat, r.KVS
}

// MarshalJSON - encodes to JSON data.
func (r *Record) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.KVS)
}

// Replace the underlying buffer of json data.
func (r *Record) Replace(k jstream.KVS) error {
	r.KVS = k
	return nil
}

// NewRecord - creates new empty JSON record.
func NewRecord(f sql.SelectObjectFormat) *Record {
	return &Record{
		KVS:          jstream.KVS{},
		SelectFormat: f,
	}
}
