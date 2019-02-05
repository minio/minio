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

package json

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"

	"github.com/minio/minio/pkg/s3select/sql"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Record - is JSON record.
type Record struct {
	data []byte
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*sql.Value, error) {
	result := gjson.GetBytes(r.data, name)
	switch result.Type {
	case gjson.Null:
		return sql.FromNull(), nil
	case gjson.False:
		return sql.FromBool(false), nil
	case gjson.Number:
		return sql.FromFloat(result.Float()), nil
	case gjson.String:
		return sql.FromString(result.String()), nil
	case gjson.True:
		return sql.FromBool(true), nil
	}

	return nil, fmt.Errorf("unsupported gjson value %v; %v", result, result.Type)
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) (err error) {
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
		v = string(b)
	} else {
		return fmt.Errorf("unsupported sql value %v and type %v", value, value.GetTypeString())
	}

	name = strings.Replace(name, "*", "__ALL__", -1)
	r.data, err = sjson.SetBytes(r.data, name, v)
	return err
}

// MarshalCSV - encodes to CSV data.
func (r *Record) MarshalCSV(fieldDelimiter rune) ([]byte, error) {
	var csvRecord []string
	result := gjson.ParseBytes(r.data)
	result.ForEach(func(key, value gjson.Result) bool {
		csvRecord = append(csvRecord, value.String())
		return true
	})

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

// MarshalJSON - encodes to JSON data.
func (r *Record) MarshalJSON() ([]byte, error) {
	return r.data, nil
}

// NewRecord - creates new empty JSON record.
func NewRecord() *Record {
	return &Record{
		data: []byte("{}"),
	}
}
