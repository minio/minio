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
		return sql.NewNull(), nil
	case gjson.False:
		return sql.NewBool(false), nil
	case gjson.Number:
		return sql.NewFloat(result.Float()), nil
	case gjson.String:
		return sql.NewString(result.String()), nil
	case gjson.True:
		return sql.NewBool(true), nil
	}

	return nil, fmt.Errorf("unsupported gjson value %v; %v", result, result.Type)
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) (err error) {
	var v interface{}
	switch value.Type() {
	case sql.Null:
		v = value.NullValue()
	case sql.Bool:
		v = value.BoolValue()
	case sql.Int:
		v = value.IntValue()
	case sql.Float:
		v = value.FloatValue()
	case sql.String:
		v = value.StringValue()
	default:
		return fmt.Errorf("unsupported sql value %v and type %v", value, value.Type())
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
