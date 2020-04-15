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

package sql

import (
	"fmt"
	"io"

	"github.com/minio/simdjson-go"
)

// SelectObjectFormat specifies the format of the underlying data
type SelectObjectFormat int

const (
	// SelectFmtUnknown - unknown format (default value)
	SelectFmtUnknown SelectObjectFormat = iota
	// SelectFmtCSV - CSV format
	SelectFmtCSV
	// SelectFmtJSON - JSON format
	SelectFmtJSON
	// SelectFmtSIMDJSON - SIMD JSON format
	SelectFmtSIMDJSON
	// SelectFmtParquet - Parquet format
	SelectFmtParquet
)

// WriteCSVOpts - encapsulates options for Select CSV output
type WriteCSVOpts struct {
	FieldDelimiter rune
	Quote          rune
	QuoteEscape    rune
	AlwaysQuote    bool
}

// Record - is a type containing columns and their values.
type Record interface {
	Get(name string) (*Value, error)

	// Set a value.
	// Can return a different record type.
	Set(name string, value *Value) (Record, error)
	WriteCSV(writer io.Writer, opts WriteCSVOpts) error
	WriteJSON(writer io.Writer) error

	// Clone the record and if possible use the destination provided.
	Clone(dst Record) Record
	Reset()

	// Returns underlying representation
	Raw() (SelectObjectFormat, interface{})

	// Replaces the underlying data
	Replace(k interface{}) error
}

// IterToValue converts a simdjson Iter to its underlying value.
// Objects are returned as simdjson.Object
// Arrays are returned as []interface{} with parsed values.
func IterToValue(iter simdjson.Iter) (interface{}, error) {
	switch iter.Type() {
	case simdjson.TypeString:
		v, err := iter.String()
		if err != nil {
			return nil, err
		}
		return v, nil
	case simdjson.TypeFloat:
		v, err := iter.Float()
		if err != nil {
			return nil, err
		}
		return v, nil
	case simdjson.TypeInt:
		v, err := iter.Int()
		if err != nil {
			return nil, err
		}
		return v, nil
	case simdjson.TypeUint:
		v, err := iter.Int()
		if err != nil {
			// Can't fit into int, convert to float.
			v, err := iter.Float()
			return v, err
		}
		return v, nil
	case simdjson.TypeBool:
		v, err := iter.Bool()
		if err != nil {
			return nil, err
		}
		return v, nil
	case simdjson.TypeObject:
		obj, err := iter.Object(nil)
		if err != nil {
			return nil, err
		}
		return *obj, err
	case simdjson.TypeArray:
		arr, err := iter.Array(nil)
		if err != nil {
			return nil, err
		}
		iter := arr.Iter()
		var dst []interface{}
		var next simdjson.Iter
		for {
			typ, err := iter.AdvanceIter(&next)
			if err != nil {
				return nil, err
			}
			if typ == simdjson.TypeNone {
				break
			}
			v, err := IterToValue(next)
			if err != nil {
				return nil, err
			}
			dst = append(dst, v)
		}
		return dst, err
	case simdjson.TypeNull:
		return nil, nil
	}
	return nil, fmt.Errorf("IterToValue: unknown JSON type: %s", iter.Type().String())
}
