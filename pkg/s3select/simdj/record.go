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

package simdj

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/bcicen/jstream"
	"github.com/fwessels/simdjson-go"
	"github.com/minio/minio/pkg/s3select/json"
	"github.com/minio/minio/pkg/s3select/sql"
)

// Record - is JSON record.
type Record struct {
	// field name to Tape index lookup
	rootFields simdjson.Elements
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*sql.Value, error) {
	elem := r.rootFields.Lookup(name)
	if elem == nil {
		return nil, nil
	}
	return iterToValue(elem.Iter)
}

func iterToValue(iter simdjson.Iter) (*sql.Value, error) {
	switch iter.Type() {
	case simdjson.TypeString:
		v, err := iter.String()
		if err != nil {
			return nil, err
		}
		return sql.FromString(v), nil
	case simdjson.TypeFloat:
		v, err := iter.Float()
		if err != nil {
			return nil, err
		}
		return sql.FromFloat(v), nil
	case simdjson.TypeInt:
		v, err := iter.Int()
		if err != nil {
			return nil, err
		}
		return sql.FromInt(v), nil
	case simdjson.TypeUint:
		v, err := iter.Int()
		if err != nil {
			// Can't fit into int, convert to float.
			v, err := iter.Float()
			return sql.FromFloat(v), err
		}
		return sql.FromInt(v), nil
	case simdjson.TypeBool:
		v, err := iter.Bool()
		if err != nil {
			return nil, err
		}
		return sql.FromBool(v), nil
	case simdjson.TypeNull:
		return sql.FromNull(), nil
	case simdjson.TypeObject, simdjson.TypeArray:
		b, err := iter.MarshalJSON()
		return sql.FromBytes(b), err
	}
	return nil, fmt.Errorf("unknown JSON type: %s", iter.Type().String())
}

// Reset the record.
func (r *Record) Reset() {
	for k := range r.rootFields.Index {
		delete(r.rootFields.Index, k)
	}
	r.rootFields.Elements = r.rootFields.Elements[:0]
}

// Clone the record and if possible use the destination provided.
func (r *Record) Clone(dst sql.Record) sql.Record {
	other, ok := dst.(*Record)
	if !ok {
		other = &Record{}
	}
	if len(other.rootFields.Elements) > 0 {
		other.rootFields.Elements = other.rootFields.Elements[:0]
	}
	other.rootFields.Elements = append(other.rootFields.Elements, r.rootFields.Elements...)
	for k := range other.rootFields.Index {
		delete(other.rootFields.Index, k)
	}
	if other.rootFields.Index == nil {
		other.rootFields.Index = make(map[string]int, len(r.rootFields.Index))
	}

	for k, v := range r.rootFields.Index {
		other.rootFields.Index[k] = v
	}
	return other
}

// CloneTo clones the record to a json Record.
// Values are only unmashaled on object level.
func (r *Record) CloneTo(dst *json.Record) (sql.Record, error) {
	if dst == nil {
		dst = &json.Record{SelectFormat: sql.SelectFmtJSON}
	}
	dst.Reset()
	if cap(dst.KVS) < len(r.rootFields.Elements) {
		dst.KVS = make(jstream.KVS, 0, len(r.rootFields.Elements))
	}
	for _, elem := range r.rootFields.Elements {
		v, err := sql.IterToValue(elem.Iter)
		if err != nil {
			v, err = elem.Iter.Interface()
			if err != nil {
				panic(err)
			}
		}
		dst.KVS = append(dst.KVS, jstream.KV{
			Key:   elem.Name,
			Value: v,
		})
	}
	return dst, nil
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) (sql.Record, error) {
	dst, err := r.CloneTo(nil)
	if err != nil {
		return nil, err
	}
	return dst.Set(name, value)
}

// WriteCSV - encodes to CSV data.
func (r *Record) WriteCSV(writer io.Writer, fieldDelimiter rune) error {
	csvRecord := make([]string, 0, len(r.rootFields.Elements))
	for _, element := range r.rootFields.Elements {
		var columnValue string
		switch element.Type {
		case simdjson.TypeNull, simdjson.TypeFloat, simdjson.TypeUint, simdjson.TypeInt, simdjson.TypeBool, simdjson.TypeString:
			val, err := element.Iter.StringCvt()
			if err != nil {
				return err
			}
			columnValue = val
		case simdjson.TypeObject, simdjson.TypeArray:
			b, err := element.Iter.MarshalJSON()
			if err != nil {
				return err
			}
			columnValue = string(b)
		default:
			return fmt.Errorf("cannot marshal unhandled type: %s", element.Type.String())
		}
		csvRecord = append(csvRecord, columnValue)
	}
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
	return sql.SelectFmtSIMDJSON, r.rootFields
}

// WriteJSON - encodes to JSON data.
func (r *Record) WriteJSON(writer io.Writer) error {
	b, err := r.rootFields.MarshalJSON()
	if err != nil {
		return err
	}
	n, err := writer.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}

// Replace the underlying buffer of json data.
func (r *Record) Replace(k interface{}) error {
	v, ok := k.(simdjson.Elements)
	if !ok {
		return fmt.Errorf("cannot replace internal data in simd json record with type %T", k)
	}
	r.rootFields = v
	return nil
}

// NewRecord - creates new empty JSON record.
func NewRecord(f sql.SelectObjectFormat, obj simdjson.Object) *Record {
	elems, err := obj.Parse(nil)
	if err != nil {
		return nil
	}
	return &Record{
		rootFields: *elems,
	}
}
