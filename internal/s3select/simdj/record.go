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

package simdj

import (
	"fmt"
	"io"

	csv "github.com/minio/csvparser"
	"github.com/minio/minio/internal/s3select/json"
	"github.com/minio/minio/internal/s3select/jstream"
	"github.com/minio/minio/internal/s3select/sql"
	"github.com/minio/simdjson-go"
)

// Record - is JSON record.
type Record struct {
	// object
	object simdjson.Object
}

// Get - gets the value for a column name.
func (r *Record) Get(name string) (*sql.Value, error) {
	elem := r.object.FindKey(name, nil)
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
	return nil, fmt.Errorf("iterToValue: unknown JSON type: %s", iter.Type().String())
}

// Reset the record.
func (r *Record) Reset() {
	r.object = simdjson.Object{}
}

// Clone the record and if possible use the destination provided.
func (r *Record) Clone(dst sql.Record) sql.Record {
	other, ok := dst.(*Record)
	if !ok {
		other = &Record{}
	}
	other.object = r.object
	return other
}

// CloneTo clones the record to a json Record.
// Values are only unmashaled on object level.
func (r *Record) CloneTo(dst *json.Record) (sql.Record, error) {
	if dst == nil {
		dst = &json.Record{SelectFormat: sql.SelectFmtJSON}
	}
	dst.Reset()
	elems, err := r.object.Parse(nil)
	if err != nil {
		return nil, err
	}
	if cap(dst.KVS) < len(elems.Elements) {
		dst.KVS = make(jstream.KVS, 0, len(elems.Elements))
	}
	for _, elem := range elems.Elements {
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
func (r *Record) WriteCSV(writer io.Writer, opts sql.WriteCSVOpts) error {
	csvRecord := make([]string, 0, 10)
	var tmp simdjson.Iter
	obj := r.object
allElems:
	for {
		_, typ, err := obj.NextElement(&tmp)
		if err != nil {
			return err
		}
		var columnValue string
		switch typ {
		case simdjson.TypeNull, simdjson.TypeFloat, simdjson.TypeUint, simdjson.TypeInt, simdjson.TypeBool, simdjson.TypeString:
			val, err := tmp.StringCvt()
			if err != nil {
				return err
			}
			columnValue = val
		case simdjson.TypeObject, simdjson.TypeArray:
			b, err := tmp.MarshalJSON()
			if err != nil {
				return err
			}
			columnValue = string(b)
		case simdjson.TypeNone:
			break allElems
		default:
			return fmt.Errorf("cannot marshal unhandled type: %s", typ.String())
		}
		csvRecord = append(csvRecord, columnValue)
	}
	w := csv.NewWriter(writer)
	w.Comma = opts.FieldDelimiter
	w.Quote = opts.Quote
	w.QuoteEscape = opts.QuoteEscape
	w.AlwaysQuote = opts.AlwaysQuote
	if err := w.Write(csvRecord); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

// Raw - returns the underlying representation.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	return sql.SelectFmtSIMDJSON, r.object
}

// WriteJSON - encodes to JSON data.
func (r *Record) WriteJSON(writer io.Writer) error {
	o := r.object
	elems, err := o.Parse(nil)
	if err != nil {
		return err
	}
	b, err := elems.MarshalJSON()
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
	v, ok := k.(simdjson.Object)
	if !ok {
		return fmt.Errorf("cannot replace internal data in simd json record with type %T", k)
	}
	r.object = v
	return nil
}

// NewRecord - creates new empty JSON record.
func NewRecord(f sql.SelectObjectFormat, obj simdjson.Object) *Record {
	return &Record{
		object: obj,
	}
}
