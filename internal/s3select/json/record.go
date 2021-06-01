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

package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/bcicen/jstream"
	csv "github.com/minio/csvparser"
	"github.com/minio/minio/internal/s3select/sql"
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

// Reset the record.
func (r *Record) Reset() {
	if len(r.KVS) > 0 {
		r.KVS = r.KVS[:0]
	}
}

// Clone the record and if possible use the destination provided.
func (r *Record) Clone(dst sql.Record) sql.Record {
	other, ok := dst.(*Record)
	if !ok {
		other = &Record{}
	}
	if len(other.KVS) > 0 {
		other.KVS = other.KVS[:0]
	}
	other.KVS = append(other.KVS, r.KVS...)
	return other
}

// Set - sets the value for a column name.
func (r *Record) Set(name string, value *sql.Value) (sql.Record, error) {
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
		// This can either be raw json or a CSV value.
		// Only treat objects and arrays as JSON.
		if len(b) > 0 && (b[0] == '{' || b[0] == '[') {
			v = RawJSON(b)
		} else {
			v = string(b)
		}
	} else if arr, ok := value.ToArray(); ok {
		v = arr
	} else {
		return nil, fmt.Errorf("unsupported sql value %v and type %v", value, value.GetTypeString())
	}

	name = strings.Replace(name, "*", "__ALL__", -1)
	r.KVS = append(r.KVS, jstream.KV{Key: name, Value: v})
	return r, nil
}

// WriteCSV - encodes to CSV data.
func (r *Record) WriteCSV(writer io.Writer, opts sql.WriteCSVOpts) error {
	var csvRecord []string
	for _, kv := range r.KVS {
		var columnValue string
		switch val := kv.Value.(type) {
		case float64:
			columnValue = jsonFloat(val)
		case string:
			columnValue = val
		case bool, int64:
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

	w := csv.NewWriter(writer)
	w.Comma = opts.FieldDelimiter
	w.Quote = opts.Quote
	w.AlwaysQuote = opts.AlwaysQuote
	w.QuoteEscape = opts.QuoteEscape
	if err := w.Write(csvRecord); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

// Raw - returns the underlying representation.
func (r *Record) Raw() (sql.SelectObjectFormat, interface{}) {
	return r.SelectFormat, r.KVS
}

// WriteJSON - encodes to JSON data.
func (r *Record) WriteJSON(writer io.Writer) error {
	return json.NewEncoder(writer).Encode(r.KVS)
}

// Replace the underlying buffer of json data.
func (r *Record) Replace(k interface{}) error {
	v, ok := k.(jstream.KVS)
	if !ok {
		return fmt.Errorf("cannot replace internal data in json record with type %T", k)
	}
	r.KVS = v
	return nil
}

// NewRecord - creates new empty JSON record.
func NewRecord(f sql.SelectObjectFormat) *Record {
	return &Record{
		KVS:          jstream.KVS{},
		SelectFormat: f,
	}
}

// jsonFloat converts a float to string similar to Go stdlib formats json floats.
func jsonFloat(f float64) string {
	var tmp [32]byte
	dst := tmp[:0]

	// Convert as if by ES6 number to string conversion.
	// This matches most other JSON generators.
	// See golang.org/issue/6384 and golang.org/issue/14135.
	// Like fmt %g, but the exponent cutoffs are different
	// and exponents themselves are not padded to two digits.
	abs := math.Abs(f)
	fmt := byte('f')
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, f, fmt, -1, 64)
	if fmt == 'e' {
		// clean up e-09 to e-9
		n := len(dst)
		if n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return string(dst)
}
