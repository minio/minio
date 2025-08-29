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

package parquet

import (
	"errors"
	"io"
	"time"

	parquetgo "github.com/fraugster/parquet-go"
	parquettypes "github.com/fraugster/parquet-go/parquet"
	jsonfmt "github.com/minio/minio/internal/s3select/json"
	"github.com/minio/minio/internal/s3select/jstream"
	"github.com/minio/minio/internal/s3select/sql"
)

// Reader implements reading records from parquet input.
type Reader struct {
	io.Closer
	r *parquetgo.FileReader
}

// NewParquetReader creates a Reader2 from a io.ReadSeekCloser.
func NewParquetReader(rsc io.ReadSeekCloser, _ *ReaderArgs) (r *Reader, err error) {
	fr, err := parquetgo.NewFileReader(rsc)
	if err != nil {
		return nil, errParquetParsingError(err)
	}

	return &Reader{Closer: rsc, r: fr}, nil
}

func (pr *Reader) Read(dst sql.Record) (rec sql.Record, rerr error) {
	nextRow, err := pr.r.NextRow()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errParquetParsingError(err)
	}

	kvs := jstream.KVS{}
	for _, col := range pr.r.Columns() {
		var value any
		if v, ok := nextRow[col.FlatName()]; ok {
			value, err = convertFromAnnotation(col.Element(), v)
			if err != nil {
				return nil, errParquetParsingError(err)
			}
		}
		kvs = append(kvs, jstream.KV{Key: col.FlatName(), Value: value})
	}

	// Reuse destination if we can.
	dstRec, ok := dst.(*jsonfmt.Record)
	if !ok {
		dstRec = &jsonfmt.Record{}
	}
	dstRec.SelectFormat = sql.SelectFmtParquet
	dstRec.KVS = kvs
	return dstRec, nil
}

// convertFromAnnotation - converts values based on the Parquet column's type
// annotations. LogicalType annotations if present override the deprecated
// ConvertedType annotations. Ref:
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
func convertFromAnnotation(se *parquettypes.SchemaElement, v any) (any, error) {
	if se == nil {
		return v, nil
	}

	var value any
	switch val := v.(type) {
	case []byte:
		// TODO: only strings are supported in s3select output (not
		// binary arrays) - perhaps we need to check the annotation to
		// ensure it's UTF8 encoded.
		value = string(val)
	case [12]byte:
		// TODO: This is returned for the parquet INT96 type. We just
		// treat it same as []byte (but AWS S3 treats it as a large int)
		// - fix this later.
		value = string(val[:])
	case int32:
		value = int64(val)
		if logicalType := se.GetLogicalType(); logicalType != nil {
			if logicalType.IsSetDATE() {
				value = sql.FormatSQLTimestamp(time.Unix(60*60*24*int64(val), 0).UTC())
			}
		} else if se.GetConvertedType() == parquettypes.ConvertedType_DATE {
			value = sql.FormatSQLTimestamp(time.Unix(60*60*24*int64(val), 0).UTC())
		}
	case int64:
		value = val
		if logicalType := se.GetLogicalType(); logicalType != nil {
			if ts := logicalType.GetTIMESTAMP(); ts != nil {
				var duration time.Duration
				// Only support UTC normalized timestamps.
				if ts.IsAdjustedToUTC {
					switch {
					case ts.Unit.IsSetNANOS():
						duration = time.Duration(val) * time.Nanosecond
					case ts.Unit.IsSetMILLIS():
						duration = time.Duration(val) * time.Millisecond
					case ts.Unit.IsSetMICROS():
						duration = time.Duration(val) * time.Microsecond
					default:
						return nil, errors.New("Invalid LogicalType annotation found")
					}
					value = sql.FormatSQLTimestamp(time.Unix(0, 0).Add(duration))
				}
			} else if se.GetConvertedType() == parquettypes.ConvertedType_TIMESTAMP_MILLIS {
				duration := time.Duration(val) * time.Millisecond
				value = sql.FormatSQLTimestamp(time.Unix(0, 0).Add(duration))
			} else if se.GetConvertedType() == parquettypes.ConvertedType_TIMESTAMP_MICROS {
				duration := time.Duration(val) * time.Microsecond
				value = sql.FormatSQLTimestamp(time.Unix(0, 0).Add(duration))
			}
		}
	case float32:
		value = float64(val)
	default:
		value = v
	}
	return value, nil
}
