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
	"fmt"
	"io"
	"time"

	"github.com/bcicen/jstream"
	jsonfmt "github.com/minio/minio/internal/s3select/json"
	"github.com/minio/minio/internal/s3select/sql"
	parquetgo "github.com/minio/parquet-go"
	parquetgen "github.com/minio/parquet-go/gen-go/parquet"
)

// Reader - Parquet record reader for S3Select.
type Reader struct {
	args   *ReaderArgs
	reader *parquetgo.Reader
}

// Read - reads single record.
func (r *Reader) Read(dst sql.Record) (rec sql.Record, rerr error) {
	defer func() {
		if rec := recover(); rec != nil {
			rerr = fmt.Errorf("panic reading parquet record: %v", rec)
		}
	}()

	parquetRecord, err := r.reader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errParquetParsingError(err)
		}

		return nil, err
	}

	kvs := jstream.KVS{}
	f := func(name string, v parquetgo.Value) bool {
		if v.Value == nil {
			kvs = append(kvs, jstream.KV{Key: name, Value: nil})
			return true
		}

		var value interface{}
		switch v.Type {
		case parquetgen.Type_BOOLEAN:
			value = v.Value.(bool)
		case parquetgen.Type_INT32:
			value = int64(v.Value.(int32))
			if v.Schema != nil && v.Schema.ConvertedType != nil {
				switch *v.Schema.ConvertedType {
				case parquetgen.ConvertedType_DATE:
					value = sql.FormatSQLTimestamp(time.Unix(60*60*24*int64(v.Value.(int32)), 0).UTC())
				}
			}
		case parquetgen.Type_INT64:
			value = v.Value.(int64)
			if v.Schema != nil && v.Schema.ConvertedType != nil {
				switch *v.Schema.ConvertedType {
				// Only UTC supported, add one NS to never be exactly midnight.
				case parquetgen.ConvertedType_TIMESTAMP_MILLIS:
					value = sql.FormatSQLTimestamp(time.Unix(0, 0).Add(time.Duration(v.Value.(int64)) * time.Millisecond).UTC())
				case parquetgen.ConvertedType_TIMESTAMP_MICROS:
					value = sql.FormatSQLTimestamp(time.Unix(0, 0).Add(time.Duration(v.Value.(int64)) * time.Microsecond).UTC())
				}
			}
		case parquetgen.Type_FLOAT:
			value = float64(v.Value.(float32))
		case parquetgen.Type_DOUBLE:
			value = v.Value.(float64)
		case parquetgen.Type_INT96, parquetgen.Type_BYTE_ARRAY, parquetgen.Type_FIXED_LEN_BYTE_ARRAY:
			value = string(v.Value.([]byte))
		default:
			rerr = errParquetParsingError(nil)
			return false
		}

		kvs = append(kvs, jstream.KV{Key: name, Value: value})
		return true
	}

	// Apply our range
	parquetRecord.Range(f)

	// Reuse destination if we can.
	dstRec, ok := dst.(*jsonfmt.Record)
	if !ok {
		dstRec = &jsonfmt.Record{}
	}
	dstRec.SelectFormat = sql.SelectFmtParquet
	dstRec.KVS = kvs
	return dstRec, nil
}

// Close - closes underlying readers.
func (r *Reader) Close() error {
	return r.reader.Close()
}

// NewReader - creates new Parquet reader using readerFunc callback.
func NewReader(getReaderFunc func(offset, length int64) (io.ReadCloser, error), args *ReaderArgs) (r *Reader, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic reading parquet header: %v", rec)
		}
	}()
	reader, err := parquetgo.NewReader(getReaderFunc, nil)
	if err != nil {
		if err != io.EOF {
			return nil, errParquetParsingError(err)
		}

		return nil, err
	}

	return &Reader{
		args:   args,
		reader: reader,
	}, nil
}
