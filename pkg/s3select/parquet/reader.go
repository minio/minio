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

package parquet

import (
	"io"

	"github.com/minio/minio/pkg/s3select/json"
	"github.com/minio/minio/pkg/s3select/sql"
	parquetgo "github.com/minio/parquet-go"
	parquetgen "github.com/minio/parquet-go/gen-go/parquet"
)

// Reader - Parquet record reader for S3Select.
type Reader struct {
	args *ReaderArgs
	file *parquetgo.File
}

// Read - reads single record.
func (r *Reader) Read() (rec sql.Record, rerr error) {
	parquetRecord, err := r.file.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errParquetParsingError(err)
		}

		return nil, err
	}

	record := json.NewRecord()
	f := func(name string, v parquetgo.Value) bool {
		if v.Value == nil {
			if err := record.Set(name, sql.FromNull()); err != nil {
				rerr = errParquetParsingError(err)
			}
			return rerr == nil
		}

		var value *sql.Value
		switch v.Type {
		case parquetgen.Type_BOOLEAN:
			value = sql.FromBool(v.Value.(bool))
		case parquetgen.Type_INT32:
			value = sql.FromInt(int64(v.Value.(int32)))
		case parquetgen.Type_INT64:
			value = sql.FromInt(int64(v.Value.(int64)))
		case parquetgen.Type_FLOAT:
			value = sql.FromFloat(float64(v.Value.(float32)))
		case parquetgen.Type_DOUBLE:
			value = sql.FromFloat(v.Value.(float64))
		case parquetgen.Type_INT96, parquetgen.Type_BYTE_ARRAY, parquetgen.Type_FIXED_LEN_BYTE_ARRAY:
			value = sql.FromString(string(v.Value.([]byte)))
		default:
			rerr = errParquetParsingError(nil)
			return false
		}

		if err = record.Set(name, value); err != nil {
			rerr = errParquetParsingError(err)
		}
		return rerr == nil
	}

	parquetRecord.Range(f)
	return record, rerr
}

// Close - closes underlaying readers.
func (r *Reader) Close() error {
	return r.file.Close()
}

// NewReader - creates new Parquet reader using readerFunc callback.
func NewReader(getReaderFunc func(offset, length int64) (io.ReadCloser, error), args *ReaderArgs) (*Reader, error) {
	file, err := parquetgo.Open(getReaderFunc, nil)
	if err != nil {
		if err != io.EOF {
			return nil, errParquetParsingError(err)
		}

		return nil, err
	}

	return &Reader{
		args: args,
		file: file,
	}, nil
}
