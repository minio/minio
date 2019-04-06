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

package parquet

import (
	"io"

	"github.com/bcicen/jstream"
	jsonfmt "github.com/minio/minio/pkg/s3select/json"
	"github.com/minio/minio/pkg/s3select/sql"
	parquetgo "github.com/minio/parquet-go"
	parquetgen "github.com/minio/parquet-go/gen-go/parquet"
)

// Reader - Parquet record reader for S3Select.
type Reader struct {
	args   *ReaderArgs
	reader *parquetgo.Reader
}

// Read - reads single record.
func (r *Reader) Read() (rec sql.Record, rerr error) {
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
		case parquetgen.Type_INT64:
			value = int64(v.Value.(int64))
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

	parquetRecord.Range(f)
	return &jsonfmt.Record{KVS: kvs, SelectFormat: sql.SelectFmtParquet}, nil
}

// Close - closes underlaying readers.
func (r *Reader) Close() error {
	return r.reader.Close()
}

// NewReader - creates new Parquet reader using readerFunc callback.
func NewReader(getReaderFunc func(offset, length int64) (io.ReadCloser, error), args *ReaderArgs) (*Reader, error) {
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
