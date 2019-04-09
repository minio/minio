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

package json

import (
	"io"

	"github.com/minio/minio/pkg/s3select/sql"

	"github.com/bcicen/jstream"
)

// Reader - JSON record reader for S3Select.
type Reader struct {
	args       *ReaderArgs
	decoder    *jstream.Decoder
	valueCh    chan *jstream.MetaValue
	readCloser io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read() (sql.Record, error) {
	v, ok := <-r.valueCh
	if !ok {
		if err := r.decoder.Err(); err != nil {
			return nil, errJSONParsingError(err)
		}
		return nil, io.EOF
	}

	var kvs jstream.KVS
	if v.ValueType == jstream.Object {
		// This is a JSON object type (that preserves key
		// order)
		kvs = v.Value.(jstream.KVS)
	} else {
		// To be AWS S3 compatible Select for JSON needs to
		// output non-object JSON as single column value
		// i.e. a map with `_1` as key and value as the
		// non-object.
		kvs = jstream.KVS{jstream.KV{Key: "_1", Value: v.Value}}
	}

	return &Record{
		KVS:          kvs,
		SelectFormat: sql.SelectFmtJSON,
	}, nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) *Reader {
	d := jstream.NewDecoder(readCloser, 0).ObjectAsKVS()
	return &Reader{
		args:       args,
		decoder:    d,
		valueCh:    d.Stream(),
		readCloser: readCloser,
	}
}
