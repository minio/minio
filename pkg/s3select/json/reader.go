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
	"encoding/json"
	"io"

	"github.com/minio/minio/pkg/s3select/sql"

	"github.com/bcicen/jstream"
	"github.com/tidwall/sjson"
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

	var data []byte
	var err error

	if v.ValueType == jstream.Object {
		data, err = json.Marshal(v.Value)
	} else {
		// To be AWS S3 compatible
		// Select for JSON needs to output non-object JSON as single column value
		// i.e. a map with `_1` as key and value as the non-object.
		data, err = sjson.SetBytes(data, "_1", v.Value)
	}
	if err != nil {
		return nil, errJSONParsingError(err)
	}

	return &Record{
		data: data,
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
