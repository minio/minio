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

package simdjson

import (
	"io"

	"github.com/fwessels/simdjson-go"

	"github.com/minio/minio/pkg/s3select/sql"
)

// Reader - JSON record reader for S3Select.
type Reader struct {
	args    *ReaderArgs
	decoded chan simdjson.Elements

	// err will only be returned after decoded has been closed.
	err        *error
	readCloser io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read(dst sql.Record) (sql.Record, error) {
	v, ok := <-r.decoded
	if !ok {
		if r.err != nil && *r.err != nil {
			return nil, errJSONParsingError(*r.err)
		}
		return nil, io.EOF
	}
	dstRec, ok := dst.(*Record)
	if !ok {
		dstRec = &Record{}
	}
	dstRec.rootFields = v
	return dstRec, nil
}

// Close - closes underlying reader.
func (r *Reader) Close() error {
	// Close the input.
	// Potentially racy if the stream decoder is still reading.
	if r.readCloser != nil {
		r.readCloser.Close()
	}
	for range r.decoded {
		// Drain values so we don't leak a goroutine.
		// Since we have closed the input, it should fail rather quickly.
	}
	return nil
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) *Reader {
	// TODO: Add simdjson input when available
	return &Reader{
		args:       args,
		readCloser: readCloser,
	}
}

// NewElementReader - creates new JSON reader using readCloser.
func NewElementReader(ch chan simdjson.Elements, err *error, args *ReaderArgs) *Reader {
	return &Reader{
		args:       args,
		decoded:    ch,
		err:        err,
		readCloser: nil,
	}
}
