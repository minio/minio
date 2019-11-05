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
	"errors"
	"io"
	"sync"

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
func (r *Reader) Read(dst sql.Record) (sql.Record, error) {
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

	dstRec, ok := dst.(*Record)
	if !ok {
		dstRec = &Record{}
	}
	dstRec.KVS = kvs
	dstRec.SelectFormat = sql.SelectFmtJSON
	return dstRec, nil
}

// Close - closes underlying reader.
func (r *Reader) Close() error {
	// Close the input.
	err := r.readCloser.Close()
	for range r.valueCh {
		// Drain values so we don't leak a goroutine.
		// Since we have closed the input, it should fail rather quickly.
	}
	return err
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) *Reader {
	readCloser = &syncReadCloser{rc: readCloser}
	d := jstream.NewDecoder(readCloser, 0).ObjectAsKVS()
	return &Reader{
		args:       args,
		decoder:    d,
		valueCh:    d.Stream(),
		readCloser: readCloser,
	}
}

// syncReadCloser will wrap a readcloser and make it safe to call Close
// while reads are running.
// All read errors are also postponed until Close is called and
// io.EOF is returned instead.
type syncReadCloser struct {
	rc    io.ReadCloser
	errMu sync.Mutex
	err   error
}

func (pr *syncReadCloser) Read(p []byte) (n int, err error) {
	// This ensures that Close will block until Read has completed.
	// This allows another goroutine to close the reader.
	pr.errMu.Lock()
	defer pr.errMu.Unlock()
	if pr.err != nil {
		return 0, io.EOF
	}
	n, pr.err = pr.rc.Read(p)
	if pr.err != nil {
		// Translate any error into io.EOF, so we don't crash:
		// https://github.com/bcicen/jstream/blob/master/scanner.go#L48
		return n, io.EOF
	}

	return n, nil
}

var errClosed = errors.New("read after close")

func (pr *syncReadCloser) Close() error {
	pr.errMu.Lock()
	defer pr.errMu.Unlock()
	if pr.err == errClosed {
		return nil
	}
	if pr.err != nil {
		return pr.err
	}
	pr.err = errClosed
	return pr.rc.Close()
}
