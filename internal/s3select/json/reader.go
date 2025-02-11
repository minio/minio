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
	"io"
	"sync"

	"github.com/minio/minio/internal/s3select/jstream"
	"github.com/minio/minio/internal/s3select/sql"
)

// Limit single document size to 10MiB, 10x the AWS limit:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html
const maxDocumentSize = 10 << 20

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
		kvs, _ = v.Value.(jstream.KVS)
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
	d := jstream.NewDecoder(io.LimitReader(readCloser, maxDocumentSize), 0).ObjectAsKVS().MaxDepth(100)
	return &Reader{
		args:       args,
		decoder:    d,
		valueCh:    d.Stream(),
		readCloser: readCloser,
	}
}

// syncReadCloser will wrap a readcloser and make it safe to call Close while
// reads are running.
type syncReadCloser struct {
	rc io.ReadCloser
	mu sync.Mutex
}

func (pr *syncReadCloser) Read(p []byte) (n int, err error) {
	// This ensures that Close will block until Read has completed.
	// This allows another goroutine to close the reader.
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.rc == nil {
		return 0, io.EOF
	}
	return pr.rc.Read(p)
}

func (pr *syncReadCloser) Close() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.rc != nil {
		err := pr.rc.Close()
		pr.rc = nil
		return err
	}
	return nil
}
