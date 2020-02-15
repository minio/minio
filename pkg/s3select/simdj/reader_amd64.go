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

package simdj

import (
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio/pkg/s3select/json"
	"github.com/minio/minio/pkg/s3select/sql"
	"github.com/minio/simdjson-go"
)

// Reader - JSON record reader for S3Select.
type Reader struct {
	args    *json.ReaderArgs
	input   chan simdjson.Stream
	decoded chan simdjson.Object

	// err will only be returned after decoded has been closed.
	err        *error
	readCloser io.ReadCloser

	exitReader chan struct{}
	readerWg   sync.WaitGroup
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
	dstRec.object = v
	return dstRec, nil
}

// Close - closes underlying reader.
func (r *Reader) Close() error {
	// Close the input.
	// Potentially racy if the stream decoder is still reading.
	if r.readCloser != nil {
		r.readCloser.Close()
	}
	if r.exitReader != nil {
		close(r.exitReader)
		r.readerWg.Wait()
		r.exitReader = nil
		r.input = nil
	}
	return nil
}

// startReader will start a reader that accepts input from r.input.
// Input should be root -> object input. Each root indicates a record.
// If r.input is closed, it is assumed that no more input will come.
// When this function returns r.readerWg will be decremented and r.decoded will be closed.
// On errors, r.err will be set. This should only be accessed after r.decoded has been closed.
func (r *Reader) startReader() {
	defer r.readerWg.Done()
	defer close(r.decoded)
	var tmpObj simdjson.Object
	for {
		var in simdjson.Stream
		select {
		case in = <-r.input:
		case <-r.exitReader:
			return
		}
		if in.Error != nil && in.Error != io.EOF {
			r.err = &in.Error
			return
		}
		if in.Value == nil {
			if in.Error == io.EOF {
				return
			}
			continue
		}
		i := in.Value.Iter()
	readloop:
		for {
			var next simdjson.Iter
			typ, err := i.AdvanceIter(&next)
			if err != nil {
				r.err = &err
				return
			}
			switch typ {
			case simdjson.TypeNone:
				break readloop
			case simdjson.TypeRoot:
				typ, obj, err := next.Root(nil)
				if err != nil {
					r.err = &err
					return
				}
				if typ != simdjson.TypeObject {
					if typ == simdjson.TypeNone {
						continue
					}
					err = fmt.Errorf("unexpected json type below root :%v", typ)
					r.err = &err
					return
				}

				o, err := obj.Object(&tmpObj)
				if err != nil {
					r.err = &err
					return
				}
				select {
				case <-r.exitReader:
					return
				case r.decoded <- *o:
				}
			default:
				err = fmt.Errorf("unexpected root json type:%v", typ)
				r.err = &err
				return
			}
		}
		if in.Error == io.EOF {
			return
		}
	}
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *json.ReaderArgs) *Reader {
	r := Reader{
		args:       args,
		readCloser: readCloser,
		decoded:    make(chan simdjson.Object, 1000),
		input:      make(chan simdjson.Stream, 2),
		exitReader: make(chan struct{}),
	}
	simdjson.ParseNDStream(readCloser, r.input, nil)
	r.readerWg.Add(1)
	go r.startReader()
	return &r
}

// NewElementReader - creates new JSON reader using readCloser.
func NewElementReader(ch chan simdjson.Object, err *error, args *json.ReaderArgs) *Reader {
	return &Reader{
		args:       args,
		decoded:    ch,
		err:        err,
		readCloser: nil,
	}
}

// NewTapeReaderChan will start a reader that will read input from the provided channel.
func NewTapeReaderChan(pj chan simdjson.Stream, args *json.ReaderArgs) *Reader {
	r := Reader{
		args:       args,
		decoded:    make(chan simdjson.Object, 1000),
		input:      pj,
		exitReader: make(chan struct{}),
	}
	r.readerWg.Add(1)
	go r.startReader()
	return &r
}
