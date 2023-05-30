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

package simdj

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/s3select/json"
	"github.com/minio/minio/internal/s3select/sql"
	"github.com/minio/simdjson-go"
)

// Reader - JSON record reader for S3Select.
type Reader struct {
	args    *json.ReaderArgs
	input   chan simdjson.Stream
	decoded chan simdjson.Object

	// err will only be returned after decoded has been closed.
	err          *error
	readCloser   io.ReadCloser
	onReaderExit func()

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
	defer r.onReaderExit()
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
		readCloser: &safeCloser{r: io.Reader(readCloser)},
		decoded:    make(chan simdjson.Object, 1000),
		input:      make(chan simdjson.Stream, 2),
		exitReader: make(chan struct{}),
	}
	r.onReaderExit = func() {
		close(r.decoded)
		readCloser.Close()
		for range r.input {
			// Read until EOF trickles through.
			// Otherwise, we risk the decoder hanging.
		}
		r.readerWg.Done()
	}

	// We cannot reuse as we are sending parsed objects elsewhere.
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

// safeCloser will wrap a Reader as a ReadCloser.
// It is safe to call Close while the reader is being used.
type safeCloser struct {
	closed uint32
	r      io.Reader
}

func (s *safeCloser) Read(p []byte) (n int, err error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return 0, io.EOF
	}
	n, err = s.r.Read(p)
	if atomic.LoadUint32(&s.closed) == 1 {
		return 0, io.EOF
	}
	return n, err
}

func (s *safeCloser) Close() error {
	atomic.CompareAndSwapUint32(&s.closed, 0, 1)
	return nil
}
