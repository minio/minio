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
	"bufio"
	"bytes"
	"io"
	"runtime"
	"sync"

	"github.com/bcicen/jstream"
	"github.com/minio/minio/internal/s3select/sql"
)

// PReader - JSON record reader for S3Select.
// Operates concurrently on line-delimited JSON.
type PReader struct {
	args        *ReaderArgs
	readCloser  io.ReadCloser   // raw input
	buf         *bufio.Reader   // input to the splitter
	current     []jstream.KVS   // current block of results to be returned
	recordsRead int             // number of records read in current slice
	input       chan *queueItem // input for workers
	queue       chan *queueItem // output from workers in order
	err         error           // global error state, only touched by Reader.Read
	bufferPool  sync.Pool       // pool of []byte objects for input
	kvDstPool   sync.Pool       // pool of []jstream.KV used for output
	close       chan struct{}   // used for shutting down the splitter before end of stream
	readerWg    sync.WaitGroup  // used to keep track of async reader.
}

// queueItem is an item in the queue.
type queueItem struct {
	input []byte             // raw input sent to the worker
	dst   chan []jstream.KVS // result of block decode
	err   error              // any error encountered will be set here
}

// Read - reads single record.
// Once Read is called the previous record should no longer be referenced.
func (r *PReader) Read(dst sql.Record) (sql.Record, error) {
	// If we have have any records left, return these before any error.
	for len(r.current) <= r.recordsRead {
		if r.err != nil {
			return nil, r.err
		}
		// Move to next block
		item, ok := <-r.queue
		if !ok {
			r.err = io.EOF
			return nil, r.err
		}
		//lint:ignore SA6002 Using pointer would allocate more since we would have to copy slice header before taking a pointer.
		r.kvDstPool.Put(r.current)
		r.current = <-item.dst
		r.err = item.err
		r.recordsRead = 0
	}
	kvRecord := r.current[r.recordsRead]
	r.recordsRead++

	dstRec, ok := dst.(*Record)
	if !ok {
		dstRec = &Record{}
	}
	dstRec.KVS = kvRecord
	dstRec.SelectFormat = sql.SelectFmtJSON
	return dstRec, nil
}

// Close - closes underlying reader.
func (r *PReader) Close() error {
	if r.close != nil {
		close(r.close)
		r.readerWg.Wait()
		r.close = nil
	}
	r.recordsRead = len(r.current)
	if r.err == nil {
		r.err = io.EOF
	}
	return r.readCloser.Close()
}

// nextSplit will attempt to skip a number of bytes and
// return the buffer until the next newline occurs.
// The last block will be sent along with an io.EOF.
func (r *PReader) nextSplit(skip int, dst []byte) ([]byte, error) {
	if cap(dst) < skip {
		dst = make([]byte, 0, skip+1024)
	}
	dst = dst[:skip]
	if skip > 0 {
		n, err := io.ReadFull(r.buf, dst)
		if err != nil && err != io.ErrUnexpectedEOF {
			// If an EOF happens after reading some but not all the bytes,
			// ReadFull returns ErrUnexpectedEOF.
			return dst[:n], err
		}
		dst = dst[:n]
		if err == io.ErrUnexpectedEOF {
			return dst, io.EOF
		}
	}
	// Read until next line.
	in, err := r.buf.ReadBytes('\n')
	dst = append(dst, in...)
	return dst, err
}

// jsonSplitSize is the size of each block.
// Blocks will read this much and find the first following newline.
// 128KB appears to be a very reasonable default.
const jsonSplitSize = 128 << 10

// startReaders will read the header if needed and spin up a parser
// and a number of workers based on GOMAXPROCS.
// If an error is returned no goroutines have been started and r.err will have been set.
func (r *PReader) startReaders() {
	r.bufferPool.New = func() interface{} {
		return make([]byte, jsonSplitSize+1024)
	}

	// Create queue
	r.queue = make(chan *queueItem, runtime.GOMAXPROCS(0))
	r.input = make(chan *queueItem, runtime.GOMAXPROCS(0))
	r.readerWg.Add(1)

	// Start splitter
	go func() {
		defer close(r.input)
		defer close(r.queue)
		defer r.readerWg.Done()
		for {
			next, err := r.nextSplit(jsonSplitSize, r.bufferPool.Get().([]byte))
			q := queueItem{
				input: next,
				dst:   make(chan []jstream.KVS, 1),
				err:   err,
			}
			select {
			case <-r.close:
				return
			case r.queue <- &q:
			}

			select {
			case <-r.close:
				return
			case r.input <- &q:
			}
			if err != nil {
				// Exit on any error.
				return
			}
		}
	}()

	// Start parsers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go func() {
			for in := range r.input {
				if len(in.input) == 0 {
					in.dst <- nil
					continue
				}
				dst, ok := r.kvDstPool.Get().([]jstream.KVS)
				if !ok {
					dst = make([]jstream.KVS, 0, 1000)
				}

				d := jstream.NewDecoder(bytes.NewBuffer(in.input), 0).ObjectAsKVS()
				stream := d.Stream()
				all := dst[:0]
				for mv := range stream {
					var kvs jstream.KVS
					if mv.ValueType == jstream.Object {
						// This is a JSON object type (that preserves key
						// order)
						kvs = mv.Value.(jstream.KVS)
					} else {
						// To be AWS S3 compatible Select for JSON needs to
						// output non-object JSON as single column value
						// i.e. a map with `_1` as key and value as the
						// non-object.
						kvs = jstream.KVS{jstream.KV{Key: "_1", Value: mv.Value}}
					}
					all = append(all, kvs)
				}
				// We don't need the input any more.
				//lint:ignore SA6002 Using pointer would allocate more since we would have to copy slice header before taking a pointer.
				r.bufferPool.Put(in.input)
				in.input = nil
				in.err = d.Err()
				in.dst <- all
			}
		}()
	}
}

// NewPReader - creates new parallel JSON reader using readCloser.
// Should only be used for LINES types.
func NewPReader(readCloser io.ReadCloser, args *ReaderArgs) *PReader {
	r := &PReader{
		args:       args,
		buf:        bufio.NewReaderSize(readCloser, jsonSplitSize*2),
		readCloser: readCloser,
		close:      make(chan struct{}),
	}
	r.startReaders()
	return r
}
