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

package csv

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/minio/minio/pkg/s3select/sql"
)

// Reader - CSV record reader for S3Select.
type Reader struct {
	args         *ReaderArgs
	readCloser   io.ReadCloser
	buf          *bufio.Reader
	columnNames  []string
	nameIndexMap map[string]int64
	current      [][]string
	recordsRead  int
	input        chan *queueItem
	queue        chan *queueItem
	err          error
	parserErr    chan error
	bufferPool   sync.Pool
	csvDstPool   sync.Pool
	close        chan struct{}
}

type queueItem struct {
	input []byte
	dst   chan [][]string
	err   error
}

// Read - reads single record.
// Once Read is called the previous record should no longer be referenced.
func (r *Reader) Read(dst sql.Record) (sql.Record, error) {
	for len(r.current) <= r.recordsRead {
		if r.err != nil {
			return nil, r.err
		}
		item, ok := <-r.queue
		if !ok {
			r.err = io.EOF
			return nil, r.err
		}
		r.csvDstPool.Put(r.current)
		r.current = <-item.dst
		r.err = item.err
		r.recordsRead = 0
	}
	csvRecord := r.current[r.recordsRead]
	r.recordsRead++

	// If no column names are set, use _(index)
	if r.columnNames == nil {
		r.columnNames = make([]string, len(csvRecord))
		for i := range csvRecord {
			r.columnNames[i] = fmt.Sprintf("_%v", i+1)
		}
	}

	// If no index max, add that.
	if r.nameIndexMap == nil {
		r.nameIndexMap = make(map[string]int64)
		for i := range r.columnNames {
			r.nameIndexMap[r.columnNames[i]] = int64(i)
		}
	}
	dstRec, ok := dst.(*Record)
	if !ok {
		dstRec = &Record{}
	}
	dstRec.columnNames = r.columnNames
	dstRec.csvRecord = csvRecord
	dstRec.nameIndexMap = r.nameIndexMap

	return dstRec, nil
}

// Close - closes underlying reader.
func (r *Reader) Close() error {
	if r.close != nil {
		close(r.close)
		r.close = nil
	}
	return r.readCloser.Close()
}

// nextSplit will attempt to skip a number of bytes and
// return the buffer until the next newline occurs.
// The last block will be sent along with an io.EOF.
func (r *Reader) nextSplit(skip int, dst []byte) ([]byte, error) {
	if cap(dst) < skip {
		dst = make([]byte, 0, skip+1024)
	}
	dst = dst[:skip]
	if skip > 0 {
		n, err := io.ReadFull(r.buf, dst)
		if err != nil && err != io.ErrUnexpectedEOF {
			// If an EOF happens after reading some but not all the bytes,
			// ReadFull returns ErrUnexpectedEOF.
			return nil, err
		}
		dst = dst[:n]
	}
	// Read until next line.
	in, err := r.buf.ReadBytes('\n')
	dst = append(dst, in...)
	return dst, err
}

func (r *Reader) startReaders(in io.Reader, newReader func(io.Reader) *csv.Reader) error {
	if r.args.FileHeaderInfo != none {
		// Read column names
		// Get one line.
		b, err := r.nextSplit(0, nil)
		if err != nil {
			r.err = err
			return err
		}
		reader := newReader(bytes.NewReader(b))
		record, err := reader.Read()
		if err != nil {
			r.err = err
			if err != io.EOF {
				r.err = errCSVParsingError(err)
				return errCSVParsingError(err)
			}
			return err
		}

		if r.args.FileHeaderInfo == use {
			// Copy column names since records will be reused.
			columns := append(make([]string, 0, len(record)), record...)
			r.columnNames = columns
		}
	}
	const splitSize = 128 << 10

	r.bufferPool.New = func() interface{} {
		return make([]byte, splitSize+1024)
	}

	// Create queue
	r.queue = make(chan *queueItem, runtime.GOMAXPROCS(0))
	r.input = make(chan *queueItem, runtime.GOMAXPROCS(0))

	// Start splitter
	go func() {
		defer close(r.input)
		defer close(r.queue)
		var read int
		var blocks int
		for {
			next, err := r.nextSplit(splitSize, r.bufferPool.Get().([]byte))
			q := queueItem{
				input: next,
				dst:   make(chan [][]string, 1),
				err:   err,
			}
			read += len(next)
			blocks++
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
				dst, ok := r.csvDstPool.Get().([][]string)
				if !ok {
					dst = make([][]string, 0, 1000)
				}

				cr := newReader(bytes.NewBuffer(in.input))
				all := dst[:0]
				err := func() error {
					for {
						record, err := cr.Read()
						if err == io.EOF {
							return nil
						}
						if err != nil {
							return errCSVParsingError(err)
						}
						var recDst []string
						if len(dst) > len(all) {
							recDst = dst[len(all)]
						}
						if cap(recDst) < len(record) {
							recDst = make([]string, len(record))
						}
						recDst = recDst[:len(record)]
						for i := range record {
							recDst[i] = record[i]
						}
						all = append(all, recDst)
					}
				}()
				if err != nil {
					in.err = err
				}
				// We don't need the input any more.
				r.bufferPool.Put(in.input)
				in.input = nil
				in.dst <- all
			}
		}()
	}
	return nil

}

// NewReader - creates new CSV reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) (*Reader, error) {
	if args == nil || args.IsEmpty() {
		panic(fmt.Errorf("empty args passed %v", args))
	}
	csvIn := io.Reader(readCloser)
	if args.RecordDelimiter != "\n" {
		csvIn = &recordReader{
			reader:          readCloser,
			recordDelimiter: []byte(args.RecordDelimiter),
			oneByte:         make([]byte, len(args.RecordDelimiter)-1),
		}
	}

	r := &Reader{
		args:       args,
		buf:        bufio.NewReaderSize(csvIn, 1<<20),
		readCloser: readCloser,
		close:      make(chan struct{}),
	}

	// Assume args are validated by ReaderArgs.UnmarshalXML()
	newCsvReader := func(r io.Reader) *csv.Reader {
		ret := csv.NewReader(r)
		ret.Comma = []rune(args.FieldDelimiter)[0]
		ret.Comment = []rune(args.CommentCharacter)[0]
		ret.FieldsPerRecord = -1
		// If LazyQuotes is true, a quote may appear in an unquoted field and a
		// non-doubled quote may appear in a quoted field.
		ret.LazyQuotes = true
		// We do not trim leading space to keep consistent with s3.
		ret.TrimLeadingSpace = false
		ret.ReuseRecord = true
		return ret
	}

	return r, r.startReaders(csvIn, newCsvReader)
}
