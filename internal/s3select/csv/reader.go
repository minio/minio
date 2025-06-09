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

package csv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"
	"unicode/utf8"

	csv "github.com/minio/csvparser"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/s3select/sql"
)

// Reader - CSV record reader for S3Select.
type Reader struct {
	args         *ReaderArgs
	readCloser   io.ReadCloser          // raw input
	buf          *bufio.Reader          // input to the splitter
	columnNames  []string               // names of columns
	nameIndexMap map[string]int64       // name to column index
	current      [][]string             // current block of results to be returned
	recordsRead  int                    // number of records read in current slice
	input        chan *queueItem        // input for workers
	queue        chan *queueItem        // output from workers in order
	err          error                  // global error state, only touched by Reader.Read
	bufferPool   bpool.Pool[[]byte]     // pool of []byte objects for input
	csvDstPool   bpool.Pool[[][]string] // pool of [][]string used for output
	close        chan struct{}          // used for shutting down the splitter before end of stream
	readerWg     sync.WaitGroup         // used to keep track of async reader.
}

// queueItem is an item in the queue.
type queueItem struct {
	input []byte          // raw input sent to the worker
	dst   chan [][]string // result of block decode
	err   error           // any error encountered will be set here
}

// Read - reads single record.
// Once Read is called the previous record should no longer be referenced.
func (r *Reader) Read(dst sql.Record) (sql.Record, error) {
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

	// If no index map, add that.
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

// csvSplitSize is the size of each block.
// Blocks will read this much and find the first following newline.
// 128KB appears to be a very reasonable default.
const csvSplitSize = 128 << 10

// startReaders will read the header if needed and spin up a parser
// and a number of workers based on GOMAXPROCS.
// If an error is returned no goroutines have been started and r.err will have been set.
func (r *Reader) startReaders(newReader func(io.Reader) *csv.Reader) error {
	if r.args.FileHeaderInfo != none {
		// Read column names
		// Get one line.
		b, err := r.nextSplit(0, nil)
		if err != nil {
			r.err = err
			return err
		}
		if !utf8.Valid(b) {
			return errInvalidTextEncodingError()
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

	r.bufferPool.New = func() []byte {
		return make([]byte, csvSplitSize+1024)
	}

	// Return first block
	next, nextErr := r.nextSplit(csvSplitSize, r.bufferPool.Get())
	// Check if first block is valid.
	if !utf8.Valid(next) {
		return errInvalidTextEncodingError()
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
			q := queueItem{
				input: next,
				dst:   make(chan [][]string, 1),
				err:   nextErr,
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
			if nextErr != nil {
				// Exit on any error.
				return
			}
			next, nextErr = r.nextSplit(csvSplitSize, r.bufferPool.Get())
		}
	}()

	// Start parsers
	for range runtime.GOMAXPROCS(0) {
		go func() {
			for in := range r.input {
				if len(in.input) == 0 {
					in.dst <- nil
					continue
				}
				dst := r.csvDstPool.Get()
				if len(dst) < 1000 {
					dst = make([][]string, 0, 1000)
				}

				cr := newReader(bytes.NewBuffer(in.input))
				all := dst[:0]
				err := func() error {
					// Read all records until EOF or another error.
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
						copy(recDst, record)
						all = append(all, recDst)
					}
				}()
				if err != nil {
					in.err = err
				}
				// We don't need the input any more.
				//nolint:staticcheck // SA6002 Using pointer would allocate more since we would have to copy slice header before taking a pointer.
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
		csvIn = &recordTransform{
			reader:          readCloser,
			recordDelimiter: []byte(args.RecordDelimiter),
			oneByte:         make([]byte, len(args.RecordDelimiter)-1),
		}
	}

	r := &Reader{
		args:       args,
		buf:        bufio.NewReaderSize(csvIn, csvSplitSize*2),
		readCloser: readCloser,
		close:      make(chan struct{}),
	}

	// Assume args are validated by ReaderArgs.UnmarshalXML()
	newCsvReader := func(r io.Reader) *csv.Reader {
		ret := csv.NewReader(r)
		ret.Comma = []rune(args.FieldDelimiter)[0]
		ret.Comment = []rune(args.CommentCharacter)[0]
		ret.Quote = []rune{}
		if len([]rune(args.QuoteCharacter)) > 0 {
			// Add the first rune of args.QuoteCharacter
			ret.Quote = append(ret.Quote, []rune(args.QuoteCharacter)[0])
		}
		ret.QuoteEscape = []rune(args.QuoteEscapeCharacter)[0]
		ret.FieldsPerRecord = -1
		// If LazyQuotes is true, a quote may appear in an unquoted field and a
		// non-doubled quote may appear in a quoted field.
		ret.LazyQuotes = true
		// We do not trim leading space to keep consistent with s3.
		ret.TrimLeadingSpace = false
		ret.ReuseRecord = true
		return ret
	}

	return r, r.startReaders(newCsvReader)
}
