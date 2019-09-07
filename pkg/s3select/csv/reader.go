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
	"bytes"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/minio/minio/pkg/s3select/sql"
)

// recordReader will convert records to always have newline records.
type recordReader struct {
	reader io.Reader
	// recordDelimiter can be up to 2 characters.
	recordDelimiter []byte
	oneByte         []byte
	useOneByte      bool
}

func (rr *recordReader) Read(p []byte) (n int, err error) {
	if rr.useOneByte {
		p[0] = rr.oneByte[0]
		rr.useOneByte = false
		n, err = rr.reader.Read(p[1:])
		n++
	} else {
		n, err = rr.reader.Read(p)
	}

	if err != nil {
		return 0, err
	}

	// Do nothing if record-delimiter is already newline.
	if string(rr.recordDelimiter) == "\n" {
		return n, nil
	}

	// Change record delimiters to newline.
	if len(rr.recordDelimiter) == 1 {
		for idx := 0; idx < len(p); {
			i := bytes.Index(p[idx:], rr.recordDelimiter)
			if i < 0 {
				break
			}
			idx += i
			p[idx] = '\n'
		}
		return n, nil
	}

	// 2 characters...
	for idx := 0; idx < len(p); {
		i := bytes.Index(p[idx:], rr.recordDelimiter)
		if i < 0 {
			break
		}
		idx += i

		p[idx] = '\n'
		p = append(p[:idx+1], p[idx+2:]...)
		n--
	}

	if p[n-1] != rr.recordDelimiter[0] {
		return n, nil
	}

	if _, err = rr.reader.Read(rr.oneByte); err != nil {
		return 0, err
	}

	if rr.oneByte[0] == rr.recordDelimiter[1] {
		p[n-1] = '\n'
		return n, nil
	}

	rr.useOneByte = true
	return n, nil
}

// Reader - CSV record reader for S3Select.
type Reader struct {
	args         *ReaderArgs
	readCloser   io.ReadCloser
	csvReader    *csv.Reader
	columnNames  []string
	nameIndexMap map[string]int64
}

// Read - reads single record.
// Once Read is called the previous record should no longer be referenced.
func (r *Reader) Read(dst sql.Record) (sql.Record, error) {
	csvRecord, err := r.csvReader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errCSVParsingError(err)
		}

		return nil, err
	}

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
	return r.readCloser.Close()
}

// NewReader - creates new CSV reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) (*Reader, error) {
	if args == nil || args.IsEmpty() {
		panic(fmt.Errorf("empty args passed %v", args))
	}

	// Assume args are validated by ReaderArgs.UnmarshalXML()
	r := &Reader{
		args:       args,
		readCloser: readCloser,
	}
	if args.RecordDelimiter == "\n" {
		// No translation needed.
		r.csvReader = csv.NewReader(readCloser)
	} else {
		r.csvReader = csv.NewReader(&recordReader{
			reader:          readCloser,
			recordDelimiter: []byte(args.RecordDelimiter),
			oneByte:         make([]byte, len(args.RecordDelimiter)-1),
		})
	}
	r.csvReader.Comma = []rune(args.FieldDelimiter)[0]
	r.csvReader.Comment = []rune(args.CommentCharacter)[0]
	r.csvReader.FieldsPerRecord = -1
	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	r.csvReader.LazyQuotes = true
	// We do not trim leading space to keep consistent with s3.
	r.csvReader.TrimLeadingSpace = false
	r.csvReader.ReuseRecord = true

	if args.FileHeaderInfo == none {
		return r, nil
	}

	// Read column names
	record, err := r.csvReader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errCSVParsingError(err)
		}

		return nil, err
	}

	if args.FileHeaderInfo == use {
		r.columnNames = record
	}

	return r, nil
}
