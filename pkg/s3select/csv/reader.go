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

type recordReader struct {
	reader          io.Reader
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

	if string(rr.recordDelimiter) == "\n" {
		return n, nil
	}

	for {
		i := bytes.Index(p, rr.recordDelimiter)
		if i < 0 {
			break
		}

		p[i] = '\n'
		if len(rr.recordDelimiter) > 1 {
			p = append(p[:i+1], p[i+len(rr.recordDelimiter):]...)
		}
	}

	n = len(p)
	if len(rr.recordDelimiter) == 1 || p[n-1] != rr.recordDelimiter[0] {
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
	args        *ReaderArgs
	readCloser  io.ReadCloser
	csvReader   *csv.Reader
	columnNames []string
}

// Read - reads single record.
func (r *Reader) Read() (sql.Record, error) {
	csvRecord, err := r.csvReader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, errCSVParsingError(err)
		}

		return nil, err
	}

	columnNames := r.columnNames
	if columnNames == nil {
		columnNames = make([]string, len(csvRecord))
		for i := range csvRecord {
			columnNames[i] = fmt.Sprintf("_%v", i+1)
		}
	}

	nameIndexMap := make(map[string]int64)
	for i := range columnNames {
		nameIndexMap[columnNames[i]] = int64(i)
	}

	return &Record{
		columnNames:  columnNames,
		csvRecord:    csvRecord,
		nameIndexMap: nameIndexMap,
	}, nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

// NewReader - creates new CSV reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) (*Reader, error) {
	if args == nil || args.IsEmpty() {
		panic(fmt.Errorf("empty args passed %v", args))
	}

	csvReader := csv.NewReader(&recordReader{
		reader:          readCloser,
		recordDelimiter: []byte(args.RecordDelimiter),
		oneByte:         []byte{0},
	})
	csvReader.Comma = []rune(args.FieldDelimiter)[0]
	csvReader.Comment = []rune(args.CommentCharacter)[0]
	csvReader.FieldsPerRecord = -1
	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	csvReader.LazyQuotes = true
	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	csvReader.TrimLeadingSpace = true

	r := &Reader{
		args:       args,
		readCloser: readCloser,
		csvReader:  csvReader,
	}

	if args.FileHeaderInfo == none {
		return r, nil
	}

	record, err := csvReader.Read()
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
