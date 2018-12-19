/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"encoding/csv"
	"encoding/xml"
	"io"
	"strconv"
	"strings"

	"github.com/tidwall/sjson"

	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/s3select/format"
)

// Options options are passed to the underlying encoding/csv reader.
type Options struct {
	// HasHeader when true, will treat the first row as a header row.
	HasHeader bool

	// RecordDelimiter is the string that records are delimited by.
	RecordDelimiter string

	// FieldDelimiter is the string that fields are delimited by.
	FieldDelimiter string

	// Comments is the string the first character of a line of
	// text matches the comment character.
	Comments string

	// Name of the table that is used for querying
	Name string

	// ReadFrom is where the data will be read from.
	ReadFrom io.Reader

	// If true then we need to add gzip or bzip reader.
	// to extract the csv.
	Compressed string

	// SQL expression meant to be evaluated.
	Expression string

	// Output CSV will be delimited by.
	OutputFieldDelimiter string

	// Output CSV record will be delimited by.
	OutputRecordDelimiter string

	// Size of incoming object
	StreamSize int64

	// Whether Header is "USE" or another
	HeaderOpt bool

	// Progress enabled, enable/disable progress messages.
	Progress bool

	// Output format type, supported values are CSV and JSON
	OutputType format.Type
}

// cinput represents a record producing input from a formatted object.
type cinput struct {
	options         *Options
	reader          *csv.Reader
	firstRow        []string
	header          []string
	minOutputLength int
	stats           struct {
		BytesScanned   int64
		BytesReturned  int64
		BytesProcessed int64
	}
}

// New sets up a new Input, the first row is read when this is run.
// If there is a problem with reading the first row, the error is returned.
// Otherwise, the returned reader can be reliably consumed with Read().
// until Read() return err.
func New(opts *Options) (format.Select, error) {
	// DelimitedReader treats custom record delimiter like `\r\n`,`\r`,`ab` etc and replaces it with `\n`.
	normalizedReader := ioutil.NewDelimitedReader(opts.ReadFrom, []rune(opts.RecordDelimiter))
	reader := &cinput{
		options: opts,
		reader:  csv.NewReader(normalizedReader),
	}
	reader.stats.BytesScanned = opts.StreamSize
	reader.stats.BytesProcessed = 0
	reader.stats.BytesReturned = 0
	reader.firstRow = nil

	reader.reader.FieldsPerRecord = -1
	if reader.options.FieldDelimiter != "" {
		reader.reader.Comma = rune(reader.options.FieldDelimiter[0])
	}

	if reader.options.Comments != "" {
		reader.reader.Comment = rune(reader.options.Comments[0])
	}

	// QuoteCharacter - " (defaulted currently)
	reader.reader.LazyQuotes = true

	if err := reader.readHeader(); err != nil {
		return nil, err
	}

	return reader, nil
}

// Replace the spaces in columnnames with underscores
func cleanHeader(columns []string) []string {
	for i := range columns {
		// Even if header name is specified, some CSV's
		// might have column header names might be empty
		// and non-empty. In such a scenario we prepare
		// indexed value.
		if columns[i] == "" {
			columns[i] = "_" + strconv.Itoa(i)
		}
		columns[i] = strings.Replace(columns[i], " ", "_", -1)
	}
	return columns
}

// readHeader reads the header into the header variable if the header is present
// as the first row of the csv
func (reader *cinput) readHeader() error {
	var readErr error
	if reader.options.HasHeader {
		reader.firstRow, readErr = reader.reader.Read()
		if readErr != nil {
			return format.ErrCSVParsingError
		}
		reader.header = cleanHeader(reader.firstRow)
		reader.firstRow = nil
	} else {
		reader.firstRow, readErr = reader.reader.Read()
		if readErr != nil {
			return format.ErrCSVParsingError
		}
		reader.header = make([]string, len(reader.firstRow))
		for i := range reader.firstRow {
			reader.header[i] = "_" + strconv.Itoa(i)
		}
	}
	reader.minOutputLength = len(reader.header)
	return nil
}

// Progress - return true if progress was requested.
func (reader *cinput) Progress() bool {
	return reader.options.Progress
}

// UpdateBytesProcessed - populates the bytes Processed
func (reader *cinput) UpdateBytesProcessed(size int64) {
	reader.stats.BytesProcessed += size

}

// Read returns byte sequence
func (reader *cinput) Read() ([]byte, error) {
	dec := reader.readRecord()
	if dec != nil {
		var data []byte
		var err error
		// Navigate column values in reverse order to preserve
		// the input order for AWS S3 compatibility, because
		// sjson adds json key/value pairs in first in last out
		// fashion. This should be fixed in sjson ideally. Following
		// work around is needed to circumvent this issue for now.
		for i := len(dec) - 1; i >= 0; i-- {
			data, err = sjson.SetBytes(data, reader.header[i], dec[i])
			if err != nil {
				return nil, err
			}
		}
		return data, nil
	}
	return nil, nil
}

// OutputFieldDelimiter - returns the requested output field delimiter.
func (reader *cinput) OutputFieldDelimiter() string {
	return reader.options.OutputFieldDelimiter
}

// OutputRecordDelimiter - returns the requested output record delimiter.
func (reader *cinput) OutputRecordDelimiter() string {
	return reader.options.OutputRecordDelimiter
}

// HasHeader - returns true or false depending upon the header.
func (reader *cinput) HasHeader() bool {
	return reader.options.HasHeader
}

// Expression - return the Select Expression for
func (reader *cinput) Expression() string {
	return reader.options.Expression
}

// UpdateBytesReturned - updates the Bytes returned for
func (reader *cinput) UpdateBytesReturned(size int64) {
	reader.stats.BytesReturned += size
}

// Header returns the header of the reader. Either the first row if a header
// set in the options, or c#, where # is the column number, starting with 0.
func (reader *cinput) Header() []string {
	return reader.header
}

// readRecord reads a single record from the stream and it always returns successfully.
// If the record is empty, an empty []string is returned.
// Record expand to match the current row size, adding blank fields as needed.
// Records never return less then the number of fields in the first row.
// Returns nil on EOF
// In the event of a parse error due to an invalid record, it is logged, and
// an empty []string is returned with the number of fields in the first row,
// as if the record were empty.
//
// In general, this is a very tolerant of problems  reader.
func (reader *cinput) readRecord() []string {
	var row []string
	var fileErr error

	if reader.firstRow != nil {
		row = reader.firstRow
		reader.firstRow = nil
		return row
	}

	row, fileErr = reader.reader.Read()
	emptysToAppend := reader.minOutputLength - len(row)
	if fileErr == io.EOF || fileErr == io.ErrClosedPipe {
		return nil
	} else if _, ok := fileErr.(*csv.ParseError); ok {
		emptysToAppend = reader.minOutputLength
	}

	if emptysToAppend > 0 {
		for counter := 0; counter < emptysToAppend; counter++ {
			row = append(row, "")
		}
	}

	return row
}

// CreateStatXML is the function which does the marshaling from the stat
// structs into XML so that the progress and stat message can be sent
func (reader *cinput) CreateStatXML() (string, error) {
	if reader.options.Compressed == "NONE" {
		reader.stats.BytesProcessed = reader.options.StreamSize
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	out, err := xml.Marshal(&format.Stats{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	})
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}

// CreateProgressXML is the function which does the marshaling from the progress
// structs into XML so that the progress and stat message can be sent
func (reader *cinput) CreateProgressXML() (string, error) {
	if reader.options.HasHeader {
		reader.stats.BytesProcessed += format.ProcessSize(reader.header)
	}
	if reader.options.Compressed == "NONE" {
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	out, err := xml.Marshal(&format.Progress{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	})
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}

// Type - return the data format type
func (reader *cinput) Type() format.Type {
	return format.CSV
}

// OutputType - return the data format type
func (reader *cinput) OutputType() format.Type {
	return reader.options.OutputType
}

// ColNameErrs is a function which makes sure that the headers are requested are
// present in the file otherwise it throws an error.
func (reader *cinput) ColNameErrs(columnNames []string) error {
	for i := 0; i < len(columnNames); i++ {
		if columnNames[i] == "" {
			continue
		}
		if !format.IsInt(columnNames[i]) && !reader.options.HeaderOpt {
			return format.ErrInvalidColumnIndex
		}
		if format.IsInt(columnNames[i]) {
			tempInt, _ := strconv.Atoi(columnNames[i])
			if tempInt > len(reader.Header()) || tempInt == 0 {
				return format.ErrInvalidColumnIndex
			}
		} else {
			if reader.options.HeaderOpt && !format.StringInSlice(columnNames[i], reader.Header()) {
				return format.ErrParseInvalidPathComponent
			}
		}
	}
	return nil
}
