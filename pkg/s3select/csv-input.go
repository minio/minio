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

package s3select

import (
	"compress/bzip2"
	"encoding/csv"
	"encoding/xml"
	"io"
	"strconv"

	gzip "github.com/klauspost/pgzip"
	"github.com/minio/minio/pkg/ioutil"
)

// NewCSVInput sets up a new Input, the first row is read when this is run.
// If there is a problem with reading the first row, the error is returned.
// Otherwise, the returned reader can be reliably consumed with ReadRecord()
// until ReadRecord() returns nil.
func NewCSVInput(opts *CSVOptions) (*CSVInput, error) {
	myReader := opts.ReadFrom
	var tempBytesScanned int64
	tempBytesScanned = 0
	switch opts.Compressed {
	case "GZIP":
		tempBytesScanned = opts.StreamSize
		var err error
		if myReader, err = gzip.NewReader(opts.ReadFrom); err != nil {
			return nil, ErrTruncatedInput
		}
	case "BZIP2":
		tempBytesScanned = opts.StreamSize
		myReader = bzip2.NewReader(opts.ReadFrom)
	}

	// DelimitedReader treats custom record delimiter like `\r\n`,`\r`,`ab` etc and replaces it with `\n`.
	normalizedReader := ioutil.NewDelimitedReader(myReader, []rune(opts.RecordDelimiter))
	progress := &statInfo{
		BytesScanned:   tempBytesScanned,
		BytesProcessed: 0,
		BytesReturned:  0,
	}

	reader := &CSVInput{
		options: opts,
		reader:  csv.NewReader(normalizedReader),
		stats:   progress,
	}
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

// Return the status for the Progress flag
func (reader *CSVInput) getProgressFlag() bool {
	return reader.options.Progress
}

// Populates the bytes Processed
func (reader *CSVInput) getBytesProcessed(individualRecord map[string]interface{}) {
	// Convert map to slice of values.
	values := []string{}
	for _, value := range individualRecord {
		values = append(values, value.(string))
	}

	reader.stats.BytesProcessed += int64(len(values))

}

// Read the CSV file and returns map[string]interface{}
func (reader *CSVInput) Read() (map[string]interface{}, error) {
	record := make(map[string]interface{})
	dec := reader.ReadRecord()
	if dec != nil {
		if reader.options.HasHeader {
			columns := reader.header
			for i, value := range dec {
				record[columns[i]] = value
			}
		} else {
			for i, value := range dec {
				record["_"+strconv.Itoa(i)] = value
			}
		}
		return record, nil
	}
	return nil, nil
}

// It returns the delimiter specified in input request
func (reader *CSVInput) getOutputFieldDelimiter() string {
	return reader.options.OutputFieldDelimiter
}

// Returns true or false depending upon the header of CSV
func (reader *CSVInput) hasHeader() bool {
	return reader.options.HasHeader
}

// Return the header for CSV
func (reader *CSVInput) getHeader() []string {
	return reader.header
}

// Return the Select Expression for CSV
func (reader *CSVInput) getExpression() string {
	return reader.options.Expression
}

// Updates the Bytes returned for CSV
func (reader *CSVInput) bytesReturned(size int64) {
	reader.stats.BytesReturned += size
}

// Header returns the header of the reader. Either the first row if a header
// set in the options, or c#, where # is the column number, starting with 0.
func (reader *CSVInput) Header() []string {
	return reader.header
}

// ReadRecord reads a single record from the stream and it always returns successfully.
// If the record is empty, an empty []string is returned.
// Record expand to match the current row size, adding blank fields as needed.
// Records never return less then the number of fields in the first row.
// Returns nil on EOF
// In the event of a parse error due to an invalid record, it is logged, and
// an empty []string is returned with the number of fields in the first row,
// as if the record were empty.
//
// In general, this is a very tolerant of problems  reader.
func (reader *CSVInput) ReadRecord() []string {
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

// createStatXML is the function which does the marshaling from the stat
// structs into XML so that the progress and stat message can be sent
func (reader *CSVInput) createStatXML() (string, error) {
	if reader.options.Compressed == "NONE" {
		reader.stats.BytesProcessed = reader.options.StreamSize
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	out, err := xml.Marshal(&stats{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	})
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}

// createProgressXML is the function which does the marshaling from the progress
// structs into XML so that the progress and stat message can be sent
func (reader *CSVInput) createProgressXML() (string, error) {
	if reader.options.HasHeader {
		reader.stats.BytesProcessed += processSize(reader.header)
	}
	if reader.options.Compressed == "NONE" {
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	out, err := xml.Marshal(&progress{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	})
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}
