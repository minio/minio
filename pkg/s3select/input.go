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
	"bytes"
	"compress/bzip2"
	"encoding/csv"
	"encoding/xml"
	"io"
	"strconv"
	"strings"
	"time"

	"net/http"

	gzip "github.com/klauspost/pgzip"
	"github.com/minio/minio/pkg/ioutil"
)

const (
	// progressTime is the time interval for which a progress message is sent.
	progressTime time.Duration = 60 * time.Second
	// continuationTime is the time interval for which a continuation message is
	// sent.
	continuationTime time.Duration = 5 * time.Second
)

// progress represents a struct that represents the format for XML of the
// progress messages
type progress struct {
	XMLName        xml.Name `xml:"Progress" json:"-"`
	BytesScanned   int64    `xml:"BytesScanned"`
	BytesProcessed int64    `xml:"BytesProcessed"`
	BytesReturned  int64    `xml:"BytesReturned"`
}

// stats represents a struct that represents the format for XML of the stat
// messages
type stats struct {
	XMLName        xml.Name `xml:"Stats" json:"-"`
	BytesScanned   int64    `xml:"BytesScanned"`
	BytesProcessed int64    `xml:"BytesProcessed"`
	BytesReturned  int64    `xml:"BytesReturned"`
}

// StatInfo is a struct that represents the
type statInfo struct {
	BytesScanned   int64
	BytesReturned  int64
	BytesProcessed int64
}

// Input represents a record producing input from a  formatted file or pipe.
type Input struct {
	options         *Options
	reader          *csv.Reader
	firstRow        []string
	header          []string
	minOutputLength int
	stats           *statInfo
}

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

	// What the outputted CSV will be delimited by .
	OutputFieldDelimiter string

	// Size of incoming object
	StreamSize int64

	// Whether Header is "USE" or another
	HeaderOpt bool

	// Progress enabled, enable/disable progress messages.
	Progress bool
}

// NewInput sets up a new Input, the first row is read when this is run.
// If there is a problem with reading the first row, the error is returned.
// Otherwise, the returned reader can be reliably consumed with ReadRecord()
// until ReadRecord() returns nil.
func NewInput(opts *Options) (*Input, error) {
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

	reader := &Input{
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

// ReadRecord reads a single record from the . Always returns successfully.
// If the record is empty, an empty []string is returned.
// Record expand to match the current row size, adding blank fields as needed.
// Records never return less then the number of fields in the first row.
// Returns nil on EOF
// In the event of a parse error due to an invalid record, it is logged, and
// an empty []string is returned with the number of fields in the first row,
// as if the record were empty.
//
// In general, this is a very tolerant of problems  reader.
func (reader *Input) ReadRecord() []string {
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

// convertMySQL Replaces double quote escape for column names with backtick for
// the MySQL parser
func convertMySQL(random string) string {
	return strings.Replace(random, "\"", "`", len(random))
}

// readHeader reads the header into the header variable if the header is present
// as the first row of the csv
func (reader *Input) readHeader() error {
	var readErr error
	if reader.options.HasHeader {
		reader.firstRow, readErr = reader.reader.Read()
		if readErr != nil {
			return ErrCSVParsingError
		}
		reader.header = reader.firstRow
		reader.firstRow = nil
		reader.minOutputLength = len(reader.header)
	} else {
		reader.firstRow, readErr = reader.reader.Read()
		reader.header = make([]string, len(reader.firstRow))
		for i := 0; i < reader.minOutputLength; i++ {
			reader.header[i] = strconv.Itoa(i)
		}

	}
	return nil
}

// createStatXML is the function which does the marshaling from the stat
// structs into XML so that the progress and stat message can be sent
func (reader *Input) createStatXML() (string, error) {
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

// createProgressXML is the function which does the marshaling from the progress structs into XML so that the progress and stat message can be sent
func (reader *Input) createProgressXML() (string, error) {
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

// Header returns the header of the reader. Either the first row if a header
// set in the options, or c#, where # is the column number, starting with 0.
func (reader *Input) Header() []string {
	return reader.header
}

// Row is a Struct for keeping track of key aspects of a row.
type Row struct {
	record string
	err    error
}

// Execute is the function where all the blocking occurs, It writes to the HTTP
// response writer in a streaming fashion so that the client can actively use
// the results before the query is finally finished executing. The
func (reader *Input) Execute(writer io.Writer) error {
	myRow := make(chan *Row)
	curBuf := bytes.NewBuffer(make([]byte, 1000000))
	curBuf.Reset()
	progressTicker := time.NewTicker(progressTime)
	continuationTimer := time.NewTimer(continuationTime)
	defer progressTicker.Stop()
	defer continuationTimer.Stop()
	go reader.runSelectParser(convertMySQL(reader.options.Expression), myRow)
	for {
		select {
		case row, ok := <-myRow:
			if ok && row.err != nil {
				errorMessage := reader.writeErrorMessage(row.err, curBuf)
				_, err := errorMessage.WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				close(myRow)
				return nil
			} else if ok {
				message := reader.writeRecordMessage(row.record, curBuf)
				_, err := message.WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				reader.stats.BytesReturned += int64(len(row.record))
				if !continuationTimer.Stop() {
					<-continuationTimer.C
				}
				continuationTimer.Reset(continuationTime)
			} else if !ok {
				statPayload, err := reader.createStatXML()
				if err != nil {
					return err
				}
				statMessage := reader.writeStatMessage(statPayload, curBuf)
				_, err = statMessage.WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				message := reader.writeEndMessage(curBuf)
				_, err = message.WriteTo(writer)
				flusher, ok = writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				return nil
			}

		case <-progressTicker.C:
			// Send progress messages only if requested by client.
			if reader.options.Progress {
				progressPayload, err := reader.createProgressXML()
				if err != nil {
					return err
				}
				progressMessage := reader.writeProgressMessage(progressPayload, curBuf)
				_, err = progressMessage.WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
			}
		case <-continuationTimer.C:
			message := reader.writeContinuationMessage(curBuf)
			_, err := message.WriteTo(writer)
			flusher, ok := writer.(http.Flusher)
			if ok {
				flusher.Flush()
			}
			if err != nil {
				return err
			}
			curBuf.Reset()
			continuationTimer.Reset(continuationTime)
		}
	}
}
