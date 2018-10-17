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
	"encoding/csv"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
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

// CSVInput represents a record producing input from a  formatted file or pipe.
type CSVInput struct {
	options         *CSVOptions
	reader          *csv.Reader
	firstRow        []string
	header          []string
	minOutputLength int
	stats           *statInfo
}

// JSONInput represents a record producing input from a  formatted file or pipe.
type JSONInput struct {
	options         *JSONOptions
	reader          *jsoniter.Decoder
	firstRow        []string
	header          []string
	minOutputLength int
	stats           *statInfo
}

// ParseSelectTokens tokenizes the select query into required Columns, Alias, limit value
// where clause, aggregate functions, myFunctions, error.
type ParseSelectTokens struct {
	reqCols          []string
	alias            string
	myLimit          int64
	whereClause      interface{}
	aggFunctionNames []string
	myFuncs          *SelectFuncs
	myErr            error
}

// SelectQuery Interface has methods implemented by csv and JSON
type SelectQuery interface {
	Header() []string
	hasHeader() bool
	getHeader() []string
	getOutputFieldDelimiter() string
	getBytesProcessed(individualRecord map[string]interface{})
	Read() (map[string]interface{}, error)
	getExpression() string
	bytesReturned(int64)
	createStatXML() (string, error)
	createProgressXML() (string, error)
	colNameErrs(columnNames []string) error
	getProgressFlag() bool
}

// CSVOptions options are passed to the underlying encoding/csv reader.
type CSVOptions struct {
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

// JSONOptions options are passed to the underlying encoding/json reader.
type JSONOptions struct {

	// Name of the table that is used for querying
	Name string

	// ReadFrom is where the data will be read from.
	ReadFrom io.Reader

	// If true then we need to add gzip or bzip reader.
	// to extract the csv.
	Compressed string

	// SQL expression meant to be evaluated.
	Expression string

	// What the outputted JSON will be delimited by .
	RecordDelimiter string

	// Size of incoming object
	StreamSize int64

	// True if JSONType is DOCUMENTS
	Type bool

	// Progress enabled, enable/disable progress messages.
	Progress bool
}

// convertMySQL Replaces double quote escape for column names with backtick for
// the MySQL parser
func convertMySQL(random string) string {
	return strings.Replace(random, "\"", "`", len(random))
}

// readHeader reads the header into the header variable if the header is present
// as the first row of the csv
func (reader *CSVInput) readHeader() error {
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

// Row is a Struct for keeping track of key aspects of a row.
type Row struct {
	record string
	err    error
}

// Execute is the function where all the blocking occurs, It writes to the HTTP
// response writer in a streaming fashion so that the client can actively use
// the results before the query is finally finished executing. The
func Execute(writer io.Writer, inputType SelectQuery) error {
	myRow := make(chan *Row)
	curBuf := bytes.NewBuffer(make([]byte, 1000000))
	curBuf.Reset()
	progressTicker := time.NewTicker(progressTime)
	continuationTimer := time.NewTimer(continuationTime)
	defer progressTicker.Stop()
	defer continuationTimer.Stop()
	var expression string
	
	switch inputType.(type) {
	case *JSONInput:
		expression = inputType.getExpression()
	case *CSVInput:
		expression = inputType.getExpression()
	}

	go runSelectParser(convertMySQL(expression), inputType, myRow)

	for {
		select {
		case row, ok := <-myRow:
			if ok && row.err != nil {
				errorMessage := writeErrorMessage(row.err, curBuf)
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
				message := writeRecordMessage(row.record, curBuf)
				_, err := message.WriteTo(writer)
				flusher, okFlush := writer.(http.Flusher)
				if okFlush {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				inputType.bytesReturned(int64(len(row.record)))
				if !continuationTimer.Stop() {
					<-continuationTimer.C
				}
				continuationTimer.Reset(continuationTime)

			} else if !ok {
				statPayload, err := inputType.createStatXML()
				if err != nil {
					return err
				}
				statMessage := writeStatMessage(statPayload, curBuf)
				_, err = statMessage.WriteTo(writer)
				flusher, ok := writer.(http.Flusher)
				if ok {
					flusher.Flush()
				}
				if err != nil {
					return err
				}
				curBuf.Reset()
				message := writeEndMessage(curBuf)
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
			if inputType.getProgressFlag() {
				progressPayload, err := inputType.createProgressXML()
				if err != nil {
					return err
				}
				progressMessage := writeProgressMessage(progressPayload, curBuf)
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
			message := writeContinuationMessage(curBuf)
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
