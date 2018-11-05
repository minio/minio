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

package json

import (
	"encoding/json"
	"encoding/xml"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/pkg/s3select/format"
)

// Options options are passed to the underlying encoding/json reader.
type Options struct {

	// Name of the table that is used for querying
	Name string

	// ReadFrom is where the data will be read from.
	ReadFrom io.Reader

	// If true then we need to add gzip or bzip reader.
	// to extract the csv.
	Compressed string

	// SQL expression meant to be evaluated.
	Expression string

	// What the outputted  will be delimited by .
	RecordDelimiter string

	// Size of incoming object
	StreamSize int64

	// True if Type is DOCUMENTS
	Type bool

	// Progress enabled, enable/disable progress messages.
	Progress bool
}

// jinput represents a record producing input from a  formatted file or pipe.
type jinput struct {
	options         *Options
	reader          *jsoniter.Decoder
	firstRow        []string
	header          []string
	minOutputLength int
	stats           struct {
		BytesScanned   int64
		BytesReturned  int64
		BytesProcessed int64
	}
}

// New sets up a new, the first Json is read when this is run.
// If there is a problem with reading the first Json, the error is returned.
// Otherwise, the returned reader can be reliably consumed with jsonRead()
// until jsonRead() returns nil.
func New(opts *Options) (format.Select, error) {
	reader := &jinput{
		options: opts,
		reader:  jsoniter.NewDecoder(opts.ReadFrom),
	}
	reader.stats.BytesScanned = opts.StreamSize
	reader.stats.BytesProcessed = 0
	reader.stats.BytesReturned = 0

	return reader, nil
}

// Progress - return true if progress was requested.
func (reader *jinput) Progress() bool {
	return reader.options.Progress
}

// UpdateBytesProcessed - populates the bytes Processed
func (reader *jinput) UpdateBytesProcessed(record map[string]interface{}) {
	out, _ := json.Marshal(record)
	reader.stats.BytesProcessed += int64(len(out))
}

// Read the file and returns map[string]interface{}
func (reader *jinput) Read() (map[string]interface{}, error) {
	dec := reader.reader
	var record interface{}
	for {
		err := dec.Decode(&record)
		if err == io.EOF || err == io.ErrClosedPipe {
			break
		}
		if err != nil {
			return nil, format.ErrJSONParsingError
		}
		return record.(map[string]interface{}), nil
	}
	return nil, nil
}

// OutputFieldDelimiter - returns the delimiter specified in input request
func (reader *jinput) OutputFieldDelimiter() string {
	return ","
}

// HasHeader - returns true or false depending upon the header.
func (reader *jinput) HasHeader() bool {
	return false
}

// Expression - return the Select Expression for
func (reader *jinput) Expression() string {
	return reader.options.Expression
}

// UpdateBytesReturned - updates the Bytes returned for
func (reader *jinput) UpdateBytesReturned(size int64) {
	reader.stats.BytesReturned += size
}

// Header returns a nil in case of
func (reader *jinput) Header() []string {
	return nil
}

// CreateStatXML is the function which does the marshaling from the stat
// structs into XML so that the progress and stat message can be sent
func (reader *jinput) CreateStatXML() (string, error) {
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
func (reader *jinput) CreateProgressXML() (string, error) {
	if !(reader.options.Compressed != "NONE") {
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

// Type - return the data format type {
func (reader *jinput) Type() format.Type {
	return format.JSON
}

// ColNameErrs - this is a dummy function for JSON input type.
func (reader *jinput) ColNameErrs(columnNames []string) error {
	return nil
}
