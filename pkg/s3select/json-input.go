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
	"encoding/json"
	"encoding/xml"
	"io"

	jsoniter "github.com/json-iterator/go"
	gzip "github.com/klauspost/pgzip"
)

// NewJSONInput sets up a new Input, the first Json is read when this is run.
// If there is a problem with reading the first Json, the error is returned.
// Otherwise, the returned reader can be reliably consumed with jsonRead()
// until jsonRead() returns nil.
func NewJSONInput(opts *JSONOptions) (*JSONInput, error) {
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
	progress := &statInfo{
		BytesScanned:   tempBytesScanned,
		BytesProcessed: 0,
		BytesReturned:  0,
	}

	reader := &JSONInput{
		options: opts,
		reader:  jsoniter.NewDecoder(myReader),
		stats:   progress,
	}
	return reader, nil
}

// Return the status for the Progress flag
func (reader *JSONInput) getProgressFlag() bool {
	return reader.options.Progress
}

// Populates the bytes Processed
func (reader *JSONInput) getBytesProcessed(individualRecord map[string]interface{}) {
	out, _ := json.Marshal(individualRecord)
	reader.stats.BytesProcessed += int64(len(out))
}

// Read the JSON file and returns map[string]interface{}
func (reader *JSONInput) Read() (map[string]interface{}, error) {
	dec := reader.reader
	var record interface{}
	for {
		err := dec.Decode(&record)
		if err == io.EOF || err == io.ErrClosedPipe {
			break
		}
		if err != nil {
			return nil, ErrJSONParsingError
		}
		return record.(map[string]interface{}), nil
	}
	return nil, nil
}

// It returns the delimitter specified in input request
func (reader *JSONInput) getOutputFieldDelimiter() string {
	return ","
}

// JSON doesnot have any header so it always retunr false
func (reader *JSONInput) hasHeader() bool {
	return false
}

// JSON doesn't contain a Header so it return nil
func (reader *JSONInput) getHeader() []string {
	return nil
}

// Return the Select Expression for CSV
func (reader *JSONInput) getExpression() string {
	return reader.options.Expression
}

// Updates the Bytes returned for CSV
func (reader *JSONInput) bytesReturned(size int64) {
	reader.stats.BytesReturned += size
}

// Header returns a nil in case of JSON
func (reader *JSONInput) Header() []string {
	return nil
}

// createStatXML is the function which does the marshaling from the stat
// structs into XML so that the progress and stat message can be sent
func (reader *JSONInput) createStatXML() (string, error) {
	if reader.options.Compressed == "NONE" {
		reader.stats.BytesProcessed = reader.options.StreamSize
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	statXML := stats{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	}
	out, err := xml.Marshal(statXML)
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}

// createProgressXML is the function which does the marshaling from the progress
// structs into XML so that the progress and stat message can be sent
func (reader *JSONInput) createProgressXML() (string, error) {

	if !(reader.options.Compressed != "NONE") {
		reader.stats.BytesScanned = reader.stats.BytesProcessed
	}
	progressXML := &progress{
		BytesScanned:   reader.stats.BytesScanned,
		BytesProcessed: reader.stats.BytesProcessed,
		BytesReturned:  reader.stats.BytesReturned,
	}
	out, err := xml.Marshal(progressXML)
	if err != nil {
		return "", err
	}
	return xml.Header + string(out), nil
}
