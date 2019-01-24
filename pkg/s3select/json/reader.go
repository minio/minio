/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"

	"github.com/minio/minio/pkg/s3select/sql"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func toSingleLineJSON(input string, currentKey string, result gjson.Result) (output string, err error) {
	switch {
	case result.IsObject():
		result.ForEach(func(key, value gjson.Result) bool {
			jsonKey := key.String()
			if currentKey != "" {
				jsonKey = currentKey + "." + key.String()
			}
			output, err = toSingleLineJSON(input, jsonKey, value)
			input = output
			return err == nil
		})
	case result.IsArray():
		i := 0
		result.ForEach(func(key, value gjson.Result) bool {
			if currentKey == "" {
				panic("currentKey is empty")
			}

			indexKey := currentKey + "." + strconv.Itoa(i)
			output, err = toSingleLineJSON(input, indexKey, value)
			input = output
			i++
			return err == nil
		})
	default:
		output, err = sjson.Set(input, currentKey, result.Value())
	}

	return output, err
}

// Reader - JSON record reader for S3Select.
type Reader struct {
	args          *ReaderArgs
	objectScanner *bufio.Scanner
	readCloser    io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read() (sql.Record, error) {
	var data []byte
	if r.objectScanner.Scan() {
		data = r.objectScanner.Bytes()
	}
	if err := r.objectScanner.Err(); err != nil {
		return nil, errJSONParsingError(err)
	}

	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, io.EOF
	}

	if !gjson.ValidBytes(data) {
		return nil, errJSONParsingError(errors.New("invalid json"))
	}

	if bytes.Count(data, []byte("\n")) > 0 {
		s, err := toSingleLineJSON("", "", gjson.ParseBytes(data))
		if err != nil {
			return nil, errJSONParsingError(err)
		}
		data = []byte(s)
	}

	return &Record{
		data: data,
	}, nil
}

// Close - closes underlaying reader.
func (r *Reader) Close() error {
	return r.readCloser.Close()
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

// scanJSON is a split function for a Scanner that returns each JSON object
// stripped of any trailing end-of-line marker. The end-of-line marker is one
// optional carriage return followed by one mandatory newline. In regular
// expression notation, it is `\r?\n`. The last non-empty line of input
// will be returned even if it has no newline.
func scanJSON(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// The following logic navigates the slice
	// to find if we end with '}' where we expect
	// to end and follow through that with '\n'
	// This scanner split function would return
	// the appropriate portions of self contained
	// JSON for parsing by other upper callers.
	foundBracesEnd := false
	for i := 0; i < len(data); i++ {
		if data[i] == '\\' {
			continue
		}
		if data[i] == '}' {
			foundBracesEnd = true
		}
		if foundBracesEnd && data[i] == '\n' {
			return i + 1, dropCR(data[0:i]), nil
		}
	}

	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}

	// Request more data.
	return 0, nil, nil
}

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) *Reader {
	scanner := bufio.NewScanner(readCloser)
	scanner.Split(scanJSON)
	return &Reader{
		args:          args,
		objectScanner: scanner,
		readCloser:    readCloser,
	}
}
