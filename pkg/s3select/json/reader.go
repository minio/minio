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
	"bytes"
	"io"
	"io/ioutil"
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

type objectReader struct {
	reader io.Reader
	err    error

	p     []byte
	start int
	end   int

	escaped     bool
	quoteOpened bool
	curlyCount  uint64
	endOfObject bool
}

func (or *objectReader) objectEndIndex(p []byte, length int) int {
	for i := 0; i < length; i++ {
		if p[i] == '\\' {
			or.escaped = !or.escaped
			continue
		}

		if p[i] == '"' && !or.escaped {
			or.quoteOpened = !or.quoteOpened
		}

		or.escaped = false

		switch p[i] {
		case '{':
			if !or.quoteOpened {
				or.curlyCount++
			}
		case '}':
			if or.quoteOpened || or.curlyCount == 0 {
				break
			}

			if or.curlyCount--; or.curlyCount == 0 {
				return i + 1
			}
		}
	}

	return -1
}

func (or *objectReader) Read(p []byte) (n int, err error) {
	if or.endOfObject {
		return 0, io.EOF
	}

	if or.p != nil {
		n = copy(p, or.p[or.start:or.end])
		or.start += n
		if or.start == or.end {
			// made full copy.
			or.p = nil
			or.start = 0
			or.end = 0
		}
	} else {
		if or.err != nil {
			return 0, or.err
		}

		n, err = or.reader.Read(p)
		or.err = err
		switch err {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe:
			or.err = io.EOF
		default:
			return 0, err
		}
	}

	index := or.objectEndIndex(p, n)
	if index == -1 || index == n {
		return n, nil
	}

	or.endOfObject = true
	if or.p == nil {
		or.p = p
		or.start = index
		or.end = n
	} else {
		or.start -= index
	}

	return index, nil
}

func (or *objectReader) Reset() error {
	or.endOfObject = false

	if or.p != nil {
		return nil
	}

	return or.err
}

// Reader - JSON record reader for S3Select.
type Reader struct {
	args         *ReaderArgs
	objectReader *objectReader
	readCloser   io.ReadCloser
}

// Read - reads single record.
func (r *Reader) Read() (sql.Record, error) {
	if err := r.objectReader.Reset(); err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(r.objectReader)
	if err != nil {
		return nil, errJSONParsingError(err)
	}

	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, io.EOF
	}

	if !gjson.ValidBytes(data) {
		return nil, errJSONParsingError(err)
	}

	if bytes.Count(data, []byte("\n")) > 0 {
		var s string
		if s, err = toSingleLineJSON("", "", gjson.ParseBytes(data)); err != nil {
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

// NewReader - creates new JSON reader using readCloser.
func NewReader(readCloser io.ReadCloser, args *ReaderArgs) *Reader {
	return &Reader{
		args:         args,
		objectReader: &objectReader{reader: readCloser},
		readCloser:   readCloser,
	}
}
