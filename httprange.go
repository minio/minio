/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	byteRangePrefix = "bytes="
)

// errInvalidRange - returned when given range value is not valid.
var errInvalidRange = errors.New("Invalid range")

// InvalidRange - invalid range typed error.
type InvalidRange struct{}

func (e InvalidRange) Error() string {
	return "The requested range is not satisfiable"
}

// HttpRange specifies the byte range to be sent to the client.
type httpRange struct {
	firstBytePos int64
	lastBytePos  int64
	size         int64
}

// String populate range stringer interface
func (hrange httpRange) String() string {
	return fmt.Sprintf("bytes %d-%d/%d", hrange.firstBytePos, hrange.lastBytePos, hrange.size)
}

// getLength - get length from the range.
func (hrange httpRange) getLength() int64 {
	return 1 + hrange.lastBytePos - hrange.firstBytePos
}

func parseRequestRange(rangeString string, size int64) (hrange *httpRange, err error) {
	// Return error if given range string doesn't start with byte range prefix.
	if !strings.HasPrefix(rangeString, byteRangePrefix) {
		return nil, fmt.Errorf("'%s' does not start with '%s'", rangeString, byteRangePrefix)
	}

	// Trim byte range prefix.
	byteRangeString := strings.TrimPrefix(rangeString, byteRangePrefix)

	// Check if range string contains delimiter '-', else return error.
	sepIndex := strings.Index(byteRangeString, "-")
	if sepIndex == -1 {
		return nil, fmt.Errorf("invalid range string '%s'", rangeString)
	}

	firstBytePosString := byteRangeString[:sepIndex]
	lastBytePosString := byteRangeString[sepIndex+1:]

	firstBytePos := int64(-1)
	lastBytePos := int64(-1)

	// Convert firstBytePosString only if its not empty.
	if len(firstBytePosString) > 0 {
		if firstBytePos, err = strconv.ParseInt(firstBytePosString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have valid first byte position value", rangeString)
		}
	}

	// Convert lastBytePosString only if its not empty.
	if len(lastBytePosString) > 0 {
		if lastBytePos, err = strconv.ParseInt(lastBytePosString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have valid last byte position value", rangeString)
		}
	}

	// Return error if firstBytePosString and lastBytePosString are empty.
	if firstBytePos == -1 && lastBytePos == -1 {
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	}

	if firstBytePos == -1 {
		// Return error if lastBytePos is zero and firstBytePos is not given eg. "bytes=-0"
		if lastBytePos == 0 {
			return nil, errInvalidRange
		}
		firstBytePos = size - lastBytePos
		lastBytePos = size - 1
	} else if lastBytePos == -1 {
		lastBytePos = size - 1
	} else if firstBytePos > lastBytePos {
		// Return error if firstBytePos is greater than lastBytePos
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	} else if lastBytePos >= size {
		// Set lastBytePos is out of size range.
		lastBytePos = size - 1
	}

	return &httpRange{firstBytePos, lastBytePos, size}, nil
}
