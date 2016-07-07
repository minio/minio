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
	"regexp"
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

// Valid byte position regexp
var validBytePos = regexp.MustCompile(`^[0-9]+$`)

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

	// Check if range string contains delimiter '-', else return error. eg. "bytes=8"
	sepIndex := strings.Index(byteRangeString, "-")
	if sepIndex == -1 {
		return nil, fmt.Errorf("'%s' does not have a valid range value", rangeString)
	}

	firstBytePosString := byteRangeString[:sepIndex]
	firstBytePos := int64(-1)
	// Convert firstBytePosString only if its not empty.
	if len(firstBytePosString) > 0 {
		if !validBytePos.MatchString(firstBytePosString) {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}

		if firstBytePos, err = strconv.ParseInt(firstBytePosString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}
	}

	lastBytePosString := byteRangeString[sepIndex+1:]
	lastBytePos := int64(-1)
	// Convert lastBytePosString only if its not empty.
	if len(lastBytePosString) > 0 {
		if !validBytePos.MatchString(lastBytePosString) {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}

		if lastBytePos, err = strconv.ParseInt(lastBytePosString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}
	}

	// rangeString contains first and last byte positions. eg. "bytes=2-5"
	if firstBytePos > -1 && lastBytePos > -1 {
		if firstBytePos > lastBytePos {
			// Last byte position is not greater than first byte position. eg. "bytes=5-2"
			return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
		}

		// First and last byte positions should not be >= size.
		if firstBytePos >= size {
			return nil, errInvalidRange
		}

		if lastBytePos >= size {
			lastBytePos = size - 1
		}
	} else if firstBytePos > -1 {
		// rangeString contains only first byte position. eg. "bytes=8-"
		if firstBytePos >= size {
			// First byte position should not be >= size.
			return nil, errInvalidRange
		}

		lastBytePos = size - 1
	} else if lastBytePos > -1 {
		// rangeString contains only last byte position. eg. "bytes=-3"
		if lastBytePos == 0 {
			// Last byte position should not be zero eg. "bytes=-0"
			return nil, errInvalidRange
		}

		if lastBytePos >= size {
			firstBytePos = 0
		} else {
			firstBytePos = size - lastBytePos
		}

		lastBytePos = size - 1
	} else {
		// rangeString contains first and last byte positions missing. eg. "bytes=-"
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	}

	return &httpRange{firstBytePos, lastBytePos, size}, nil
}
