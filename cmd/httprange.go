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

package cmd

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	byteRangePrefix = "bytes="
)

// Valid byte position regexp
var validBytePos = regexp.MustCompile(`^[0-9]+$`)

// HttpRange specifies the byte range to be sent to the client.
type httpRange struct {
	offsetBegin  int64
	offsetEnd    int64
	resourceSize int64
}

// String populate range stringer interface
func (hrange httpRange) String() string {
	return fmt.Sprintf("bytes %d-%d/%d", hrange.offsetBegin, hrange.offsetEnd, hrange.resourceSize)
}

// getlength - get length from the range.
func (hrange httpRange) getLength() int64 {
	return 1 + hrange.offsetEnd - hrange.offsetBegin
}

func parseRequestRange(rangeString string, resourceSize int64) (hrange *httpRange, err error) {
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

	offsetBeginString := byteRangeString[:sepIndex]
	offsetBegin := int64(-1)
	// Convert offsetBeginString only if its not empty.
	if len(offsetBeginString) > 0 {
		if !validBytePos.MatchString(offsetBeginString) {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}

		if offsetBegin, err = strconv.ParseInt(offsetBeginString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}
	}

	offsetEndString := byteRangeString[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndString only if its not empty.
	if len(offsetEndString) > 0 {
		if !validBytePos.MatchString(offsetEndString) {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}

		if offsetEnd, err = strconv.ParseInt(offsetEndString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}
	}

	// rangeString contains first and last byte positions. eg. "bytes=2-5"
	switch {
	case offsetBegin > -1 && offsetEnd > -1:
		if offsetBegin > offsetEnd {
			// Last byte position is not greater than first byte position. eg. "bytes=5-2"
			return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
		}

		// First and last byte positions should not be >= resourceSize.
		if offsetBegin >= resourceSize {
			return nil, errInvalidRange
		}

		if offsetEnd >= resourceSize {
			offsetEnd = resourceSize - 1
		}
	case offsetBegin > -1:
		// rangeString contains only first byte position. eg. "bytes=8-"
		if offsetBegin >= resourceSize {
			// First byte position should not be >= resourceSize.
			return nil, errInvalidRange
		}

		offsetEnd = resourceSize - 1
	case offsetEnd > -1:
		// rangeString contains only last byte position. eg. "bytes=-3"
		if offsetEnd == 0 {
			// Last byte position should not be zero eg. "bytes=-0"
			return nil, errInvalidRange
		}

		if offsetEnd >= resourceSize {
			offsetBegin = 0
		} else {
			offsetBegin = resourceSize - offsetEnd
		}

		offsetEnd = resourceSize - 1
	default:
		// rangeString contains first and last byte positions missing. eg. "bytes=-"
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	}

	return &httpRange{offsetBegin, offsetEnd, resourceSize}, nil
}

// HTTPRangeSpec represents a range specification as supported by S3 GET
// object request.
//
// Case 1: Not present -> represented by a nil RangeSpec
// Case 2: bytes=1-10 (absolute start and end offsets) -> RangeSpec{false, 1, 10}
// Case 3: bytes=10- (absolute start offset with end offset unspecified) -> RangeSpec{false, 10, -1}
// Case 4: bytes=-30 (suffix length specification) -> RangeSpec{true, -30, -1}
type HTTPRangeSpec struct {
	// Does the range spec refer to a suffix of the object?
	IsSuffixLength bool

	// Start and end offset specified in range spec
	Start, End int64
}

// ContentRangeString populate range stringer interface
func (h *HTTPRangeSpec) ContentRangeString(resourceSize int64) string {
	start, rangeLength := h.GetOffsetLength(resourceSize)
	return fmt.Sprintf("bytes %d-%d/%d", start, start+rangeLength-1, resourceSize)
}

// GetLength - get length of range
func (h *HTTPRangeSpec) GetLength(resourceSize int64) int64 {
	switch {
	case h.IsSuffixLength:
		specifiedLen := -h.Start
		if specifiedLen > resourceSize {
			specifiedLen = resourceSize
		}
		return specifiedLen
	case h.End > -1:
		end := h.End
		if resourceSize < end {
			end = resourceSize - 1
		}
		return end - h.Start + 1
	default:
		return resourceSize - h.Start
	}
}

// GetOffsetLength computes the start offset and length of the range
// given the size of the resource
func (h *HTTPRangeSpec) GetOffsetLength(resourceSize int64) (start int64, length int64) {
	length = h.GetLength(resourceSize)
	start = h.Start
	if h.IsSuffixLength {
		start = resourceSize + h.Start
	}
	return
}

// Parses a range header value into a HTTPRangeSpec
func parseRequestRangeSpec(rangeString string) (hrange *HTTPRangeSpec, err error) {
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

	offsetBeginString := byteRangeString[:sepIndex]
	offsetBegin := int64(-1)
	// Convert offsetBeginString only if its not empty.
	if len(offsetBeginString) > 0 {
		if offsetBegin, err = strconv.ParseInt(offsetBeginString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}
	}

	offsetEndString := byteRangeString[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndString only if its not empty.
	if len(offsetEndString) > 0 {
		if offsetEnd, err = strconv.ParseInt(offsetEndString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}
	}

	switch {
	case offsetBegin > -1 && offsetEnd > -1:
		if offsetBegin > offsetEnd {
			return nil, errInvalidRange
		}
		return &HTTPRangeSpec{false, offsetBegin, offsetEnd}, nil
	case offsetBegin > -1:
		return &HTTPRangeSpec{false, offsetBegin, -1}, nil
	case offsetEnd > -1:
		if offsetEnd == 0 {
			return nil, errInvalidRange
		}
		return &HTTPRangeSpec{true, -offsetEnd, -1}, nil
	default:
		// rangeString contains first and last byte positions missing. eg. "bytes=-"
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	}
}
