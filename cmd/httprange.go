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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	byteRangePrefix = "bytes="
)

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

// GetLength - get length of range
func (h *HTTPRangeSpec) GetLength(resourceSize int64) (rangeLength int64, err error) {
	switch {
	case resourceSize < 0:
		return 0, errors.New("Resource size cannot be negative")

	case h == nil:
		rangeLength = resourceSize

	case h.IsSuffixLength:
		specifiedLen := -h.Start
		rangeLength = specifiedLen
		if specifiedLen > resourceSize {
			rangeLength = resourceSize
		}

	case h.Start >= resourceSize:
		return 0, errInvalidRange

	case h.End > -1:
		end := h.End
		if resourceSize <= end {
			end = resourceSize - 1
		}
		rangeLength = end - h.Start + 1

	case h.End == -1:
		rangeLength = resourceSize - h.Start

	default:
		return 0, errors.New("Unexpected range specification case")
	}

	return rangeLength, nil
}

// GetOffsetLength computes the start offset and length of the range
// given the size of the resource
func (h *HTTPRangeSpec) GetOffsetLength(resourceSize int64) (start, length int64, err error) {
	if h == nil {
		// No range specified, implies whole object.
		return 0, resourceSize, nil
	}

	length, err = h.GetLength(resourceSize)
	if err != nil {
		return 0, 0, err
	}

	start = h.Start
	if h.IsSuffixLength {
		start = resourceSize + h.Start
		if start < 0 {
			start = 0
		}
	}
	return start, length, nil
}

// Parse a HTTP range header value into a HTTPRangeSpec
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
		if offsetBeginString[0] == '+' {
			return nil, fmt.Errorf("Byte position ('%s') must not have a sign", offsetBeginString)
		} else if offsetBegin, err = strconv.ParseInt(offsetBeginString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		} else if offsetBegin < 0 {
			return nil, fmt.Errorf("First byte position is negative ('%d')", offsetBegin)
		}
	}

	offsetEndString := byteRangeString[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndString only if its not empty.
	if len(offsetEndString) > 0 {
		if offsetEndString[0] == '+' {
			return nil, fmt.Errorf("Byte position ('%s') must not have a sign", offsetEndString)
		} else if offsetEnd, err = strconv.ParseInt(offsetEndString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		} else if offsetEnd < 0 {
			return nil, fmt.Errorf("Last byte position is negative ('%d')", offsetEnd)
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
