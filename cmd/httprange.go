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

// Valid byte position regexp
var validBytePos = regexp.MustCompile(`^[0-9]+$`)

// HttpRange specifies the byte range to be sent to the client.
type objectRange struct {
	offsetBegin  int64
	offsetEnd    int64
	resourceSize int64
}

// String populate range stringer interface
func (r objectRange) String() string {
	return fmt.Sprintf("bytes %d-%d/%d", r.offsetBegin, r.offsetEnd, r.resourceSize)
}

// getlength - get length from the range.
func (r objectRange) getLength() int64 {
	return 1 + r.offsetEnd - r.offsetBegin
}

func parseObjectRange(objRangeStr string, resourceSize int64) (r *objectRange, err error) {
	if len(objRangeStr) == 0 {
		return nil, nil
	}
	// Check if range string contains delimiter '-', else return error. eg. "bytes=8"
	sepIndex := strings.Index(objRangeStr, "-")
	if sepIndex == -1 {
		return nil, traceError(fmt.Errorf("'%s' does not have a valid range value", objRangeStr))
	}

	offsetBeginStr := objRangeStr[:sepIndex]
	offsetBegin := int64(-1)
	// Convert offsetBeginStr only if its not empty.
	if len(offsetBeginStr) > 0 {
		if !validBytePos.MatchString(offsetBeginStr) {
			return nil, traceError(fmt.Errorf("'%s' does not have a valid first byte position value", objRangeStr))
		}

		if offsetBegin, err = strconv.ParseInt(offsetBeginStr, 10, 64); err != nil {
			return nil, traceError(fmt.Errorf("'%s' does not have a valid first byte position value", objRangeStr))
		}
	}

	offsetEndStr := objRangeStr[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndStr only if its not empty.
	if len(offsetEndStr) > 0 {
		if !validBytePos.MatchString(offsetEndStr) {
			return nil, traceError(fmt.Errorf("'%s' does not have a valid last byte position value", objRangeStr))
		}

		if offsetEnd, err = strconv.ParseInt(offsetEndStr, 10, 64); err != nil {
			return nil, traceError(fmt.Errorf("'%s' does not have a valid last byte position value", objRangeStr))
		}
	}

	// objRangeStr contains first and last byte positions. eg. "bytes=2-5"
	if offsetBegin > -1 && offsetEnd > -1 {
		if offsetBegin > offsetEnd {
			// Last byte position is not greater than first byte position. eg. "bytes=5-2"
			return nil, traceError(fmt.Errorf("'%s' does not have valid range value", objRangeStr))
		}

		// First and last byte positions should not be >= resourceSize.
		if offsetBegin >= resourceSize {
			return nil, traceError(InvalidRange{
				offsetBegin:  offsetBegin,
				offsetEnd:    offsetEnd,
				resourceSize: resourceSize,
			})
		}

		if offsetEnd >= resourceSize {
			offsetEnd = resourceSize - 1
		}
	} else if offsetBegin > -1 {
		// objRangeStr contains only first byte position. eg. "bytes=8-"
		if offsetBegin >= resourceSize {
			// First byte position should not be >= resourceSize.
			return nil, traceError(InvalidRange{
				offsetBegin:  offsetBegin,
				offsetEnd:    offsetEnd,
				resourceSize: resourceSize,
			})
		}

		offsetEnd = resourceSize - 1
	} else if offsetEnd > -1 {
		// objRangeStr contains only last byte position. eg. "bytes=-3"
		if offsetEnd == 0 {
			// Last byte position should not be zero eg. "bytes=-0"
			return nil, traceError(InvalidRange{
				offsetBegin:  offsetBegin,
				offsetEnd:    offsetEnd,
				resourceSize: resourceSize,
			})
		}

		if offsetEnd >= resourceSize {
			offsetBegin = 0
		} else {
			offsetBegin = resourceSize - offsetEnd
		}

		offsetEnd = resourceSize - 1
	} else {
		// objRangeStr contains first and last byte positions missing. eg. "bytes=-"
		return nil, traceError(fmt.Errorf("'%s' does not have valid range value", objRangeStr))
	}

	return &objectRange{offsetBegin, offsetEnd, resourceSize}, nil
}
