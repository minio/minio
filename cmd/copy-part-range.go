/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Writes S3 compatible copy part range error.
func writeCopyPartErr(w http.ResponseWriter, err error, url *url.URL) {
	switch err {
	case errInvalidRange:
		writeErrorResponse(w, ErrInvalidCopyPartRange, url)
		return
	case errInvalidRangeSource:
		writeErrorResponse(w, ErrInvalidCopyPartRangeSource, url)
		return
	default:
		writeErrorResponse(w, ErrInternalError, url)
		return
	}
}

// Parses x-amz-copy-source-range for CopyObjectPart API. Specifically written to
// differentiate the behavior between regular httpRange header v/s x-amz-copy-source-range.
// The range of bytes to copy from the source object. The range value must use the form
// bytes=first-last, where the first and last are the zero-based byte offsets to copy.
// For example, bytes=0-9 indicates that you want to copy the first ten bytes of the source.
// http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
func parseCopyPartRange(rangeString string, resourceSize int64) (hrange *httpRange, err error) {
	// Return error if given range string doesn't start with byte range prefix.
	if !strings.HasPrefix(rangeString, byteRangePrefix) {
		return nil, fmt.Errorf("'%s' does not start with '%s'", rangeString, byteRangePrefix)
	}

	// Trim byte range prefix.
	byteRangeString := strings.TrimPrefix(rangeString, byteRangePrefix)

	// Check if range string contains delimiter '-', else return error. eg. "bytes=8"
	sepIndex := strings.Index(byteRangeString, "-")
	if sepIndex == -1 {
		return nil, errInvalidRange
	}

	offsetBeginString := byteRangeString[:sepIndex]
	offsetBegin := int64(-1)
	// Convert offsetBeginString only if its not empty.
	if len(offsetBeginString) > 0 {
		if !validBytePos.MatchString(offsetBeginString) {
			return nil, errInvalidRange
		}
		if offsetBegin, err = strconv.ParseInt(offsetBeginString, 10, 64); err != nil {
			return nil, errInvalidRange
		}
	}

	offsetEndString := byteRangeString[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndString only if its not empty.
	if len(offsetEndString) > 0 {
		if !validBytePos.MatchString(offsetEndString) {
			return nil, errInvalidRange
		}
		if offsetEnd, err = strconv.ParseInt(offsetEndString, 10, 64); err != nil {
			return nil, errInvalidRange
		}
	}

	// rangeString contains first byte positions. eg. "bytes=2-" or
	// rangeString contains last bye positions. eg. "bytes=-2"
	if offsetBegin == -1 || offsetEnd == -1 {
		return nil, errInvalidRange
	}

	// Last byte position should not be greater than first byte
	// position. eg. "bytes=5-2"
	if offsetBegin > offsetEnd {
		return nil, errInvalidRange
	}

	// First and last byte positions should not be >= resourceSize.
	if offsetBegin >= resourceSize || offsetEnd >= resourceSize {
		return nil, errInvalidRangeSource
	}

	// Success..
	return &httpRange{offsetBegin, offsetEnd, resourceSize}, nil
}
