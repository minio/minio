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
	"net/http"
	"net/url"
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

// Parses x-amz-copy-source-range for CopyObjectPart API. Its behavior
// is different from regular HTTP range header. It only supports the
// form `bytes=first-last` where first and last are zero-based byte
// offsets. See
// http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
// for full details. This function treats an empty rangeString as
// referring to the whole resource.
//
// In addition to parsing the range string, it also validates the
// specified range against the given object size, so that Copy API
// specific error can be returned.
func parseCopyPartRange(rangeString string, resourceSize int64) (offset, length int64, err error) {
	var hrange *HTTPRangeSpec
	if rangeString != "" {
		hrange, err = parseRequestRangeSpec(rangeString)
		if err != nil {
			return -1, -1, err
		}

		// Require that both start and end are specified.
		if hrange.IsSuffixLength || hrange.Start == -1 || hrange.End == -1 {
			return -1, -1, errInvalidRange
		}

		// Validate specified range against object size.
		if hrange.Start >= resourceSize || hrange.End >= resourceSize {
			return -1, -1, errInvalidRangeSource
		}
	}

	return hrange.GetOffsetLength(resourceSize)
}
