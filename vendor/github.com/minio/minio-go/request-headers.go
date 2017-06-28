/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2016-17 Minio, Inc.
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

package minio

import (
	"fmt"
	"net/http"
	"time"
)

// RequestHeaders - implement methods for setting special
// request headers for GET, HEAD object operations.
// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
type RequestHeaders struct {
	http.Header
}

// NewGetReqHeaders - initializes a new request headers for GET request.
func NewGetReqHeaders() RequestHeaders {
	return RequestHeaders{
		Header: make(http.Header),
	}
}

// NewHeadReqHeaders - initializes a new request headers for HEAD request.
func NewHeadReqHeaders() RequestHeaders {
	return RequestHeaders{
		Header: make(http.Header),
	}
}

// SetMatchETag - set match etag.
func (c RequestHeaders) SetMatchETag(etag string) error {
	if etag == "" {
		return ErrInvalidArgument("ETag cannot be empty.")
	}
	c.Set("If-Match", "\""+etag+"\"")
	return nil
}

// SetMatchETagExcept - set match etag except.
func (c RequestHeaders) SetMatchETagExcept(etag string) error {
	if etag == "" {
		return ErrInvalidArgument("ETag cannot be empty.")
	}
	c.Set("If-None-Match", "\""+etag+"\"")
	return nil
}

// SetUnmodified - set unmodified time since.
func (c RequestHeaders) SetUnmodified(modTime time.Time) error {
	if modTime.IsZero() {
		return ErrInvalidArgument("Modified since cannot be empty.")
	}
	c.Set("If-Unmodified-Since", modTime.Format(http.TimeFormat))
	return nil
}

// SetModified - set modified time since.
func (c RequestHeaders) SetModified(modTime time.Time) error {
	if modTime.IsZero() {
		return ErrInvalidArgument("Modified since cannot be empty.")
	}
	c.Set("If-Modified-Since", modTime.Format(http.TimeFormat))
	return nil
}

// SetRange - set the start and end offset of the object to be read.
// See https://tools.ietf.org/html/rfc7233#section-3.1 for reference.
func (c RequestHeaders) SetRange(start, end int64) error {
	switch {
	case start == 0 && end < 0:
		// Read last '-end' bytes. `bytes=-N`.
		c.Set("Range", fmt.Sprintf("bytes=%d", end))
	case 0 < start && end == 0:
		// Read everything starting from offset
		// 'start'. `bytes=N-`.
		c.Set("Range", fmt.Sprintf("bytes=%d-", start))
	case 0 <= start && start <= end:
		// Read everything starting at 'start' till the
		// 'end'. `bytes=N-M`
		c.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	default:
		// All other cases such as
		// bytes=-3-
		// bytes=5-3
		// bytes=-2-4
		// bytes=-3-0
		// bytes=-3--2
		// are invalid.
		return ErrInvalidArgument(
			fmt.Sprintf(
				"Invalid range specified: start=%d end=%d",
				start, end))
	}
	return nil
}
