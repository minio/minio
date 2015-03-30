/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"net/http"
)

const (
	b = "bytes="
)

// HttpRange specifies the byte range to be sent to the client.
type httpRange struct {
	start, length, size int64
}

// GetContentRange populate range header
func (r *httpRange) getContentRange() string {
	return fmt.Sprintf("bytes %d-%d/%d", r.start, r.start+r.length-1, r.size)
}

// Grab new range from request header
func getRequestedRange(req *http.Request, size int64) (*httpRange, error) {
	r := &httpRange{
		start:  0,
		length: 0,
		size:   0,
	}
	r.size = size
	if s := req.Header.Get("Range"); s != "" {
		err := r.parseRange(s)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *httpRange) parse(ra string) error {
	i := strings.Index(ra, "-")
	if i < 0 {
		return errors.New("invalid range")
	}
	start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
	if start == "" {
		// If no start is specified, end specifies the
		// range start relative to the end of the file.
		i, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return errors.New("invalid range")
		}
		if i > r.size {
			i = r.size
		}
		r.start = r.size - i
		r.length = r.size - r.start
	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i > r.size || i < 0 {
			return errors.New("invalid range")
		}
		r.start = i
		if end == "" {
			// If no end is specified, range extends to end of the file.
			r.length = r.size - r.start
		} else {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || r.start > i {
				return errors.New("invalid range")
			}
			if i >= r.size {
				i = r.size - 1
			}
			r.length = i - r.start + 1
		}
	}
	return nil
}

// parseRange parses a Range header string as per RFC 2616.
func (r *httpRange) parseRange(s string) error {
	if s == "" {
		return errors.New("header not present")
	}
	if !strings.HasPrefix(s, b) {
		return errors.New("invalid range")
	}

	ras := strings.Split(s[len(b):], ",")
	if len(ras) == 0 {
		return errors.New("invalid request")
	}
	// Just pick the first one and ignore the rest, we only support one range per object
	if len(ras) > 1 {
		return errors.New("multiple ranges specified")
	}

	ra := strings.TrimSpace(ras[0])
	if ra == "" {
		return errors.New("invalid range")
	}
	return r.parse(ra)
}
