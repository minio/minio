// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/klauspost/compress/gzip"
)

// ResponseRecorder - is a wrapper to trap the http response
// status code and to record the response body
type ResponseRecorder struct {
	http.ResponseWriter
	io.ReaderFrom
	StatusCode int
	// Log body of 4xx or 5xx responses
	LogErrBody bool
	// Log body of all responses
	LogAllBody bool

	ttfbHeader time.Duration
	ttfbBody   time.Duration

	StartTime time.Time
	// number of bytes written
	bytesWritten int
	// number of bytes of response headers written
	headerBytesWritten int
	// Internal recording buffer
	headers bytes.Buffer
	body    bytes.Buffer
	// Indicate if headers are written in the log
	headersLogged bool
}

// Hijack - hijacks the underlying connection
func (lrw *ResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := lrw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking. Type is %T", lrw.ResponseWriter)
	}
	return hj.Hijack()
}

// TTFB of the request - this function needs to be called
// when the request is finished to provide accurate data
func (lrw *ResponseRecorder) TTFB() time.Duration {
	if lrw.ttfbBody != 0 {
		return lrw.ttfbBody
	}
	return lrw.ttfbHeader
}

// NewResponseRecorder - returns a wrapped response writer to trap
// http status codes for auditing purposes.
func NewResponseRecorder(w http.ResponseWriter) *ResponseRecorder {
	rf, _ := w.(io.ReaderFrom)
	return &ResponseRecorder{
		ResponseWriter: w,
		ReaderFrom:     rf,
		StatusCode:     http.StatusOK,
		StartTime:      time.Now().UTC(),
	}
}

// ErrNotImplemented when a functionality is not implemented
var ErrNotImplemented = errors.New("not implemented")

// ReadFrom implements support for calling internal io.ReaderFrom implementations
// returns an error if the underlying ResponseWriter does not implement io.ReaderFrom
func (lrw *ResponseRecorder) ReadFrom(r io.Reader) (int64, error) {
	if lrw.ReaderFrom != nil {
		n, err := lrw.ReaderFrom.ReadFrom(r)
		lrw.bytesWritten += int(n)
		return n, err
	}
	return 0, ErrNotImplemented
}

func (lrw *ResponseRecorder) Write(p []byte) (int, error) {
	if !lrw.headersLogged {
		// We assume the response code to be '200 OK' when WriteHeader() is not called,
		// that way following Golang HTTP response behavior.
		lrw.WriteHeader(http.StatusOK)
	}
	n, err := lrw.ResponseWriter.Write(p)
	lrw.bytesWritten += n
	if lrw.ttfbBody == 0 {
		lrw.ttfbBody = time.Now().UTC().Sub(lrw.StartTime)
	}

	if (lrw.LogErrBody && lrw.StatusCode >= http.StatusBadRequest) || lrw.LogAllBody {
		// If body is > 10MB, drop it.
		if lrw.bytesWritten+len(p) > 10<<20 {
			lrw.LogAllBody = false
			lrw.body = bytes.Buffer{}
		} else {
			// Always logging error responses.
			lrw.body.Write(p)
		}
	}
	if err != nil {
		return n, err
	}
	return n, err
}

// Write the headers into the given buffer
func (lrw *ResponseRecorder) writeHeaders(w io.Writer, statusCode int, headers http.Header) {
	n, _ := fmt.Fprintf(w, "%d %s\n", statusCode, http.StatusText(statusCode))
	lrw.headerBytesWritten += n
	for k, v := range headers {
		n, _ := fmt.Fprintf(w, "%s: %s\n", k, v[0])
		lrw.headerBytesWritten += n
	}
}

// blobBody returns a dummy body placeholder for blob (binary stream)
var blobBody = []byte("<BLOB>")

// gzippedBody returns a dummy body placeholder for gzipped content
var gzippedBody = []byte("<GZIP>")

// Body - Return response body.
func (lrw *ResponseRecorder) Body() []byte {
	if lrw.Header().Get("Content-Encoding") == "gzip" {
		if lrw.body.Len() > 1<<20 {
			return gzippedBody
		}
		r, err := gzip.NewReader(&lrw.body)
		if err != nil {
			return gzippedBody
		}
		defer r.Close()
		b, _ := io.ReadAll(io.LimitReader(r, 10<<20))
		return b
	}
	// If there was an error response or body logging is enabled
	// then we return the body contents
	if (lrw.LogErrBody && lrw.StatusCode >= http.StatusBadRequest) || lrw.LogAllBody {
		return lrw.body.Bytes()
	}
	// ... otherwise we return the <BLOB> place holder
	return blobBody
}

// WriteHeader - writes http status code
func (lrw *ResponseRecorder) WriteHeader(code int) {
	if !lrw.headersLogged {
		lrw.ttfbHeader = time.Now().UTC().Sub(lrw.StartTime)
		lrw.StatusCode = code
		lrw.writeHeaders(&lrw.headers, code, lrw.Header())
		lrw.headersLogged = true
		lrw.ResponseWriter.WriteHeader(code)
	}
}

// Flush - Calls the underlying Flush.
func (lrw *ResponseRecorder) Flush() {
	if flusher, ok := lrw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Size - returns  the number of bytes written
func (lrw *ResponseRecorder) Size() int {
	return lrw.bytesWritten
}

// HeaderSize - returns the number of bytes of response headers written
func (lrw *ResponseRecorder) HeaderSize() int {
	return lrw.headerBytesWritten
}
