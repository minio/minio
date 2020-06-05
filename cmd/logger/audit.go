/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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

package logger

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger/message/audit"
)

// ResponseWriter - is a wrapper to trap the http response status code.
type ResponseWriter struct {
	http.ResponseWriter
	StatusCode int
	// Log body of 4xx or 5xx responses
	LogErrBody bool
	// Log body of all responses
	LogAllBody bool

	TimeToFirstByte time.Duration
	StartTime       time.Time
	// number of bytes written
	bytesWritten int
	// Internal recording buffer
	headers bytes.Buffer
	body    bytes.Buffer
	// Indicate if headers are written in the log
	headersLogged bool
}

// NewResponseWriter - returns a wrapped response writer to trap
// http status codes for auditiing purposes.
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		StatusCode:     http.StatusOK,
		StartTime:      time.Now().UTC(),
	}
}

func (lrw *ResponseWriter) Write(p []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(p)
	lrw.bytesWritten += n
	if lrw.TimeToFirstByte == 0 {
		lrw.TimeToFirstByte = time.Now().UTC().Sub(lrw.StartTime)
	}
	if !lrw.headersLogged {
		// We assume the response code to be '200 OK' when WriteHeader() is not called,
		// that way following Golang HTTP response behavior.
		lrw.writeHeaders(&lrw.headers, http.StatusOK, lrw.Header())
		lrw.headersLogged = true
	}
	if (lrw.LogErrBody && lrw.StatusCode >= http.StatusBadRequest) || lrw.LogAllBody {
		// Always logging error responses.
		lrw.body.Write(p)
	}
	if err != nil {
		return n, err
	}
	return n, err
}

// Write the headers into the given buffer
func (lrw *ResponseWriter) writeHeaders(w io.Writer, statusCode int, headers http.Header) {
	n, _ := fmt.Fprintf(w, "%d %s\n", statusCode, http.StatusText(statusCode))
	lrw.bytesWritten += n
	for k, v := range headers {
		n, _ := fmt.Fprintf(w, "%s: %s\n", k, v[0])
		lrw.bytesWritten += n
	}
}

// BodyPlaceHolder returns a dummy body placeholder
var BodyPlaceHolder = []byte("<BODY>")

// Body - Return response body.
func (lrw *ResponseWriter) Body() []byte {
	// If there was an error response or body logging is enabled
	// then we return the body contents
	if (lrw.LogErrBody && lrw.StatusCode >= http.StatusBadRequest) || lrw.LogAllBody {
		return lrw.body.Bytes()
	}
	// ... otherwise we return the <BODY> place holder
	return BodyPlaceHolder
}

// WriteHeader - writes http status code
func (lrw *ResponseWriter) WriteHeader(code int) {
	lrw.StatusCode = code
	if !lrw.headersLogged {
		lrw.writeHeaders(&lrw.headers, code, lrw.ResponseWriter.Header())
		lrw.headersLogged = true
	}
	lrw.ResponseWriter.WriteHeader(code)
}

// Flush - Calls the underlying Flush.
func (lrw *ResponseWriter) Flush() {
	lrw.ResponseWriter.(http.Flusher).Flush()
}

// Size - reutrns the number of bytes written
func (lrw *ResponseWriter) Size() int {
	return lrw.bytesWritten
}

// AuditTargets is the list of enabled audit loggers
var AuditTargets = []Target{}

// AddAuditTarget adds a new audit logger target to the
// list of enabled loggers
func AddAuditTarget(t Target) {
	AuditTargets = append(AuditTargets, t)
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(w http.ResponseWriter, r *http.Request, api string, reqClaims map[string]interface{}, filterKeys ...string) {
	// Fast exit if there is not audit target configured
	if len(AuditTargets) == 0 {
		return
	}

	var (
		statusCode      int
		timeToResponse  time.Duration
		timeToFirstByte time.Duration
	)

	st, ok := w.(*ResponseWriter)
	if ok {
		statusCode = st.StatusCode
		timeToResponse = time.Now().UTC().Sub(st.StartTime)
		timeToFirstByte = st.TimeToFirstByte
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := url.PathUnescape(vars["object"])
	if err != nil {
		object = vars["object"]
	}

	entry := audit.ToEntry(w, r, reqClaims, globalDeploymentID)
	for _, filterKey := range filterKeys {
		delete(entry.ReqClaims, filterKey)
		delete(entry.ReqQuery, filterKey)
		delete(entry.ReqHeader, filterKey)
		delete(entry.RespHeader, filterKey)
	}
	entry.API.Name = api
	entry.API.Bucket = bucket
	entry.API.Object = object
	entry.API.Status = http.StatusText(statusCode)
	entry.API.StatusCode = statusCode
	entry.API.TimeToResponse = strconv.FormatInt(timeToResponse.Nanoseconds(), 10) + "ns"
	// ttfb will be recorded only for GET requests, Ignore such cases where ttfb will be empty.
	if timeToFirstByte != 0 {
		entry.API.TimeToFirstByte = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10) + "ns"
	}

	// Send audit logs only to http targets.
	for _, t := range AuditTargets {
		_ = t.Send(entry, string(All))
	}
}
