// Copyright (c) 2015-2021 MinIO, Inc.
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

package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/minio/minio/internal/logger/message/audit"
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
// http status codes for auditing purposes.
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		StatusCode:     http.StatusOK,
		StartTime:      time.Now().UTC(),
	}
}

func (lrw *ResponseWriter) Write(p []byte) (int, error) {
	if !lrw.headersLogged {
		// We assume the response code to be '200 OK' when WriteHeader() is not called,
		// that way following Golang HTTP response behavior.
		lrw.WriteHeader(http.StatusOK)
	}
	n, err := lrw.ResponseWriter.Write(p)
	lrw.bytesWritten += n
	if lrw.TimeToFirstByte == 0 {
		lrw.TimeToFirstByte = time.Now().UTC().Sub(lrw.StartTime)
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
	if !lrw.headersLogged {
		lrw.StatusCode = code
		lrw.writeHeaders(&lrw.headers, code, lrw.ResponseWriter.Header())
		lrw.headersLogged = true
		lrw.ResponseWriter.WriteHeader(code)
	}
}

// Flush - Calls the underlying Flush.
func (lrw *ResponseWriter) Flush() {
	lrw.ResponseWriter.(http.Flusher).Flush()
}

// Size - reutrns the number of bytes written
func (lrw *ResponseWriter) Size() int {
	return lrw.bytesWritten
}

const contextAuditKey = contextKeyType("audit-entry")

// SetAuditEntry sets Audit info in the context.
func SetAuditEntry(ctx context.Context, audit *audit.Entry) context.Context {
	if ctx == nil {
		LogIf(context.Background(), fmt.Errorf("context is nil"))
		return nil
	}
	return context.WithValue(ctx, contextAuditKey, audit)
}

// GetAuditEntry returns Audit entry if set.
func GetAuditEntry(ctx context.Context) *audit.Entry {
	if ctx != nil {
		r, ok := ctx.Value(contextAuditKey).(*audit.Entry)
		if ok {
			return r
		}
		r = &audit.Entry{}
		SetAuditEntry(ctx, r)
		return r
	}
	return nil
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(ctx context.Context, w http.ResponseWriter, r *http.Request, reqClaims map[string]interface{}, filterKeys ...string) {
	// Fast exit if there is not audit target configured
	if len(AuditTargets) == 0 {
		return
	}

	var entry audit.Entry

	if w != nil && r != nil {
		reqInfo := GetReqInfo(ctx)
		if reqInfo == nil {
			return
		}

		entry = audit.ToEntry(w, r, reqClaims, globalDeploymentID)
		entry.Trigger = "external-request"

		for _, filterKey := range filterKeys {
			delete(entry.ReqClaims, filterKey)
			delete(entry.ReqQuery, filterKey)
			delete(entry.ReqHeader, filterKey)
			delete(entry.RespHeader, filterKey)
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

		entry.API.Name = reqInfo.API
		entry.API.Bucket = reqInfo.BucketName
		entry.API.Object = reqInfo.ObjectName
		entry.API.Status = http.StatusText(statusCode)
		entry.API.StatusCode = statusCode
		entry.API.TimeToResponse = strconv.FormatInt(timeToResponse.Nanoseconds(), 10) + "ns"
		entry.Tags = reqInfo.GetTagsMap()
		// ttfb will be recorded only for GET requests, Ignore such cases where ttfb will be empty.
		if timeToFirstByte != 0 {
			entry.API.TimeToFirstByte = strconv.FormatInt(timeToFirstByte.Nanoseconds(), 10) + "ns"
		}
	} else {
		auditEntry := GetAuditEntry(ctx)
		if auditEntry != nil {
			entry = *auditEntry
		}
	}

	// Send audit logs only to http targets.
	for _, t := range AuditTargets {
		_ = t.Send(entry, string(All))
	}
}
