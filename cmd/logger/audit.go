/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger/message/audit"
)

// ResponseWriter - is a wrapper to trap the http response status code.
type ResponseWriter struct {
	http.ResponseWriter
	statusCode      int
	startTime       time.Time
	timeToFirstByte time.Duration
}

// NewResponseWriter - returns a wrapped response writer to trap
// http status codes for auditiing purposes.
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		startTime:      time.Now().UTC(),
	}
}

func (lrw *ResponseWriter) Write(p []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(p)
	if err != nil {
		return n, err
	}
	if lrw.timeToFirstByte == 0 {
		lrw.timeToFirstByte = time.Now().UTC().Sub(lrw.startTime)
	}
	return n, err
}

// WriteHeader - writes http status code
func (lrw *ResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Flush - Calls the underlying Flush.
func (lrw *ResponseWriter) Flush() {
	lrw.ResponseWriter.(http.Flusher).Flush()
}

// AuditTargets is the list of enabled audit loggers
var AuditTargets = []Target{}

// AddAuditTarget adds a new audit logger target to the
// list of enabled loggers
func AddAuditTarget(t Target) {
	AuditTargets = append(AuditTargets, t)
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(w http.ResponseWriter, r *http.Request, api string, reqClaims map[string]interface{}) {
	var statusCode int
	var timeToResponse time.Duration
	var timeToFirstByte time.Duration
	lrw, ok := w.(*ResponseWriter)
	if ok {
		statusCode = lrw.statusCode
		timeToResponse = time.Now().UTC().Sub(lrw.startTime)
		timeToFirstByte = lrw.timeToFirstByte
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Send audit logs only to http targets.
	for _, t := range AuditTargets {
		entry := audit.ToEntry(w, r, reqClaims, globalDeploymentID)
		entry.API.Name = api
		entry.API.Bucket = bucket
		entry.API.Object = object
		entry.API.Status = http.StatusText(statusCode)
		entry.API.StatusCode = statusCode
		entry.API.TimeToFirstByte = timeToFirstByte.String()
		entry.API.TimeToResponse = timeToResponse.String()
		_ = t.Send(entry)
	}
}
