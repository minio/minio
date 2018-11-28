/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

	"github.com/minio/minio/cmd/logger/message/audit"
)

// ResponseWriter - is a wrapper to trap the http response status code.
type ResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

// NewResponseWriter - returns a wrapped response writer to trap
// http status codes for auditiing purposes.
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{w, http.StatusOK}
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
	lrw, ok := w.(*ResponseWriter)
	if ok {
		statusCode = lrw.statusCode
	}
	// Send audit logs only to http targets.
	for _, t := range AuditTargets {
		_ = t.Send(audit.ToEntry(w, r, api, statusCode, reqClaims))
	}
}
