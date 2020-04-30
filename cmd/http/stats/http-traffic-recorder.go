/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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

package stats

import (
	"io"
	"net/http"
	"time"
)

// IncomingTrafficMeter counts the incoming bytes from the underlying request.Body.
type IncomingTrafficMeter struct {
	io.ReadCloser
	countBytes int
}

// Read calls the underlying Read and counts the transferred bytes.
func (r *IncomingTrafficMeter) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.countBytes += n
	return n, err
}

// BytesCount returns the number of transferred bytes
func (r IncomingTrafficMeter) BytesCount() int {
	return r.countBytes
}

// OutgoingTrafficMeter counts the outgoing bytes through the responseWriter.
type OutgoingTrafficMeter struct {
	// wrapper for underlying http.ResponseWriter.
	http.ResponseWriter
	countBytes int
}

// Write calls the underlying write and counts the output bytes
func (w *OutgoingTrafficMeter) Write(p []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(p)
	w.countBytes += n
	return n, err
}

// Flush calls the underlying Flush.
func (w *OutgoingTrafficMeter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

// BytesCount returns the number of transferred bytes
func (w OutgoingTrafficMeter) BytesCount() int {
	return w.countBytes
}

// RecordAPIStats is a response writer which stores
// information of the underlying http response.
type RecordAPIStats struct {
	http.ResponseWriter
	TTFB           time.Duration // TimeToFirstByte.
	StartTime      time.Time
	RespStatusCode int

	firstByteRead bool
}

// NewRecordAPIStats creates a new response writer with
// start time set to the function call time.
func NewRecordAPIStats(w http.ResponseWriter) *RecordAPIStats {
	return &RecordAPIStats{
		ResponseWriter: w,
		StartTime:      time.Now().UTC(),
	}
}

// WriteHeader calls the underlying WriteHeader
// and records the response status code.
func (r *RecordAPIStats) WriteHeader(i int) {
	r.RespStatusCode = i
	r.ResponseWriter.WriteHeader(i)
}

// Write calls the underlying Write and updates TTFB and other info
func (r *RecordAPIStats) Write(p []byte) (n int, err error) {
	if !r.firstByteRead {
		r.TTFB = time.Now().UTC().Sub(r.StartTime)
		r.firstByteRead = true
	}
	n, err = r.ResponseWriter.Write(p)
	return
}

// Flush calls the underlying Flush.
func (r *RecordAPIStats) Flush() {
	r.ResponseWriter.(http.Flusher).Flush()
}
