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
	"sync/atomic"
)

// counter - simplify atomic counting
type counter struct {
	val uint64
}

// Inc increases counter atomically
func (c *counter) Inc(n uint64) {
	atomic.AddUint64(&c.val, n)
}

// Value fetches counter's value atomically
func (c *counter) Value() uint64 {
	return atomic.LoadUint64(&c.val)
}

// ConnStats - Network statistics
// Count total input/output transferred bytes during
// the server's life.
type ConnStats struct {
	totalInputBytes  counter
	totalOutputBytes counter
}

// Increase total input bytes
func (s *ConnStats) incInputBytes(n int) {
	s.totalInputBytes.Inc(uint64(n))
}

// Increase total output bytes
func (s *ConnStats) incOutputBytes(n int) {
	s.totalOutputBytes.Inc(uint64(n))
}

// Return total input bytes
func (s *ConnStats) getTotalInputBytes() uint64 {
	return s.totalInputBytes.Value()
}

// Return total output bytes
func (s *ConnStats) getTotalOutputBytes() uint64 {
	return s.totalOutputBytes.Value()
}

// Prepare new ConnStats structure
func newConnStats() *ConnStats {
	return &ConnStats{}
}

// httpStats holds statistics information about
// HTTP requests made by all clients
type httpStats struct {
	// HEAD request stats
	totalHEADs   counter
	successHEADs counter
	// GET request stats
	totalGETs   counter
	successGETs counter
	// PUT request
	totalPUTs   counter
	successPUTs counter
	// POST request
	totalPOSTs   counter
	successPOSTs counter
	// DELETE request
	totalDELETEs   counter
	successDELETEs counter
}

// Update statistics from http request and response data
func (st *httpStats) updateStats(r *http.Request, w *httpResponseRecorder) {
	// A successful request has a 2xx response code
	successReq := (w.respStatusCode >= 200 && w.respStatusCode < 300)
	// Update stats according to method verb
	switch r.Method {
	case "HEAD":
		st.totalHEADs.Inc(1)
		if successReq {
			st.successHEADs.Inc(1)
		}
	case "GET":
		st.totalGETs.Inc(1)
		if successReq {
			st.successGETs.Inc(1)
		}
	case "PUT":
		st.totalPUTs.Inc(1)
		if successReq {
			st.successPUTs.Inc(1)
		}
	case "POST":
		st.totalPOSTs.Inc(1)
		if successReq {
			st.successPOSTs.Inc(1)
		}
	case "DELETE":
		st.totalDELETEs.Inc(1)
		if successReq {
			st.successDELETEs.Inc(1)
		}
	}
}

// Prepare new HttpStats structure
func newHTTPStats() *httpStats {
	return &httpStats{}
}
