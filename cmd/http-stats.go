/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/cmd/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// ConnStats - Network statistics
// Count total input/output transferred bytes during
// the server's life.
type ConnStats struct {
	totalInputBytes  uint64
	totalOutputBytes uint64
	s3InputBytes     uint64
	s3OutputBytes    uint64
}

// Increase total input bytes
func (s *ConnStats) incInputBytes(n int) {
	atomic.AddUint64(&s.totalInputBytes, uint64(n))
}

// Increase total output bytes
func (s *ConnStats) incOutputBytes(n int) {
	atomic.AddUint64(&s.totalOutputBytes, uint64(n))
}

// Return total input bytes
func (s *ConnStats) getTotalInputBytes() uint64 {
	return atomic.LoadUint64(&s.totalInputBytes)
}

// Return total output bytes
func (s *ConnStats) getTotalOutputBytes() uint64 {
	return atomic.LoadUint64(&s.totalOutputBytes)
}

// Increase outbound input bytes
func (s *ConnStats) incS3InputBytes(n int) {
	atomic.AddUint64(&s.s3InputBytes, uint64(n))
}

// Increase outbound output bytes
func (s *ConnStats) incS3OutputBytes(n int) {
	atomic.AddUint64(&s.s3OutputBytes, uint64(n))
}

// Return outbound input bytes
func (s *ConnStats) getS3InputBytes() uint64 {
	return atomic.LoadUint64(&s.s3InputBytes)
}

// Return outbound output bytes
func (s *ConnStats) getS3OutputBytes() uint64 {
	return atomic.LoadUint64(&s.s3OutputBytes)
}

// Return connection stats (total input/output bytes and total s3 input/output bytes)
func (s *ConnStats) toServerConnStats() ServerConnStats {
	return ServerConnStats{
		TotalInputBytes:  s.getTotalInputBytes(),
		TotalOutputBytes: s.getTotalOutputBytes(),
		S3InputBytes:     s.getS3InputBytes(),
		S3OutputBytes:    s.getS3OutputBytes(),
	}
}

// Prepare new ConnStats structure
func newConnStats() *ConnStats {
	return &ConnStats{}
}

// HTTPAPIStats holds statistics information about
// a given API in the requests.
type HTTPAPIStats struct {
	apiStats map[string]int
	sync.RWMutex
}

// Inc increments the api stats counter.
func (stats *HTTPAPIStats) Inc(api string) {
	if stats == nil {
		return
	}
	stats.Lock()
	defer stats.Unlock()
	if stats.apiStats == nil {
		stats.apiStats = make(map[string]int)
	}
	stats.apiStats[api]++
}

// Dec increments the api stats counter.
func (stats *HTTPAPIStats) Dec(api string) {
	if stats == nil {
		return
	}
	stats.Lock()
	defer stats.Unlock()
	if val, ok := stats.apiStats[api]; ok && val > 0 {
		stats.apiStats[api]--
	}
}

// Load returns the recorded stats.
func (stats *HTTPAPIStats) Load() map[string]int {
	stats.Lock()
	defer stats.Unlock()
	var apiStats = make(map[string]int, len(stats.apiStats))
	for k, v := range stats.apiStats {
		apiStats[k] = v
	}
	return apiStats
}

// HTTPStats holds statistics information about
// HTTP requests made by all clients
type HTTPStats struct {
	currentS3Requests HTTPAPIStats
	totalS3Requests   HTTPAPIStats
	totalS3Errors     HTTPAPIStats
}

// Converts http stats into struct to be sent back to the client.
func (st *HTTPStats) toServerHTTPStats() ServerHTTPStats {
	serverStats := ServerHTTPStats{}

	serverStats.CurrentS3Requests = ServerHTTPAPIStats{
		APIStats: st.currentS3Requests.Load(),
	}

	serverStats.TotalS3Requests = ServerHTTPAPIStats{
		APIStats: st.totalS3Requests.Load(),
	}

	serverStats.TotalS3Errors = ServerHTTPAPIStats{
		APIStats: st.totalS3Errors.Load(),
	}
	return serverStats
}

// Update statistics from http request and response data
func (st *HTTPStats) updateStats(api string, r *http.Request, w *logger.ResponseWriter, durationSecs float64) {
	// A successful request has a 2xx response code
	successReq := (w.StatusCode >= 200 && w.StatusCode < 300)

	if !strings.HasSuffix(r.URL.Path, prometheusMetricsPath) {
		st.totalS3Requests.Inc(api)
		if !successReq && w.StatusCode != 0 {
			st.totalS3Errors.Inc(api)
		}
	}

	if r.Method == http.MethodGet {
		// Increment the prometheus http request response histogram with appropriate label
		httpRequestsDuration.With(prometheus.Labels{"api": api}).Observe(durationSecs)
	}
}

// Prepare new HTTPStats structure
func newHTTPStats() *HTTPStats {
	return &HTTPStats{}
}
