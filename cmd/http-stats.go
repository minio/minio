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

package cmd

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/internal/logger"
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
		TotalInputBytes:  s.getTotalInputBytes(),  // Traffic including reserved bucket
		TotalOutputBytes: s.getTotalOutputBytes(), // Traffic including reserved bucket
		S3InputBytes:     s.getS3InputBytes(),     // Traffic for client buckets
		S3OutputBytes:    s.getS3OutputBytes(),    // Traffic for client buckets
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
	rejectedRequestsAuth    uint64
	rejectedRequestsTime    uint64
	rejectedRequestsHeader  uint64
	rejectedRequestsInvalid uint64
	s3RequestsInQueue       int32
	currentS3Requests       HTTPAPIStats
	totalS3Requests         HTTPAPIStats
	totalS3Errors           HTTPAPIStats
	totalS3Canceled         HTTPAPIStats
}

func (st *HTTPStats) addRequestsInQueue(i int32) {
	atomic.AddInt32(&st.s3RequestsInQueue, i)
}

// Converts http stats into struct to be sent back to the client.
func (st *HTTPStats) toServerHTTPStats() ServerHTTPStats {
	serverStats := ServerHTTPStats{}
	serverStats.S3RequestsInQueue = atomic.LoadInt32(&st.s3RequestsInQueue)
	serverStats.TotalS3RejectedAuth = atomic.LoadUint64(&st.rejectedRequestsAuth)
	serverStats.TotalS3RejectedTime = atomic.LoadUint64(&st.rejectedRequestsTime)
	serverStats.TotalS3RejectedHeader = atomic.LoadUint64(&st.rejectedRequestsHeader)
	serverStats.TotalS3RejectedInvalid = atomic.LoadUint64(&st.rejectedRequestsInvalid)
	serverStats.CurrentS3Requests = ServerHTTPAPIStats{
		APIStats: st.currentS3Requests.Load(),
	}
	serverStats.TotalS3Requests = ServerHTTPAPIStats{
		APIStats: st.totalS3Requests.Load(),
	}
	serverStats.TotalS3Errors = ServerHTTPAPIStats{
		APIStats: st.totalS3Errors.Load(),
	}
	serverStats.TotalS3Canceled = ServerHTTPAPIStats{
		APIStats: st.totalS3Canceled.Load(),
	}
	return serverStats
}

// Update statistics from http request and response data
func (st *HTTPStats) updateStats(api string, r *http.Request, w *logger.ResponseWriter) {
	// A successful request has a 2xx response code
	successReq := w.StatusCode >= 200 && w.StatusCode < 300

	if !strings.HasSuffix(r.URL.Path, prometheusMetricsPathLegacy) ||
		!strings.HasSuffix(r.URL.Path, prometheusMetricsV2ClusterPath) ||
		!strings.HasSuffix(r.URL.Path, prometheusMetricsV2NodePath) {
		st.totalS3Requests.Inc(api)
		if !successReq {
			switch w.StatusCode {
			case 0:
			case 499:
				// 499 is a good error, shall be counted at canceled.
				st.totalS3Canceled.Inc(api)
			default:
				st.totalS3Errors.Inc(api)
			}
		}
	}

	// Increment the prometheus http request response histogram with appropriate label
	httpRequestsDuration.With(prometheus.Labels{"api": api}).Observe(w.TimeToFirstByte.Seconds())
}

// Prepare new HTTPStats structure
func newHTTPStats() *HTTPStats {
	return &HTTPStats{}
}
