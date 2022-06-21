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
	adminInputBytes  uint64
	adminOutputBytes uint64
}

// Increase internode total input bytes
func (s *ConnStats) incInputBytes(n int64) {
	atomic.AddUint64(&s.totalInputBytes, uint64(n))
}

// Increase internode total output bytes
func (s *ConnStats) incOutputBytes(n int64) {
	atomic.AddUint64(&s.totalOutputBytes, uint64(n))
}

// Return internode total input bytes
func (s *ConnStats) getTotalInputBytes() uint64 {
	return atomic.LoadUint64(&s.totalInputBytes)
}

// Return total output bytes
func (s *ConnStats) getTotalOutputBytes() uint64 {
	return atomic.LoadUint64(&s.totalOutputBytes)
}

// Increase S3 total input bytes
func (s *ConnStats) incS3InputBytes(n int64) {
	atomic.AddUint64(&s.s3InputBytes, uint64(n))
}

// Increase S3 total output bytes
func (s *ConnStats) incS3OutputBytes(n int64) {
	atomic.AddUint64(&s.s3OutputBytes, uint64(n))
}

// Return S3 total input bytes
func (s *ConnStats) getS3InputBytes() uint64 {
	return atomic.LoadUint64(&s.s3InputBytes)
}

// Return S3 total output bytes
func (s *ConnStats) getS3OutputBytes() uint64 {
	return atomic.LoadUint64(&s.s3OutputBytes)
}

// Increase Admin total input bytes
func (s *ConnStats) incAdminInputBytes(n int64) {
	atomic.AddUint64(&s.adminInputBytes, uint64(n))
}

// Increase Admin total output bytes
func (s *ConnStats) incAdminOutputBytes(n int64) {
	atomic.AddUint64(&s.adminOutputBytes, uint64(n))
}

// Return Admin total input bytes
func (s *ConnStats) getAdminInputBytes() uint64 {
	return atomic.LoadUint64(&s.adminInputBytes)
}

// Return Admin total output bytes
func (s *ConnStats) getAdminOutputBytes() uint64 {
	return atomic.LoadUint64(&s.adminOutputBytes)
}

// Return connection stats (total input/output bytes and total s3 input/output bytes)
func (s *ConnStats) toServerConnStats() ServerConnStats {
	return ServerConnStats{
		TotalInputBytes:  s.getTotalInputBytes(),  // Traffic internode received
		TotalOutputBytes: s.getTotalOutputBytes(), // Traffic internode sent
		S3InputBytes:     s.getS3InputBytes(),     // Traffic S3 received
		S3OutputBytes:    s.getS3OutputBytes(),    // Traffic S3 sent
		AdminInputBytes:  s.getAdminInputBytes(),  // Traffic admin calls received
		AdminOutputBytes: s.getAdminOutputBytes(), // Traffic admin calls sent
	}
}

// Prepare new ConnStats structure
func newConnStats() *ConnStats {
	return &ConnStats{}
}

type bucketS3RXTX struct {
	s3InputBytes  uint64
	s3OutputBytes uint64
}

type bucketConnStats struct {
	sync.RWMutex
	stats map[string]*bucketS3RXTX
}

func newBucketConnStats() *bucketConnStats {
	return &bucketConnStats{
		stats: make(map[string]*bucketS3RXTX),
	}
}

// Increase S3 total input bytes for input bucket
func (s *bucketConnStats) incS3InputBytes(bucket string, n int64) {
	s.Lock()
	defer s.Unlock()
	stats, ok := s.stats[bucket]
	if !ok {
		stats = &bucketS3RXTX{
			s3InputBytes: uint64(n),
		}
	} else {
		stats.s3InputBytes += uint64(n)
	}
	s.stats[bucket] = stats
}

// Increase S3 total output bytes for input bucket
func (s *bucketConnStats) incS3OutputBytes(bucket string, n int64) {
	s.Lock()
	defer s.Unlock()
	stats, ok := s.stats[bucket]
	if !ok {
		stats = &bucketS3RXTX{
			s3OutputBytes: uint64(n),
		}
	} else {
		stats.s3OutputBytes += uint64(n)
	}
	s.stats[bucket] = stats
}

// Return S3 total input bytes for input bucket
func (s *bucketConnStats) getS3InputBytes(bucket string) uint64 {
	s.RLock()
	defer s.RUnlock()

	stats := s.stats[bucket]
	if stats == nil {
		return 0
	}
	return stats.s3InputBytes
}

// Return S3 total output bytes
func (s *bucketConnStats) getS3OutputBytes(bucket string) uint64 {
	s.RLock()
	defer s.RUnlock()

	stats := s.stats[bucket]
	if stats == nil {
		return 0
	}
	return stats.s3OutputBytes
}

// delete metrics once bucket is deleted.
func (s *bucketConnStats) delete(bucket string) {
	s.Lock()
	defer s.Unlock()

	delete(s.stats, bucket)
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
	apiStats := make(map[string]int, len(stats.apiStats))
	for k, v := range stats.apiStats {
		apiStats[k] = v
	}
	return apiStats
}

// HTTPStats holds statistics information about
// HTTP requests made by all clients
type HTTPStats struct {
	s3RequestsInQueue       int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	_                       int32 // For 64 bits alignment
	s3RequestsIncoming      uint64
	rejectedRequestsAuth    uint64
	rejectedRequestsTime    uint64
	rejectedRequestsHeader  uint64
	rejectedRequestsInvalid uint64
	currentS3Requests       HTTPAPIStats
	totalS3Requests         HTTPAPIStats
	totalS3Errors           HTTPAPIStats
	totalS34xxErrors        HTTPAPIStats
	totalS35xxErrors        HTTPAPIStats
	totalS3Canceled         HTTPAPIStats
}

func (st *HTTPStats) addRequestsInQueue(i int32) {
	atomic.AddInt32(&st.s3RequestsInQueue, i)
}

func (st *HTTPStats) incS3RequestsIncoming() {
	// Golang automatically resets to zero if this overflows
	atomic.AddUint64(&st.s3RequestsIncoming, 1)
}

// Converts http stats into struct to be sent back to the client.
func (st *HTTPStats) toServerHTTPStats() ServerHTTPStats {
	serverStats := ServerHTTPStats{}
	serverStats.S3RequestsIncoming = atomic.SwapUint64(&st.s3RequestsIncoming, 0)
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
	serverStats.TotalS34xxErrors = ServerHTTPAPIStats{
		APIStats: st.totalS34xxErrors.Load(),
	}
	serverStats.TotalS35xxErrors = ServerHTTPAPIStats{
		APIStats: st.totalS35xxErrors.Load(),
	}
	serverStats.TotalS3Canceled = ServerHTTPAPIStats{
		APIStats: st.totalS3Canceled.Load(),
	}
	return serverStats
}

// Update statistics from http request and response data
func (st *HTTPStats) updateStats(api string, r *http.Request, w *logger.ResponseWriter) {
	// Ignore non S3 requests
	if strings.HasSuffix(r.URL.Path, minioReservedBucketPathWithSlash) {
		return
	}

	st.totalS3Requests.Inc(api)

	// Increment the prometheus http request response histogram with appropriate label
	httpRequestsDuration.With(prometheus.Labels{"api": api}).Observe(w.TimeToFirstByte.Seconds())

	code := w.StatusCode

	switch {
	case code == 0:
	case code == 499:
		// 499 is a good error, shall be counted as canceled.
		st.totalS3Canceled.Inc(api)
	case code >= http.StatusBadRequest:
		st.totalS3Errors.Inc(api)
		if code >= http.StatusInternalServerError {
			st.totalS35xxErrors.Inc(api)
		} else {
			st.totalS34xxErrors.Inc(api)
		}
	}
}

// Prepare new HTTPStats structure
func newHTTPStats() *HTTPStats {
	return &HTTPStats{}
}
