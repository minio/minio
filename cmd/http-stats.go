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

	xhttp "github.com/minio/minio/internal/http"
	"github.com/prometheus/client_golang/prometheus"
)

// connStats - Network statistics
// Count total input/output transferred bytes during
// the server's life.
type connStats struct {
	internodeInputBytes  uint64
	internodeOutputBytes uint64
	s3InputBytes         uint64
	s3OutputBytes        uint64
}

// Increase internode total input bytes
func (s *connStats) incInternodeInputBytes(n int64) {
	atomic.AddUint64(&s.internodeInputBytes, uint64(n))
}

// Increase internode total output bytes
func (s *connStats) incInternodeOutputBytes(n int64) {
	atomic.AddUint64(&s.internodeOutputBytes, uint64(n))
}

// Return internode total input bytes
func (s *connStats) getInternodeInputBytes() uint64 {
	return atomic.LoadUint64(&s.internodeInputBytes)
}

// Return total output bytes
func (s *connStats) getInternodeOutputBytes() uint64 {
	return atomic.LoadUint64(&s.internodeOutputBytes)
}

// Increase S3 total input bytes
func (s *connStats) incS3InputBytes(n int64) {
	atomic.AddUint64(&s.s3InputBytes, uint64(n))
}

// Increase S3 total output bytes
func (s *connStats) incS3OutputBytes(n int64) {
	atomic.AddUint64(&s.s3OutputBytes, uint64(n))
}

// Return S3 total input bytes
func (s *connStats) getS3InputBytes() uint64 {
	return atomic.LoadUint64(&s.s3InputBytes)
}

// Return S3 total output bytes
func (s *connStats) getS3OutputBytes() uint64 {
	return atomic.LoadUint64(&s.s3OutputBytes)
}

// Return connection stats (total input/output bytes and total s3 input/output bytes)
func (s *connStats) toServerConnStats() serverConnStats {
	return serverConnStats{
		internodeInputBytes:  s.getInternodeInputBytes(),  // Traffic internode received
		internodeOutputBytes: s.getInternodeOutputBytes(), // Traffic internode sent
		s3InputBytes:         s.getS3InputBytes(),         // Traffic S3 received
		s3OutputBytes:        s.getS3OutputBytes(),        // Traffic S3 sent
	}
}

// Prepare new ConnStats structure
func newConnStats() *connStats {
	return &connStats{}
}

type bucketS3RXTX struct {
	s3InputBytes  uint64
	s3OutputBytes uint64
}

type bucketHTTPAPIStats struct {
	currentS3Requests *HTTPAPIStats
	totalS3Requests   *HTTPAPIStats
	totalS34xxErrors  *HTTPAPIStats
	totalS35xxErrors  *HTTPAPIStats
	totalS3Canceled   *HTTPAPIStats
}

type bucketHTTPStats struct {
	sync.RWMutex
	httpStats map[string]bucketHTTPAPIStats
}

func newBucketHTTPStats() *bucketHTTPStats {
	return &bucketHTTPStats{
		httpStats: make(map[string]bucketHTTPAPIStats),
	}
}

func (bh *bucketHTTPStats) delete(bucket string) {
	bh.Lock()
	defer bh.Unlock()

	delete(bh.httpStats, bucket)
}

func (bh *bucketHTTPStats) updateHTTPStats(bucket, api string, w *xhttp.ResponseRecorder) {
	if bh == nil {
		return
	}

	if w != nil {
		// Increment the prometheus http request response histogram with API, Bucket
		bucketHTTPRequestsDuration.With(prometheus.Labels{
			"api":    api,
			"bucket": bucket,
		}).Observe(w.TTFB().Seconds())
	}

	bh.Lock()
	defer bh.Unlock()

	hstats, ok := bh.httpStats[bucket]
	if !ok {
		hstats = bucketHTTPAPIStats{
			currentS3Requests: &HTTPAPIStats{},
			totalS3Requests:   &HTTPAPIStats{},
			totalS3Canceled:   &HTTPAPIStats{},
			totalS34xxErrors:  &HTTPAPIStats{},
			totalS35xxErrors:  &HTTPAPIStats{},
		}
	}

	if w == nil { // when response recorder nil, this is an active request
		hstats.currentS3Requests.Inc(api)
		bh.httpStats[bucket] = hstats
		return
	} // else {
	hstats.currentS3Requests.Dec(api) // decrement this once we have the response recorder.

	hstats.totalS3Requests.Inc(api)
	code := w.StatusCode

	switch {
	case code == 0:
	case code == 499:
		// 499 is a good error, shall be counted as canceled.
		hstats.totalS3Canceled.Inc(api)
	case code >= http.StatusBadRequest:
		if code >= http.StatusInternalServerError {
			hstats.totalS35xxErrors.Inc(api)
		} else {
			hstats.totalS34xxErrors.Inc(api)
		}
	}

	bh.httpStats[bucket] = hstats
}

func (bh *bucketHTTPStats) load(bucket string) bucketHTTPAPIStats {
	if bh == nil {
		return bucketHTTPAPIStats{
			currentS3Requests: &HTTPAPIStats{},
			totalS3Requests:   &HTTPAPIStats{},
			totalS3Canceled:   &HTTPAPIStats{},
			totalS34xxErrors:  &HTTPAPIStats{},
			totalS35xxErrors:  &HTTPAPIStats{},
		}
	}

	bh.RLock()
	defer bh.RUnlock()

	val, ok := bh.httpStats[bucket]
	if ok {
		return val
	}

	return bucketHTTPAPIStats{
		currentS3Requests: &HTTPAPIStats{},
		totalS3Requests:   &HTTPAPIStats{},
		totalS3Canceled:   &HTTPAPIStats{},
		totalS34xxErrors:  &HTTPAPIStats{},
		totalS35xxErrors:  &HTTPAPIStats{},
	}
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

type inOutBytes struct {
	In  uint64
	Out uint64
}

// Return S3 total input bytes for input bucket
func (s *bucketConnStats) getS3InOutBytes() map[string]inOutBytes {
	s.RLock()
	defer s.RUnlock()

	if len(s.stats) == 0 {
		return nil
	}

	bucketStats := make(map[string]inOutBytes, len(s.stats))
	for k, v := range s.stats {
		bucketStats[k] = inOutBytes{
			In:  v.s3InputBytes,
			Out: v.s3OutputBytes,
		}
	}
	return bucketStats
}

// Return S3 total input/output bytes for each
func (s *bucketConnStats) getBucketS3InOutBytes(buckets []string) map[string]inOutBytes {
	s.RLock()
	defer s.RUnlock()

	if len(s.stats) == 0 || len(buckets) == 0 {
		return nil
	}

	bucketStats := make(map[string]inOutBytes, len(buckets))
	for _, bucket := range buckets {
		if stats, ok := s.stats[bucket]; ok {
			bucketStats[bucket] = inOutBytes{
				In:  stats.s3InputBytes,
				Out: stats.s3OutputBytes,
			}
		}
	}

	return bucketStats
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

// Get returns the current counter on input API string
func (stats *HTTPAPIStats) Get(api string) int {
	if stats == nil {
		return 0
	}

	stats.RLock()
	defer stats.RUnlock()

	val, ok := stats.apiStats[api]
	if ok {
		return val
	}

	return 0
}

// Load returns the recorded stats.
func (stats *HTTPAPIStats) Load(toLower bool) map[string]int {
	if stats == nil {
		return map[string]int{}
	}

	stats.RLock()
	defer stats.RUnlock()

	apiStats := make(map[string]int, len(stats.apiStats))
	for k, v := range stats.apiStats {
		if toLower {
			k = strings.ToLower(k)
		}
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

func (st *HTTPStats) loadRequestsInQueue() int32 {
	return atomic.LoadInt32(&st.s3RequestsInQueue)
}

func (st *HTTPStats) addRequestsInQueue(i int32) {
	atomic.AddInt32(&st.s3RequestsInQueue, i)
}

func (st *HTTPStats) incS3RequestsIncoming() {
	// Golang automatically resets to zero if this overflows
	atomic.AddUint64(&st.s3RequestsIncoming, 1)
}

// Converts http stats into struct to be sent back to the client.
func (st *HTTPStats) toServerHTTPStats(toLowerKeys bool) ServerHTTPStats {
	serverStats := ServerHTTPStats{}
	serverStats.S3RequestsIncoming = atomic.SwapUint64(&st.s3RequestsIncoming, 0)
	serverStats.S3RequestsInQueue = atomic.LoadInt32(&st.s3RequestsInQueue)
	serverStats.TotalS3RejectedAuth = atomic.LoadUint64(&st.rejectedRequestsAuth)
	serverStats.TotalS3RejectedTime = atomic.LoadUint64(&st.rejectedRequestsTime)
	serverStats.TotalS3RejectedHeader = atomic.LoadUint64(&st.rejectedRequestsHeader)
	serverStats.TotalS3RejectedInvalid = atomic.LoadUint64(&st.rejectedRequestsInvalid)
	serverStats.CurrentS3Requests = ServerHTTPAPIStats{
		APIStats: st.currentS3Requests.Load(toLowerKeys),
	}
	serverStats.TotalS3Requests = ServerHTTPAPIStats{
		APIStats: st.totalS3Requests.Load(toLowerKeys),
	}
	serverStats.TotalS3Errors = ServerHTTPAPIStats{
		APIStats: st.totalS3Errors.Load(toLowerKeys),
	}
	serverStats.TotalS34xxErrors = ServerHTTPAPIStats{
		APIStats: st.totalS34xxErrors.Load(toLowerKeys),
	}
	serverStats.TotalS35xxErrors = ServerHTTPAPIStats{
		APIStats: st.totalS35xxErrors.Load(toLowerKeys),
	}
	serverStats.TotalS3Canceled = ServerHTTPAPIStats{
		APIStats: st.totalS3Canceled.Load(toLowerKeys),
	}
	return serverStats
}

// Update statistics from http request and response data
func (st *HTTPStats) updateStats(api string, w *xhttp.ResponseRecorder) {
	st.totalS3Requests.Inc(api)

	// Increment the prometheus http request response histogram with appropriate label
	httpRequestsDuration.With(prometheus.Labels{"api": api}).Observe(w.TTFB().Seconds())

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
