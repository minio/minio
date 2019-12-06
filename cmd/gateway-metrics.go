/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"sync"

	"go.uber.org/atomic"
)

// Metrics - represents bytes served from backend
// only implemented for S3 Gateway
type Metrics struct {
	BytesReceived atomic.Uint64
	BytesSent     atomic.Uint64
	RequestStats  map[string]int
	sync.RWMutex
}

// IncBytesReceived - Increase total bytes received from gateway backend
func (s *Metrics) IncBytesReceived(n int64) {
	s.BytesReceived.Add(uint64(n))
}

// GetBytesReceived - Get total bytes received from gateway backend
func (s *Metrics) GetBytesReceived() uint64 {
	return s.BytesReceived.Load()
}

// IncBytesSent - Increase total bytes sent to gateway backend
func (s *Metrics) IncBytesSent(n int64) {
	s.BytesSent.Add(uint64(n))
}

// GetBytesSent - Get total bytes received from gateway backend
func (s *Metrics) GetBytesSent() uint64 {
	return s.BytesSent.Load()
}

// IncRequests - Increase request sent to gateway backend by 1
func (s *Metrics) IncRequests(method string) {
	s.Lock()
	defer s.Unlock()
	if s == nil {
		return
	}
	if s.RequestStats == nil {
		s.RequestStats = make(map[string]int)
	}
	if _, ok := s.RequestStats[method]; ok {
		s.RequestStats[method]++
		return
	}
	s.RequestStats[method] = 1
}

// GetRequests - Get total number of requests sent to gateway backend
func (s *Metrics) GetRequests() map[string]int {
	return s.RequestStats
}

// NewMetrics - Prepare new Metrics structure
func NewMetrics() *Metrics {
	return &Metrics{}
}
