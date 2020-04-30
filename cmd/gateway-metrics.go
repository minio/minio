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
	"net/http"
	"sync/atomic"
)

// RequestStats - counts for Get and Head requests
type RequestStats struct {
	Get  uint64 `json:"Get"`
	Head uint64 `json:"Head"`
	Put  uint64 `json:"Put"`
	Post uint64 `json:"Post"`
}

// Metrics - represents bytes served from backend
// only implemented for S3 Gateway
type Metrics struct {
	bytesReceived uint64
	bytesSent     uint64
	requestStats  RequestStats
}

// IncBytesReceived - Increase total bytes received from gateway backend
func (s *Metrics) IncBytesReceived(n uint64) {
	atomic.AddUint64(&s.bytesReceived, n)
}

// GetBytesReceived - Get total bytes received from gateway backend
func (s *Metrics) GetBytesReceived() uint64 {
	return atomic.LoadUint64(&s.bytesReceived)
}

// IncBytesSent - Increase total bytes sent to gateway backend
func (s *Metrics) IncBytesSent(n uint64) {
	atomic.AddUint64(&s.bytesSent, n)
}

// GetBytesSent - Get total bytes received from gateway backend
func (s *Metrics) GetBytesSent() uint64 {
	return atomic.LoadUint64(&s.bytesSent)
}

// IncRequests - Increase request count sent to gateway backend by 1
func (s *Metrics) IncRequests(method string) {
	// Only increment for Head & Get requests, else no op
	if method == http.MethodGet {
		atomic.AddUint64(&s.requestStats.Get, 1)
	} else if method == http.MethodHead {
		atomic.AddUint64(&s.requestStats.Head, 1)
	} else if method == http.MethodPut {
		atomic.AddUint64(&s.requestStats.Put, 1)
	} else if method == http.MethodPost {
		atomic.AddUint64(&s.requestStats.Post, 1)
	}
}

// GetRequests - Get total number of Get & Headrequests sent to gateway backend
func (s *Metrics) GetRequests() RequestStats {
	return s.requestStats
}

// NewMetrics - Prepare new Metrics structure
func NewMetrics() *Metrics {
	return &Metrics{}
}
