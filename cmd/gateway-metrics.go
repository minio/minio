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

	"go.uber.org/atomic"
)

// RequestStats - counts for Get and Head requests
type RequestStats struct {
	Get  atomic.Uint64 `json:"Get"`
	Head atomic.Uint64 `json:"Head"`
	Put  atomic.Uint64 `json:"Put"`
	Post atomic.Uint64 `json:"Post"`
}

// Metrics - represents bytes served from backend
// only implemented for S3 Gateway
type Metrics struct {
	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64
	requestStats  RequestStats
}

// IncBytesReceived - Increase total bytes received from gateway backend
func (s *Metrics) IncBytesReceived(n uint64) {
	s.bytesReceived.Add(n)
}

// GetBytesReceived - Get total bytes received from gateway backend
func (s *Metrics) GetBytesReceived() uint64 {
	return s.bytesReceived.Load()
}

// IncBytesSent - Increase total bytes sent to gateway backend
func (s *Metrics) IncBytesSent(n uint64) {
	s.bytesSent.Add(n)
}

// GetBytesSent - Get total bytes received from gateway backend
func (s *Metrics) GetBytesSent() uint64 {
	return s.bytesSent.Load()
}

// IncRequests - Increase request count sent to gateway backend by 1
func (s *Metrics) IncRequests(method string) {
	// Only increment for Head & Get requests, else no op
	if method == http.MethodGet {
		s.requestStats.Get.Add(1)
	} else if method == http.MethodHead {
		s.requestStats.Head.Add(1)
	} else if method == http.MethodPut {
		s.requestStats.Put.Add(1)
	} else if method == http.MethodPost {
		s.requestStats.Post.Add(1)
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
