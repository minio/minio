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
	"sync/atomic"
)

// RequestStats - counts for Get and Head requests
type RequestStats struct {
	Get  uint64 `json:"Get"`
	Head uint64 `json:"Head"`
	Put  uint64 `json:"Put"`
	Post uint64 `json:"Post"`
}

// IncBytesReceived - Increase total bytes received from gateway backend
func (s *BackendMetrics) IncBytesReceived(n uint64) {
	atomic.AddUint64(&s.bytesReceived, n)
}

// GetBytesReceived - Get total bytes received from gateway backend
func (s *BackendMetrics) GetBytesReceived() uint64 {
	return atomic.LoadUint64(&s.bytesReceived)
}

// IncBytesSent - Increase total bytes sent to gateway backend
func (s *BackendMetrics) IncBytesSent(n uint64) {
	atomic.AddUint64(&s.bytesSent, n)
}

// GetBytesSent - Get total bytes received from gateway backend
func (s *BackendMetrics) GetBytesSent() uint64 {
	return atomic.LoadUint64(&s.bytesSent)
}

// IncRequests - Increase request count sent to gateway backend by 1
func (s *BackendMetrics) IncRequests(method string) {
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
func (s *BackendMetrics) GetRequests() RequestStats {
	return s.requestStats
}

// NewMetrics - Prepare new BackendMetrics structure
func NewMetrics() *BackendMetrics {
	return &BackendMetrics{}
}
