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
	"sync/atomic"
)

// CacheDiskStats represents cache disk statistics
// such as current disk usage and available.
type CacheDiskStats struct {
	// used cache size
	UsageSize uint64
	// total cache disk capacity
	TotalCapacity uint64
	// indicates if usage is high or low, if high value is '1', if low its '0'
	UsageState int32
	// indicates the current usage percentage of this cache disk
	UsagePercent uint64
	Dir          string
}

// GetUsageLevelString gets the string representation for the usage level.
func (c *CacheDiskStats) GetUsageLevelString() (u string) {
	if atomic.LoadInt32(&c.UsageState) == 0 {
		return "low"
	}
	return "high"
}

// CacheStats - represents bytes served from cache,
// cache hits and cache misses.
type CacheStats struct {
	BytesServed  uint64
	Hits         uint64
	Misses       uint64
	GetDiskStats func() []CacheDiskStats
}

// Increase total bytes served from cache
func (s *CacheStats) incBytesServed(n int64) {
	atomic.AddUint64(&s.BytesServed, uint64(n))
}

// Increase cache hit by 1
func (s *CacheStats) incHit() {
	atomic.AddUint64(&s.Hits, 1)
}

// Increase cache miss by 1
func (s *CacheStats) incMiss() {
	atomic.AddUint64(&s.Misses, 1)
}

// Get total bytes served
func (s *CacheStats) getBytesServed() uint64 {
	return atomic.LoadUint64(&s.BytesServed)
}

// Get total cache hits
func (s *CacheStats) getHits() uint64 {
	return atomic.LoadUint64(&s.Hits)
}

// Get total cache misses
func (s *CacheStats) getMisses() uint64 {
	return atomic.LoadUint64(&s.Misses)
}

// Prepare new CacheStats structure
func newCacheStats() *CacheStats {
	return &CacheStats{}
}
