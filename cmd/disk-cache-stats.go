/*
 * MinIO Cloud Storage, (C) 2019, 2020 MinIO, Inc.
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
	"sync/atomic"
)

// CacheDiskStats represents cache disk statistics
// such as current disk usage and available.
type CacheDiskStats struct {
	// indicates if usage is high or low, if high value is '1', if low its '0'
	UsageState int32
	// indicates the current usage percentage of this cache disk
	UsagePercent uint64
	Dir          string
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
