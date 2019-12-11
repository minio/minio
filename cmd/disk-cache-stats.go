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
	"go.uber.org/atomic"
)

// CacheStats - represents bytes served from cache,
// cache hits and cache misses.
type CacheStats struct {
	BytesServed atomic.Uint64
	Hits        atomic.Uint64
	Misses      atomic.Uint64
}

// Increase total bytes served from cache
func (s *CacheStats) incBytesServed(n int64) {
	s.BytesServed.Add(uint64(n))
}

// Increase cache hit by 1
func (s *CacheStats) incHit() {
	s.Hits.Add(uint64(1))
}

// Increase cache miss by 1
func (s *CacheStats) incMiss() {
	s.Misses.Add(uint64(1))
}

// Get total bytes served
func (s *CacheStats) getBytesServed() uint64 {
	return s.BytesServed.Load()
}

// Get total cache hits
func (s *CacheStats) getHits() uint64 {
	return s.Hits.Load()
}

// Get total cache misses
func (s *CacheStats) getMisses() uint64 {
	return s.Misses.Load()
}

// Prepare new CacheStats structure
func newCacheStats() *CacheStats {
	return &CacheStats{}
}
