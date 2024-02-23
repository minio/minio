// Copyright (c) 2015-2024 MinIO, Inc.
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

package cachevalue

import (
	"sync"
	"time"
)

// Cache contains a synchronized value that is considered valid
// for a specific amount of time.
// An Update function must be set to provide an updated value when needed.
type Cache[I any] struct {
	// Update must return an updated value.
	// If an error is returned the cached value is not set.
	// Only one caller will call this function at any time, others will be blocking.
	// The returned value can no longer be modified once returned.
	// Should be set before calling Get().
	Update func() (item I, err error)

	// TTL for a cached value.
	// If not set 1 second TTL is assumed.
	// Should be set before calling Get().
	TTL time.Duration

	// When set to true, return the last cached value
	// even if updating the value errors out
	Relax bool

	// Once can be used to initialize values for lazy initialization.
	// Should be set before calling Get().
	Once sync.Once

	// Managed values.
	value      I         // our cached value
	valueSet   bool      // 'true' if the value 'I' has a value
	lastUpdate time.Time // indicates when value 'I' was updated last, used for invalidation.
	mu         sync.RWMutex
}

// New initializes a new cached value instance.
func New[I any]() *Cache[I] {
	return &Cache[I]{}
}

// Get will return a cached value or fetch a new one.
// If the Update function returns an error the value is forwarded as is and not cached.
func (t *Cache[I]) Get() (item I, err error) {
	item, ok := t.get(t.ttl())
	if ok {
		return item, nil
	}

	item, err = t.Update()
	if err != nil {
		if t.Relax {
			// if update fails, return current
			// cached value along with error.
			//
			// Let the caller decide if they want
			// to use the returned value based
			// on error.
			item, ok = t.get(0)
			if ok {
				return item, err
			}
		}
		return item, err
	}

	t.update(item)
	return item, nil
}

func (t *Cache[_]) ttl() time.Duration {
	ttl := t.TTL
	if ttl <= 0 {
		ttl = time.Second
	}
	return ttl
}

func (t *Cache[I]) get(ttl time.Duration) (item I, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.valueSet {
		item = t.value
		if ttl <= 0 {
			return item, true
		}
		if time.Since(t.lastUpdate) < ttl {
			return item, true
		}
	}
	return item, false
}

func (t *Cache[I]) update(item I) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.value = item
	t.valueSet = true
	t.lastUpdate = time.Now()
}
