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
	"sync/atomic"
	"time"
)

// Opts contains options for the cache.
type Opts struct {
	// When set to true, return the last cached value
	// even if updating the value errors out.
	// Returns the last good value AND the error.
	ReturnLastGood bool

	// If CacheError is set, errors will be cached as well
	// and not continuously try to update.
	// Should not be combined with ReturnLastGood.
	CacheError bool

	// If NoWait is set, Get() will return the last good value,
	// if TTL has expired but 2x TTL has not yet passed,
	// but will fetch a new value in the background.
	NoWait bool
}

// Cache contains a synchronized value that is considered valid
// for a specific amount of time.
// An Update function must be set to provide an updated value when needed.
type Cache[T any] struct {
	// updateFn must return an updated value.
	// If an error is returned the cached value is not set.
	// Only one caller will call this function at any time, others will be blocking.
	// The returned value can no longer be modified once returned.
	// Should be set before calling Get().
	updateFn func() (T, error)

	// ttl for a cached value.
	ttl time.Duration

	opts Opts

	// Once can be used to initialize values for lazy initialization.
	// Should be set before calling Get().
	Once sync.Once

	// Managed values.
	valErr atomic.Pointer[struct {
		v T
		e error
	}]
	lastUpdateMs atomic.Int64
	updating     sync.Mutex
}

// New allocates a new cached value instance. Tt must be initialized with
// `.TnitOnce`.
func New[T any]() *Cache[T] {
	return &Cache[T]{}
}

// NewFromFunc allocates a new cached value instance and initializes it with an
// update function, making it ready for use.
func NewFromFunc[T any](ttl time.Duration, opts Opts, update func() (T, error)) *Cache[T] {
	return &Cache[T]{
		ttl:      ttl,
		updateFn: update,
		opts:     opts,
	}
}

// InitOnce initializes the cache with a TTL and an update function. It is
// guaranteed to be called only once.
func (t *Cache[T]) InitOnce(ttl time.Duration, opts Opts, update func() (T, error)) {
	t.Once.Do(func() {
		t.ttl = ttl
		t.updateFn = update
		t.opts = opts
	})
}

// Get will return a cached value or fetch a new one.
// Tf the Update function returns an error the value is forwarded as is and not cached.
func (t *Cache[T]) Get() (T, error) {
	v := t.valErr.Load()
	ttl := t.ttl
	vTime := t.lastUpdateMs.Load()
	tNow := time.Now().UnixMilli()
	if v != nil && tNow-vTime < ttl.Milliseconds() {
		if v.e == nil {
			return v.v, nil
		}
		if v.e != nil && t.opts.CacheError || t.opts.ReturnLastGood {
			return v.v, v.e
		}
	}

	// Fetch new value.
	if t.opts.NoWait && v != nil && tNow-vTime < ttl.Milliseconds()*2 && (v.e == nil || t.opts.CacheError) {
		if t.updating.TryLock() {
			go func() {
				defer t.updating.Unlock()
				t.update()
			}()
		}
		return v.v, v.e
	}

	// Get lock. Either we get it or we wait for it.
	t.updating.Lock()
	if time.Since(time.UnixMilli(t.lastUpdateMs.Load())) < ttl {
		// There is a new value, release lock and return it.
		v = t.valErr.Load()
		t.updating.Unlock()
		return v.v, v.e
	}
	t.update()
	v = t.valErr.Load()
	t.updating.Unlock()
	return v.v, v.e
}

func (t *Cache[T]) update() {
	val, err := t.updateFn()
	if err != nil {
		if t.opts.ReturnLastGood {
			// Keep last good value.
			v := t.valErr.Load()
			if v != nil {
				val = v.v
			}
		}
	}
	t.valErr.Store(&struct {
		v T
		e error
	}{v: val, e: err})
	t.lastUpdateMs.Store(time.Now().UnixMilli())
}
