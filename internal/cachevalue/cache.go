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
	"context"
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
	updateFn func(ctx context.Context) (T, error)

	// ttl for a cached value.
	ttl time.Duration

	opts Opts

	// Once can be used to initialize values for lazy initialization.
	// Should be set before calling Get().
	Once sync.Once

	// Managed values.
	val          atomic.Pointer[T]
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
func NewFromFunc[T any](ttl time.Duration, opts Opts, update func(ctx context.Context) (T, error)) *Cache[T] {
	return &Cache[T]{
		ttl:      ttl,
		updateFn: update,
		opts:     opts,
	}
}

// InitOnce initializes the cache with a TTL and an update function. It is
// guaranteed to be called only once.
func (t *Cache[T]) InitOnce(ttl time.Duration, opts Opts, update func(ctx context.Context) (T, error)) {
	t.Once.Do(func() {
		t.ttl = ttl
		t.updateFn = update
		t.opts = opts
	})
}

// GetWithCtx will return a cached value or fetch a new one.
// passes a caller context, if caller context cancels nothing
// is cached.
// If the Update function returns an error the value is forwarded as is and not cached.
func (t *Cache[T]) GetWithCtx(ctx context.Context) (T, error) {
	v := t.val.Load()
	ttl := t.ttl
	vTime := t.lastUpdateMs.Load()
	tNow := time.Now().UnixMilli()
	if v != nil && tNow-vTime < ttl.Milliseconds() {
		return *v, nil
	}

	// Fetch new value asynchronously, while we do not return an error
	// if v != nil value or
	if t.opts.NoWait && v != nil && tNow-vTime < ttl.Milliseconds()*2 {
		if t.updating.TryLock() {
			go func() {
				defer t.updating.Unlock()
				t.update(context.Background())
			}()
		}
		return *v, nil
	}

	// Get lock. Either we get it or we wait for it.
	t.updating.Lock()
	defer t.updating.Unlock()

	if time.Since(time.UnixMilli(t.lastUpdateMs.Load())) < ttl {
		// There is a new value, release lock and return it.
		if v = t.val.Load(); v != nil {
			return *v, nil
		}
	}

	if err := t.update(ctx); err != nil {
		var empty T
		return empty, err
	}

	return *t.val.Load(), nil
}

// Get will return a cached value or fetch a new one.
// Tf the Update function returns an error the value is forwarded as is and not cached.
func (t *Cache[T]) Get() (T, error) {
	return t.GetWithCtx(context.Background())
}

func (t *Cache[T]) update(ctx context.Context) error {
	val, err := t.updateFn(ctx)
	if err != nil {
		if t.opts.ReturnLastGood && t.val.Load() != nil {
			// Keep last good value, so update
			// does not return an error.
			return nil
		}
		return err
	}

	t.val.Store(&val)
	t.lastUpdateMs.Store(time.Now().UnixMilli())
	return nil
}
