/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

// Package data implements in memory caching methods for data
package data

import (
	"sync"
	"time"
)

var noExpiration = time.Duration(0)

// Cache holds the required variables to compose an in memory cache system
// which also provides expiring key mechanism and also maxSize
type Cache struct {
	// Mutex is used for handling the concurrent
	// read/write requests for cache
	sync.Mutex

	// items hold the cached objects
	items map[string][]byte

	// updatedAt holds the time that related item's updated at
	updatedAt map[string]time.Time

	// expiration is a duration for a cache key to expire
	expiration time.Duration

	// stopExpireTimer channel to quit the timer thread
	stopExpireTimer chan struct{}

	// maxSize is a total size for overall cache
	maxSize uint64

	// currentSize is a current size in memory
	currentSize uint64

	// OnExpired - callback function for eviction
	OnExpired func(a ...interface{})

	// totalExpired counter to keep track of total expirations
	totalExpired uint64
}

// Stats current cache statistics
type Stats struct {
	Bytes   uint64
	Items   uint64
	Expired uint64
}

// NewCache creates an inmemory cache
//
// maxSize is used for expiring objects before we run out of memory
// expiration is used for expiration of a key from cache
func NewCache(maxSize uint64, expiration time.Duration) *Cache {
	return &Cache{
		items:      make(map[string][]byte),
		updatedAt:  map[string]time.Time{},
		expiration: expiration,
		maxSize:    maxSize,
	}
}

// Stats get current cache statistics
func (r *Cache) Stats() Stats {
	return Stats{
		Bytes:   r.currentSize,
		Items:   uint64(len(r.items)),
		Expired: r.totalExpired,
	}
}

// ExpireObjects expire objects in go routine
func (r *Cache) ExpireObjects(gcInterval time.Duration) {
	r.stopExpireTimer = make(chan struct{})
	ticker := time.NewTicker(gcInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				r.Expire()
			case <-r.stopExpireTimer:
				ticker.Stop()
				return
			}

		}
	}()
}

// Get returns a value of a given key if it exists
func (r *Cache) Get(key string) ([]byte, bool) {
	r.Lock()
	defer r.Unlock()
	value, ok := r.items[key]
	if !ok {
		return nil, false
	}
	r.updatedAt[key] = time.Now()
	return value, true
}

// Len returns length of the value of a given key, returns zero if key doesn't exist
func (r *Cache) Len(key string) int {
	r.Lock()
	defer r.Unlock()
	_, ok := r.items[key]
	if !ok {
		return 0
	}
	return len(r.items[key])
}

// Append will append new data to an existing key,
// if key doesn't exist it behaves like Set()
func (r *Cache) Append(key string, value []byte) bool {
	r.Lock()
	defer r.Unlock()
	valueLen := uint64(len(value))
	if r.maxSize > 0 {
		// check if the size of the object is not bigger than the
		// capacity of the cache
		if valueLen > r.maxSize {
			return false
		}
		// remove random key if only we reach the maxSize threshold
		for (r.currentSize + valueLen) > r.maxSize {
			for randomKey := range r.items {
				r.doDelete(randomKey)
				break
			}
		}
	}
	_, ok := r.items[key]
	if !ok {
		r.items[key] = value
		r.currentSize += valueLen
		r.updatedAt[key] = time.Now()
		return true
	}
	r.items[key] = append(r.items[key], value...)
	r.currentSize += valueLen
	r.updatedAt[key] = time.Now()
	return true
}

// Set will persist a value to the cache
func (r *Cache) Set(key string, value []byte) bool {
	r.Lock()
	defer r.Unlock()
	valueLen := uint64(len(value))
	if r.maxSize > 0 {
		// check if the size of the object is not bigger than the
		// capacity of the cache
		if valueLen > r.maxSize {
			return false
		}
		// remove random key if only we reach the maxSize threshold
		for (r.currentSize + valueLen) > r.maxSize {
			for randomKey := range r.items {
				r.doDelete(randomKey)
				break
			}
		}
	}
	r.items[key] = value
	r.currentSize += valueLen
	r.updatedAt[key] = time.Now()
	return true
}

// Expire expires keys which have expired
func (r *Cache) Expire() {
	r.Lock()
	defer r.Unlock()
	for key := range r.items {
		if !r.isValid(key) {
			r.doDelete(key)
		}
	}
}

// Delete deletes a given key if exists
func (r *Cache) Delete(key string) {
	r.Lock()
	defer r.Unlock()
	r.doDelete(key)
}

func (r *Cache) doDelete(key string) {
	if _, ok := r.items[key]; ok {
		r.currentSize -= uint64(len(r.items[key]))
		delete(r.items, key)
		delete(r.updatedAt, key)
		r.totalExpired++
		if r.OnExpired != nil {
			r.OnExpired(key)
		}
	}
}

func (r *Cache) isValid(key string) bool {
	updatedAt, ok := r.updatedAt[key]
	if !ok {
		return false
	}
	if r.expiration == noExpiration {
		return true
	}
	return updatedAt.Add(r.expiration).After(time.Now())
}
