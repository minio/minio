/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"container/list"
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
	items *list.List

	// reverseItems holds the time that related item's updated at
	reverseItems map[interface{}]*list.Element

	// maxSize is a total size for overall cache
	maxSize uint64

	// currentSize is a current size in memory
	currentSize uint64

	// OnEvicted - callback function for eviction
	OnEvicted func(a ...interface{})

	// totalEvicted counter to keep track of total expirations
	totalEvicted int
}

// Stats current cache statistics
type Stats struct {
	Bytes   uint64
	Items   int
	Evicted int
}

type element struct {
	key   interface{}
	value []byte
}

// NewCache creates an inmemory cache
//
// maxSize is used for expiring objects before we run out of memory
// expiration is used for expiration of a key from cache
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		items:        list.New(),
		reverseItems: make(map[interface{}]*list.Element),
		maxSize:      maxSize,
	}
}

// SetMaxSize set a new max size
func (r *Cache) SetMaxSize(maxSize uint64) {
	r.Lock()
	defer r.Unlock()
	r.maxSize = maxSize
	return
}

// Stats get current cache statistics
func (r *Cache) Stats() Stats {
	return Stats{
		Bytes:   r.currentSize,
		Items:   r.items.Len(),
		Evicted: r.totalEvicted,
	}
}

// Get returns a value of a given key if it exists
func (r *Cache) Get(key interface{}) ([]byte, bool) {
	r.Lock()
	defer r.Unlock()
	ele, hit := r.reverseItems[key]
	if !hit {
		return nil, false
	}
	r.items.MoveToFront(ele)
	return ele.Value.(*element).value, true
}

// Len returns length of the value of a given key, returns zero if key doesn't exist
func (r *Cache) Len(key interface{}) int {
	r.Lock()
	defer r.Unlock()
	_, ok := r.reverseItems[key]
	if !ok {
		return 0
	}
	return len(r.reverseItems[key].Value.(*element).value)
}

// Append will append new data to an existing key,
// if key doesn't exist it behaves like Set()
func (r *Cache) Append(key interface{}, value []byte) bool {
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
			r.doDeleteOldest()
			break
		}
	}
	ele, hit := r.reverseItems[key]
	if !hit {
		ele := r.items.PushFront(&element{key, value})
		r.currentSize += valueLen
		r.reverseItems[key] = ele
		return true
	}
	r.items.MoveToFront(ele)
	r.currentSize += valueLen
	ele.Value.(*element).value = append(ele.Value.(*element).value, value...)
	return true
}

// Set will persist a value to the cache
func (r *Cache) Set(key interface{}, value []byte) bool {
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
			r.doDeleteOldest()
		}
	}
	if _, hit := r.reverseItems[key]; hit {
		return false
	}
	ele := r.items.PushFront(&element{key, value})
	r.currentSize += valueLen
	r.reverseItems[key] = ele
	return true
}

// Delete deletes a given key if exists
func (r *Cache) Delete(key interface{}) {
	r.Lock()
	defer r.Unlock()
	ele, ok := r.reverseItems[key]
	if !ok {
		return
	}
	if ele != nil {
		r.currentSize -= uint64(len(r.reverseItems[key].Value.(*element).value))
		r.items.Remove(ele)
		delete(r.reverseItems, key)
		r.totalEvicted++
		if r.OnEvicted != nil {
			r.OnEvicted(key)
		}
	}
}

func (r *Cache) doDeleteOldest() {
	ele := r.items.Back()
	if ele != nil {
		r.currentSize -= uint64(len(r.reverseItems[ele.Value.(*element).key].Value.(*element).value))
		delete(r.reverseItems, ele.Value.(*element).key)
		r.items.Remove(ele)
		r.totalEvicted++
		if r.OnEvicted != nil {
			r.OnEvicted(ele.Value.(*element).key)
		}
	}
}
