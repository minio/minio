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

// Package metadata implements in memory caching methods for metadata information
package metadata

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
	items map[string]interface{}

	// updatedAt holds the time that related item's updated at
	updatedAt map[string]time.Time
}

// Stats current cache statistics
type Stats struct {
	Items int
}

// NewCache creates an inmemory cache
//
func NewCache() *Cache {
	return &Cache{
		items:     make(map[string]interface{}),
		updatedAt: map[string]time.Time{},
	}
}

// Stats get current cache statistics
func (r *Cache) Stats() Stats {
	return Stats{
		Items: len(r.items),
	}
}

// GetAll returs all the items
func (r *Cache) GetAll() map[string]interface{} {
	r.Lock()
	defer r.Unlock()
	// copy
	items := r.items
	return items
}

// Get returns a value of a given key if it exists
func (r *Cache) Get(key string) interface{} {
	r.Lock()
	defer r.Unlock()
	value, ok := r.items[key]
	if !ok {
		return nil
	}
	return value
}

// Exists returns true if key exists
func (r *Cache) Exists(key string) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.items[key]
	return ok
}

// Set will persist a value to the cache
func (r *Cache) Set(key string, value interface{}) bool {
	r.Lock()
	defer r.Unlock()
	r.items[key] = value
	return true
}

// Delete deletes a given key if exists
func (r *Cache) Delete(key string) {
	r.Lock()
	defer r.Unlock()
	r.doDelete(key)
}

func (r *Cache) doDelete(key string) {
	if _, ok := r.items[key]; ok {
		delete(r.items, key)
		delete(r.updatedAt, key)
	}
}
