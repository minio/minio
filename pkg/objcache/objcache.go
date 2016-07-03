/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
 *
 */

// Package objcache implements in memory caching methods.
package objcache

import (
	"errors"
	"io"
	"sync"
	"time"
)

// NoExpiration represents caches to be permanent and can only be deleted.
var NoExpiration = time.Duration(0)

// Cache holds the required variables to compose an in memory cache system
// which also provides expiring key mechanism and also maxSize.
type Cache struct {
	// Mutex is used for handling the concurrent
	// read/write requests for cache
	mutex *sync.RWMutex

	// maxSize is a total size for overall cache
	maxSize uint64

	// currentSize is a current size in memory
	currentSize uint64

	// OnEviction - callback function for eviction
	OnEviction func(a ...interface{})

	// totalEvicted counter to keep track of total expirations
	totalEvicted int

	// Represents in memory file system.
	entries map[string]*Buffer

	// Expiration in time duration.
	expiry time.Duration
}

// New creates an inmemory cache
//
// maxSize is used for expiring objects before we run out of memory
// expiration is used for expiration of a key from cache
func New(maxSize uint64, expiry time.Duration) *Cache {
	return &Cache{
		mutex:   &sync.RWMutex{},
		maxSize: maxSize,
		entries: make(map[string]*Buffer),
		expiry:  expiry,
	}
}

// ErrKeyNotFoundInCache - key not found in cache.
var ErrKeyNotFoundInCache = errors.New("Key not found in cache")

// ErrCacheFull - cache is full.
var ErrCacheFull = errors.New("Not enough space in cache")

// Size returns length of the value of a given key, returns -1 if key doesn't exist
func (c *Cache) Size(key string) int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.entries[key]
	if ok {
		return c.entries[key].Size()
	}
	return -1
}

// Create validates and returns an in memory writer referencing entry.
func (c *Cache) Create(key string, size int64) (io.Writer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	valueLen := uint64(size)
	if c.maxSize > 0 {
		// Check if the size of the object is not bigger than the capacity of the cache.
		if valueLen > c.maxSize {
			return nil, ErrCacheFull
		}
		// TODO - auto expire random key.
		if c.currentSize+valueLen > c.maxSize {
			return nil, ErrCacheFull
		}
	}
	c.entries[key] = NewBuffer(make([]byte, 0, int(size)))
	c.currentSize += valueLen
	return c.entries[key], nil
}

// Open - open the in-memory file, returns an memory reader.
// returns error ErrNotFoundInCache if fsPath does not exist.
func (c *Cache) Open(key string) (io.ReadSeeker, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Entry exists, return the readable buffer.
	buffer, ok := c.entries[key]
	if !ok {
		return nil, ErrKeyNotFoundInCache
	}
	return buffer, nil
}

// Delete - delete deletes an entry from in-memory fs.
func (c *Cache) Delete(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Delete an entry.
	buffer, ok := c.entries[key]
	if ok {
		size := buffer.Size()
		c.deleteEntry(key, size)
	}
}

// Deletes the entry that was found.
func (c *Cache) deleteEntry(key string, size int64) {
	delete(c.entries, key)
	c.currentSize -= uint64(size)
	c.totalEvicted++
	if c.OnEviction != nil {
		c.OnEviction(key)
	}
}
