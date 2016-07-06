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
	"bytes"
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

	// map of objectName and its contents
	entries map[string][]byte

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
		entries: make(map[string][]byte),
		expiry:  expiry,
	}
}

// ErrKeyNotFoundInCache - key not found in cache.
var ErrKeyNotFoundInCache = errors.New("Key not found in cache")

// ErrCacheFull - cache is full.
var ErrCacheFull = errors.New("Not enough space in cache")

// Used for adding entry to the object cache. Implements io.WriteCloser
type cacheBuffer struct {
	*bytes.Buffer // Implements io.Writer
	onClose       func()
}

// On close, onClose() is called which checks if all object contents
// have been written so that it can save the buffer to the cache.
func (c cacheBuffer) Close() error {
	c.onClose()
	return nil
}

// Create - validates if object size fits with in cache size limit and returns a io.WriteCloser
// to which object contents can be written and finally Close()'d. During Close() we
// checks if the amount of data written is equal to the size of the object, in which
// case it saves the contents to object cache.
func (c *Cache) Create(key string, size int64) (w io.WriteCloser, err error) {
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

	// Will hold the object contents.
	buf := bytes.NewBuffer(make([]byte, 0, size))
	// Account for the memory allocated above.
	c.currentSize += uint64(size)

	// Function called on close which saves the object contents
	// to the object cache.
	onClose := func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if buf.Len() != int(size) {
			// Full object not available hence do not save buf to object cache.
			c.currentSize -= uint64(size)
			return
		}
		// Full object available in buf, save it to cache.
		c.entries[key] = buf.Bytes()
		return
	}

	// Object contents that is written - cacheBuffer.Write(data)
	// will be accumulated in buf which implements io.Writer.
	return cacheBuffer{
		buf,
		onClose,
	}, nil
}

// Open - open the in-memory file, returns an in memory read seeker.
// returns an error ErrNotFoundInCache, if the key does not exist.
func (c *Cache) Open(key string) (io.ReadSeeker, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Entry exists, return the readable buffer.
	buffer, ok := c.entries[key]
	if !ok {
		return nil, ErrKeyNotFoundInCache
	}
	return bytes.NewReader(buffer), nil
}

// Delete - delete deletes an entry from in-memory fs.
func (c *Cache) Delete(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Delete an entry.
	buffer, ok := c.entries[key]
	if ok {
		c.deleteEntry(key, int64(len(buffer)))
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
