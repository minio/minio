/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
---
Modifications from Minio under the following license:

Minimalist Object Storage, (C) 2015 Minio, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memory

import "container/list"

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     uint64
	Evictions int64
}

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxSize is the maximum number of cache size for entries
	// before an item is evicted. Zero means no limit
	MaxSize uint64

	// OnEvicted optionally specificies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(a ...interface{})

	ll           *list.List
	totalSize    uint64
	totalEvicted int64
	cache        map[interface{}]*list.Element
	value        []byte
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

type entry struct {
	key   Key
	value []byte
}

// NewCache creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		MaxSize: maxSize,
		ll:      list.New(),
		cache:   make(map[interface{}]*list.Element),
	}
}

// Stats return cache stats
func (c *Cache) Stats() CacheStats {
	return CacheStats{
		Bytes:     c.totalSize,
		Evictions: c.totalEvicted,
	}
}

func (c *Cache) Write(p []byte) (n int, err error) {
	c.totalSize = c.totalSize + uint64(len(p))
	// If MaxSize is zero expecting infinite memory
	if c.MaxSize != 0 && c.totalSize > c.MaxSize {
		c.totalSize -= uint64(len(p))
		c.RemoveOldest()
	}
	c.value = append(c.value, p...)
	return len(p), nil
}

// Add adds a value to the cache.
func (c *Cache) Add(key Key) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	ele := c.ll.PushFront(&entry{key, c.value})
	c.value = nil
	c.cache[key] = ele
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key Key) (value []byte, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

// GetOldest returns the oldest key
func (c *Cache) GetOldest() (key Key, ok bool) {
	if c.cache == nil {
		return nil, false
	}
	ele := c.ll.Back()
	if ele != nil {
		return ele.Value.(*entry).key, true
	}
	return nil, false
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	c.totalEvicted++
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key)
	}
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}
