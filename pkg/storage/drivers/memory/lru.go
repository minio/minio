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

import (
	"bytes"
	"container/list"
	"io"
	"strconv"
	"time"

	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
)

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

	// Expiration is the maximum duration of individual objects to exist
	// in cache before its evicted.
	Expiration time.Duration

	// OnEvicted optionally specificies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(a ...interface{})

	ll           *list.List
	totalSize    uint64
	totalEvicted int64
	cache        map[interface{}]*list.Element
}

type entry struct {
	key   string
	time  time.Time
	value *bytes.Buffer
}

// NewCache creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewCache(maxSize uint64, expiration time.Duration) *Cache {
	return &Cache{
		MaxSize:    maxSize,
		Expiration: expiration,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Stats return cache stats
func (c *Cache) Stats() CacheStats {
	return CacheStats{
		Bytes:     c.totalSize,
		Evictions: c.totalEvicted,
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, size int64) io.WriteCloser {
	r, w := io.Pipe()
	blockingWriter := NewBlockingWriteCloser(w)
	go func() {
		if uint64(size) > c.MaxSize {
			err := iodine.New(drivers.EntityTooLarge{
				Size:    strconv.FormatInt(size, 10),
				MaxSize: strconv.FormatUint(c.MaxSize, 10),
			}, nil)
			r.CloseWithError(err)
			blockingWriter.Release(err)
			return
		}
		// If MaxSize is zero expecting infinite memory
		if c.MaxSize != 0 {
			for (c.totalSize + uint64(size)) > c.MaxSize {
				c.RemoveOldest()
			}
		}
		value := new(bytes.Buffer)
		n, err := io.CopyN(value, r, size)
		if err != nil {
			err := iodine.New(err, nil)
			r.CloseWithError(err)
			blockingWriter.Release(err)
			return
		}
		ele := c.ll.PushFront(&entry{key, time.Now(), value})
		c.cache[key] = ele
		c.totalSize += uint64(n)
		r.Close()
		blockingWriter.Release(nil)
	}()
	return blockingWriter
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key string) (value []byte, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).time = time.Now()
		return ele.Value.(*entry).value.Bytes(), true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key string) {
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

// ExpireOldestAndWait expire old key which is expired and return wait times if any
func (c *Cache) ExpireOldestAndWait() time.Duration {
	if c.cache == nil {
		return 0
	}
	ele := c.ll.Back()
	if ele != nil {
		switch {
		case time.Now().Sub(ele.Value.(*entry).time) > c.Expiration:
			c.removeElement(ele)
		default:
			return (c.Expiration - time.Now().Sub(ele.Value.(*entry).time))
		}
	}
	return 0
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	c.totalEvicted++
	c.totalSize -= uint64(kv.value.Len())
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
