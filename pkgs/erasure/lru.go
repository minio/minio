/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package erasure

import (
	"github.com/golang/groupcache/lru"
	"sync"
)

// thread-safe LRU cache from GroupCache
type Cache struct {
	mutex sync.RWMutex
	cache *lru.Cache
}

var DefaultCache *Cache = GetCache(0)

// Allocate ``Cache`` LRU
func GetCache(capacity int) *Cache {
	return &Cache{
		cache: lru.New(capacity),
	}
}

// ``GetC()`` -- Grab encoder from LRU
func (c *Cache) GetC(ep *EncoderParams) *Encoder {
	if encoder, ret := c._Get(ep); ret {
		return encoder
	}
	encoder := NewEncoder(ep)
	c._Put(ep, encoder)
	return encoder
}

// ``_Get()`` -- Get key from existing LRU
func (c *Cache) _Get(ep *EncoderParams) (*Encoder, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if encoder, ret := c.cache.Get(ep); ret {
		return encoder.(*Encoder), ret
	}
	return nil, false
}

// ``_Put()`` -- Add key to existing LRU
func (c *Cache) _Put(ep *EncoderParams, encoder *Encoder) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache.Add(ep, encoder)
}
