/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// localMetacacheMgr is the *local* manager for this peer.
// It should never be used directly since buckets are
// distributed deterministically.
// Therefore no cluster locks are required.
var localMetacacheMgr = &metacacheManager{
	buckets: make(map[string]*bucketMetacache),
	trash:   make(map[string]metacache),
}

type metacacheManager struct {
	mu      sync.RWMutex
	init    sync.Once
	buckets map[string]*bucketMetacache
	trash   map[string]metacache // Recently deleted lists.
}

const metacacheMaxEntries = 5000

// initManager will start async saving the cache.
func (m *metacacheManager) initManager() {
	// Start saver when object layer is ready.
	go func() {
		if !globalIsErasure {
			return
		}

		t := time.NewTicker(time.Minute)
		defer t.Stop()

		var exit bool
		for !exit {
			select {
			case <-t.C:
			case <-GlobalContext.Done():
				exit = true
			}
			m.mu.RLock()
			for _, v := range m.buckets {
				if !exit {
					v.cleanup()
				}
			}
			m.mu.RUnlock()
			m.mu.Lock()
			for k, v := range m.trash {
				if time.Since(v.lastUpdate) > metacacheMaxRunningAge {
					delete(m.trash, k)
				}
			}
			m.mu.Unlock()
		}
	}()
}

// findCache will get a metacache.
func (m *metacacheManager) findCache(ctx context.Context, o listPathOptions) metacache {
	m.mu.RLock()
	b, ok := m.buckets[o.Bucket]
	if ok {
		m.mu.RUnlock()
		return b.findCache(o)
	}
	if meta, ok := m.trash[o.ID]; ok {
		m.mu.RUnlock()
		return meta
	}
	m.mu.RUnlock()
	return m.getBucket(ctx, o.Bucket).findCache(o)
}

// updateCacheEntry will update non-transient state.
func (m *metacacheManager) updateCacheEntry(update metacache) (metacache, error) {
	m.mu.RLock()
	if meta, ok := m.trash[update.id]; ok {
		m.mu.RUnlock()
		return meta, nil
	}

	b, ok := m.buckets[update.bucket]
	m.mu.RUnlock()
	if ok {
		return b.updateCacheEntry(update)
	}

	// We should have either a trashed bucket or this
	return metacache{}, errVolumeNotFound
}

// getBucket will get a bucket metacache or load it from disk if needed.
func (m *metacacheManager) getBucket(ctx context.Context, bucket string) *bucketMetacache {
	m.init.Do(m.initManager)

	m.mu.RLock()
	b, ok := m.buckets[bucket]
	m.mu.RUnlock()
	if ok {
		if b.bucket != bucket {
			logger.Info("getBucket: cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}

	m.mu.Lock()
	// See if someone else fetched it while we waited for the lock.
	b, ok = m.buckets[bucket]
	if ok {
		m.mu.Unlock()
		if b.bucket != bucket {
			logger.Info("getBucket: newly cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}

	b = newBucketMetacache(bucket)
	m.buckets[bucket] = b
	m.mu.Unlock()
	return b
}

// deleteBucketCache will delete the bucket cache if it exists.
func (m *metacacheManager) deleteBucketCache(bucket string) {
	m.init.Do(m.initManager)
	m.mu.Lock()
	b, ok := m.buckets[bucket]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.buckets, bucket)
	m.mu.Unlock()

	// Since deletes may take some time we try to do it without
	// holding lock to m all the time.
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, v := range b.caches {
		v.error = "Bucket deleted"
		v.status = scanStateError
		m.mu.Lock()
		m.trash[k] = v
		m.mu.Unlock()
	}
}

// deleteAll will delete all caches.
func (m *metacacheManager) deleteAll() {
	m.init.Do(m.initManager)
	m.mu.Lock()
	defer m.mu.Unlock()
	for bucket := range m.buckets {
		delete(m.buckets, bucket)
	}
}
