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
	"fmt"
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
}

type metacacheManager struct {
	mu      sync.RWMutex
	init    sync.Once
	buckets map[string]*bucketMetacache
}

const metacacheManagerTransientBucket = "**transient**"

// initManager will start async saving the cache.
func (m *metacacheManager) initManager() {
	// Add a transient bucket.
	tb := newBucketMetacache(metacacheManagerTransientBucket)
	tb.transient = true
	m.buckets[metacacheManagerTransientBucket] = tb

	// Start saver when object layer is ready.
	go func() {
		objAPI := newObjectLayerFn()
		for objAPI == nil {
			time.Sleep(time.Second)
			objAPI = newObjectLayerFn()
		}
		if !globalIsErasure {
			logger.Info("metacacheManager was initialized in non-erasure mode, skipping save")
			return
		}

		t := time.NewTicker(time.Minute)
		var exit bool
		bg := context.Background()
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
				logger.LogIf(bg, v.save(bg))
			}
			m.mu.RUnlock()
		}
		m.getTransient().deleteAll()
	}()
}

// getBucket will get a bucket metacache or load it from disk if needed.
func (m *metacacheManager) getBucket(ctx context.Context, bucket string) *bucketMetacache {
	m.init.Do(m.initManager)

	// Return a transient bucket for invalid or system buckets.
	if isReservedOrInvalidBucket(bucket, false) {
		return m.getTransient()
	}
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

	// Load bucket. If we fail return the transient bucket.
	b, err := loadBucketMetaCache(ctx, bucket)
	if err != nil {
		m.mu.Unlock()
		return m.getTransient()
	}
	if b.bucket != bucket {
		logger.LogIf(ctx, fmt.Errorf("getBucket: loaded bucket %s does not match this bucket %s", b.bucket, bucket))
	}
	m.buckets[bucket] = b
	m.mu.Unlock()
	return b
}

// deleteAll will delete all caches.
func (m *metacacheManager) deleteAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range m.buckets {
		b.deleteAll()
	}
}

// getTransient will return a transient bucket.
func (m *metacacheManager) getTransient() *bucketMetacache {
	m.init.Do(m.initManager)
	m.mu.RLock()
	bmc := m.buckets[metacacheManagerTransientBucket]
	m.mu.RUnlock()
	return bmc
}

// checkMetacacheState should be used if data is not updating.
// Should only be called if a failure occurred.
func (o listPathOptions) checkMetacacheState(ctx context.Context) error {
	// We operate on a copy...
	o.Create = false
	var cache metacache
	if !o.Transient {
		rpc := globalNotificationSys.restClientFromHash(o.Bucket)
		if rpc == nil {
			// Local
			cache = localMetacacheMgr.getBucket(ctx, o.Bucket).findCache(o)
		} else {
			c, err := rpc.GetMetacacheListing(ctx, o)
			if err != nil {
				return err
			}
			cache = *c
		}
	} else {
		cache = localMetacacheMgr.getTransient().findCache(o)
	}

	if cache.status == scanStateNone || cache.fileNotFound {
		return errFileNotFound
	}
	if cache.status == scanStateSuccess {
		if time.Since(cache.lastUpdate) > metacacheMaxRunningAge {
			return fmt.Errorf("timeout: list %s finished and no update for 1 minute", cache.id)
		}
		return nil
	}
	if cache.error != "" {
		return fmt.Errorf("async cache listing failed with: %s", cache.error)
	}
	if cache.status == scanStateStarted {
		if time.Since(cache.lastUpdate) > metacacheMaxRunningAge {
			return fmt.Errorf("cache id %s listing not updating. Last update %s seconds ago", cache.id, time.Since(cache.lastUpdate).Round(time.Second))
		}
	}
	return nil
}
