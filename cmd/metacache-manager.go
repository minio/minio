// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/minio/minio/internal/logger"
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
	// Add a transient bucket.
	// Start saver when object layer is ready.
	go func() {
		objAPI := newObjectLayerFn()
		for objAPI == nil {
			time.Sleep(time.Second)
			objAPI = newObjectLayerFn()
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
					v.delete(context.Background())
					delete(m.trash, k)
				}
			}
			m.mu.Unlock()
		}
	}()
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

	// Return a transient bucket for invalid or system buckets.
	m.mu.RLock()
	b, ok := m.buckets[bucket]
	if ok {
		m.mu.RUnlock()
		if b.bucket != bucket {
			logger.Info("getBucket: cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}

	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	// See if someone else fetched it while we waited for the lock.
	b, ok = m.buckets[bucket]
	if ok {
		if b.bucket != bucket {
			logger.Info("getBucket: newly cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}

	// New bucket. If we fail return the transient bucket.
	b = newBucketMetacache(bucket, true)
	m.buckets[bucket] = b
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
		if time.Since(v.lastUpdate) > metacacheMaxRunningAge {
			v.delete(context.Background())
			continue
		}
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
	for bucket, b := range m.buckets {
		b.deleteAll()
		delete(m.buckets, bucket)
	}
}

// checkMetacacheState should be used if data is not updating.
// Should only be called if a failure occurred.
func (o listPathOptions) checkMetacacheState(ctx context.Context, rpc *peerRESTClient) error {
	// We operate on a copy...
	o.Create = false
	c, err := rpc.GetMetacacheListing(ctx, o)
	if err != nil {
		return err
	}
	cache := *c

	if cache.status == scanStateNone || cache.fileNotFound {
		return errFileNotFound
	}
	if cache.status == scanStateSuccess || cache.status == scanStateStarted {
		if time.Since(cache.lastUpdate) > metacacheMaxRunningAge {
			// We got a stale entry, mark error on handling server.
			err := fmt.Errorf("timeout: list %s not updated", cache.id)
			cache.error = err.Error()
			cache.status = scanStateError
			rpc.UpdateMetacacheListing(ctx, cache)
			return err
		}
		return nil
	}
	if cache.error != "" {
		return fmt.Errorf("async cache listing failed with: %s", cache.error)
	}
	return nil
}
