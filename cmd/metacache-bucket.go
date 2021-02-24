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
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/console"
)

//go:generate msgp -file $GOFILE -unexported

// a bucketMetacache keeps track of all caches generated
// for a bucket.
type bucketMetacache struct {
	// Name of bucket
	bucket string

	// caches indexed by id.
	caches map[string]metacache
	// cache ids indexed by root paths
	cachesRoot map[string][]string `msg:"-"`

	// Internal state
	mu        sync.RWMutex `msg:"-"`
	updated   bool         `msg:"-"`
	transient bool         `msg:"-"` // bucket used for non-persisted caches.
}

// newBucketMetacache creates a new bucketMetacache.
// Optionally remove all existing caches.
func newBucketMetacache(bucket string) *bucketMetacache {
	return &bucketMetacache{
		bucket:     bucket,
		caches:     make(map[string]metacache, 10),
		cachesRoot: make(map[string][]string, 10),
	}
}

func (b *bucketMetacache) debugf(format string, data ...interface{}) {
	if serverDebugLog {
		console.Debugf(format+"\n", data...)
	}
}

// findCache will attempt to find a matching cache for the provided options.
// If a cache with the same ID exists already it will be returned.
// If none can be found a new is created with the provided ID.
func (b *bucketMetacache) findCache(o listPathOptions) metacache {
	if b == nil {
		logger.Info("bucketMetacache.findCache: nil cache for bucket %s", o.Bucket)
		return metacache{}
	}

	if o.Bucket != b.bucket && !b.transient {
		logger.Info("bucketMetacache.findCache: bucket %s does not match this bucket %s", o.Bucket, b.bucket)
		debug.PrintStack()
		return metacache{}
	}

	extend := globalAPIConfig.getExtendListLife()

	// Grab a write lock, since we create one if we cannot find one.
	if o.Create {
		b.mu.Lock()
		defer b.mu.Unlock()
	} else {
		b.mu.RLock()
		defer b.mu.RUnlock()
	}

	// Check if exists already.
	if c, ok := b.caches[o.ID]; ok {
		b.debugf("returning existing %v", o.ID)
		return c
	}
	// No need to do expensive checks on transients.
	if b.transient {
		if !o.Create {
			return metacache{
				id:     o.ID,
				bucket: o.Bucket,
				status: scanStateNone,
			}
		}

		// Create new
		best := o.newMetacache()
		b.caches[o.ID] = best
		b.updated = true
		b.debugf("returning new cache %s, bucket: %v", best.id, best.bucket)
		return best
	}

	var best metacache
	rootSplit := strings.Split(o.BaseDir, slashSeparator)
	for i := range rootSplit {
		interesting := b.cachesRoot[path.Join(rootSplit[:i+1]...)]

		for _, id := range interesting {
			cached, ok := b.caches[id]
			if !ok {
				continue
			}
			if !cached.matches(&o, extend) {
				continue
			}
			if cached.started.Before(best.started) {
				b.debugf("cache %s disregarded - we have a better", cached.id)
				// If we already have a newer, keep that.
				continue
			}
			best = cached
		}
	}
	if !best.started.IsZero() {
		if o.Create {
			best.lastHandout = UTCNow()
			b.caches[best.id] = best
			b.updated = true
		}
		b.debugf("returning cached %s, status: %v, ended: %v", best.id, best.status, best.ended)
		return best
	}
	if !o.Create {
		return metacache{
			id:     o.ID,
			bucket: o.Bucket,
			status: scanStateNone,
		}
	}

	// Create new and add.
	best = o.newMetacache()
	b.caches[o.ID] = best
	b.cachesRoot[best.root] = append(b.cachesRoot[best.root], best.id)
	b.updated = true
	b.debugf("returning new cache %s, bucket: %v", best.id, best.bucket)
	return best
}

// cleanup removes redundant and outdated entries.
func (b *bucketMetacache) cleanup() {
	// Entries to remove.
	remove := make(map[string]struct{})
	currentCycle := intDataUpdateTracker.current()

	// Test on a copy
	// cleanup is the only one deleting caches.
	caches, rootIdx := b.cloneCaches()

	for id, cache := range caches {
		if b.transient && time.Since(cache.lastUpdate) > 10*time.Minute && time.Since(cache.lastHandout) > 10*time.Minute {
			// Keep transient caches only for 15 minutes.
			remove[id] = struct{}{}
			continue
		}
		if !cache.worthKeeping(currentCycle) {
			b.debugf("cache %s not worth keeping", id)
			remove[id] = struct{}{}
			continue
		}
		if cache.id != id {
			logger.Info("cache ID mismatch %s != %s", id, cache.id)
			remove[id] = struct{}{}
			continue
		}
		if cache.bucket != b.bucket && !b.transient {
			logger.Info("cache bucket mismatch %s != %s", b.bucket, cache.bucket)
			remove[id] = struct{}{}
			continue
		}
	}

	// Check all non-deleted against eachother.
	// O(n*n), but should still be rather quick.
	for id, cache := range caches {
		if b.transient {
			break
		}
		if _, ok := remove[id]; ok {
			continue
		}

		interesting := interestingCaches(cache.root, rootIdx)
		for _, id2 := range interesting {
			if _, ok := remove[id2]; ok || id2 == id {
				// Don't check against one we are already removing
				continue
			}
			cache2, ok := caches[id2]
			if !ok {
				continue
			}

			if cache.canBeReplacedBy(&cache2) {
				b.debugf("cache %s can be replaced by %s", id, cache2.id)
				remove[id] = struct{}{}
				break
			} else {
				b.debugf("cache %s can be NOT replaced by %s", id, cache2.id)
			}
		}
	}

	// If above limit, remove the caches with the oldest handout time.
	if len(caches)-len(remove) > metacacheMaxEntries {
		remainCaches := make([]metacache, 0, len(caches)-len(remove))
		for id, cache := range caches {
			if _, ok := remove[id]; ok {
				continue
			}
			remainCaches = append(remainCaches, cache)
		}
		if len(remainCaches) > metacacheMaxEntries {
			// Sort oldest last...
			sort.Slice(remainCaches, func(i, j int) bool {
				return remainCaches[i].lastHandout.Before(remainCaches[j].lastHandout)
			})
			// Keep first metacacheMaxEntries...
			for _, cache := range remainCaches[metacacheMaxEntries:] {
				if time.Since(cache.lastHandout) > 30*time.Minute {
					remove[cache.id] = struct{}{}
				}
			}
		}
	}

	for id := range remove {
		b.deleteCache(id)
	}
}

// Potentially interesting caches.
// Will only add root if request is for root.
func interestingCaches(root string, cachesRoot map[string][]string) []string {
	var interesting []string
	rootSplit := strings.Split(root, slashSeparator)
	for i := range rootSplit {
		want := path.Join(rootSplit[:i+1]...)
		interesting = append(interesting, cachesRoot[want]...)
	}
	return interesting
}

// updateCache will update a cache by id.
// If the cache cannot be found nil is returned.
// The bucket cache will be locked until the done .
func (b *bucketMetacache) updateCache(id string) (cache *metacache, done func()) {
	b.mu.Lock()
	c, ok := b.caches[id]
	if !ok {
		b.mu.Unlock()
		return nil, func() {}
	}
	return &c, func() {
		c.lastUpdate = UTCNow()
		b.caches[id] = c
		b.mu.Unlock()
	}
}

// updateCacheEntry will update a cache.
// Returns the updated status.
func (b *bucketMetacache) updateCacheEntry(update metacache) (metacache, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	existing, ok := b.caches[update.id]
	if !ok {
		return update, errFileNotFound
	}
	existing.update(update)
	b.caches[update.id] = existing
	b.updated = true
	return existing, nil
}

// cloneCaches will return a clone of all current caches.
func (b *bucketMetacache) cloneCaches() (map[string]metacache, map[string][]string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	dst := make(map[string]metacache, len(b.caches))
	for k, v := range b.caches {
		dst[k] = v
	}
	// Copy indexes
	dst2 := make(map[string][]string, len(b.cachesRoot))
	for k, v := range b.cachesRoot {
		tmp := make([]string, len(v))
		copy(tmp, v)
		dst2[k] = tmp
	}

	return dst, dst2
}

// getCache will return a clone of a specific metacache.
// Will return nil if the cache doesn't exist.
func (b *bucketMetacache) getCache(id string) *metacache {
	b.mu.RLock()
	c, ok := b.caches[id]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return &c
}

// deleteCache will delete a specific cache and all files related to it across the cluster.
func (b *bucketMetacache) deleteCache(id string) {
	b.mu.Lock()
	c, ok := b.caches[id]
	if ok {
		// Delete from root map.
		list := b.cachesRoot[c.root]
		for i, lid := range list {
			if id == lid {
				list = append(list[:i], list[i+1:]...)
				break
			}
		}
		b.cachesRoot[c.root] = list
		delete(b.caches, id)
		b.updated = true
	}
	b.mu.Unlock()
}
