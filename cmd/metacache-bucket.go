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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/hash"
	"github.com/tinylib/msgp/msgp"
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
func newBucketMetacache(bucket string, cleanup bool) *bucketMetacache {
	if cleanup {
		// Recursively delete all caches.
		objAPI := newObjectLayerFn()
		ez, ok := objAPI.(*erasureServerPools)
		if ok {
			ctx := context.Background()
			ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(bucket, slashSeparator))
		}
	}
	return &bucketMetacache{
		bucket:     bucket,
		caches:     make(map[string]metacache, 10),
		cachesRoot: make(map[string][]string, 10),
	}
}

// loadBucketMetaCache will load the cache from the object layer.
// If the cache cannot be found a new one is created.
func loadBucketMetaCache(ctx context.Context, bucket string) (*bucketMetacache, error) {
	objAPI := newObjectLayerFn()
	for objAPI == nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		objAPI = newObjectLayerFn()
		if objAPI == nil {
			logger.LogIf(ctx, fmt.Errorf("loadBucketMetaCache: object layer not ready. bucket: %q", bucket))
		}
	}
	var meta bucketMetacache
	var decErr error
	var wg sync.WaitGroup
	wg.Add(1)

	r, w := io.Pipe()
	go func() {
		defer wg.Done()
		dec := s2DecPool.Get().(*s2.Reader)
		dec.Reset(r)
		decErr = meta.DecodeMsg(msgp.NewReader(dec))
		dec.Reset(nil)
		s2DecPool.Put(dec)
		r.CloseWithError(decErr)
	}()
	// Use global context for this.
	err := objAPI.GetObject(GlobalContext, minioMetaBucket, pathJoin("buckets", bucket, ".metacache", "index.s2"), 0, -1, w, "", ObjectOptions{})
	logger.LogIf(ctx, w.CloseWithError(err))
	if err != nil {
		switch err.(type) {
		case ObjectNotFound:
			err = nil
		case InsufficientReadQuorum:
			// Cache is likely lost. Clean up and return new.
			return newBucketMetacache(bucket, true), nil
		default:
			logger.LogIf(ctx, err)
		}
		return newBucketMetacache(bucket, false), err
	}
	wg.Wait()
	if decErr != nil {
		if errors.Is(err, context.Canceled) {
			return newBucketMetacache(bucket, false), err
		}
		// Log the error, but assume the data is lost and return a fresh bucket.
		// Otherwise a broken cache will never recover.
		logger.LogIf(ctx, decErr)
		return newBucketMetacache(bucket, true), nil
	}
	// Sanity check...
	if meta.bucket != bucket {
		logger.Info("loadBucketMetaCache: loaded cache name mismatch, want %s, got %s. Discarding.", bucket, meta.bucket)
		return newBucketMetacache(bucket, true), nil
	}
	meta.cachesRoot = make(map[string][]string, len(meta.caches)/10)
	// Index roots
	for id, cache := range meta.caches {
		meta.cachesRoot[cache.root] = append(meta.cachesRoot[cache.root], id)
	}
	return &meta, nil
}

// save the bucket cache to the object storage.
func (b *bucketMetacache) save(ctx context.Context) error {
	if b.transient {
		return nil
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Keep lock while we marshal.
	// We need a write lock since we update 'updated'
	b.mu.Lock()
	if !b.updated {
		b.mu.Unlock()
		return nil
	}
	// Save as s2 compressed msgpack
	tmp := bytes.NewBuffer(make([]byte, 0, b.Msgsize()))
	enc := s2.NewWriter(tmp)
	err := msgp.Encode(enc, b)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	err = enc.Close()
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.updated = false
	b.mu.Unlock()

	hr, err := hash.NewReader(tmp, int64(tmp.Len()), "", "", int64(tmp.Len()), false)
	if err != nil {
		return err
	}
	_, err = objAPI.PutObject(ctx, minioMetaBucket, pathJoin("buckets", b.bucket, ".metacache", "index.s2"), NewPutObjReader(hr, nil, nil), ObjectOptions{})
	logger.LogIf(ctx, err)
	return err
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
	const debugPrint = false

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
		if debugPrint {
			console.Info("returning existing %v", o.ID)
		}
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
		if debugPrint {
			console.Info("returning new cache %s, bucket: %v", best.id, best.bucket)
		}
		return best
	}

	interesting := interestingCaches(o.BaseDir, b.cachesRoot)

	var best metacache
	for _, id := range interesting {
		cached, ok := b.caches[id]
		if !ok {
			continue
		}

		// Never return transient caches if there is no id.
		if cached.status == scanStateError || cached.status == scanStateNone || cached.dataVersion != metacacheStreamVersion {
			if debugPrint {
				console.Info("cache %s state or stream version mismatch", cached.id)
			}
			continue
		}
		if cached.startedCycle < o.OldestCycle {
			if debugPrint {
				console.Info("cache %s cycle too old", cached.id)
			}
			continue
		}

		// If the existing listing wasn't recursive root must match.
		if !cached.recursive && o.BaseDir != cached.root {
			if debugPrint {
				console.Info("cache %s  non rec prefix mismatch, cached:%v, want:%v", cached.id, cached.root, o.BaseDir)
			}
			continue
		}

		// Root of what we are looking for must at least have
		if !strings.HasPrefix(o.BaseDir, cached.root) {
			if debugPrint {
				console.Info("cache %s prefix mismatch, cached:%v, want:%v", cached.id, cached.root, o.BaseDir)
			}
			continue
		}
		if cached.filter != "" && strings.HasPrefix(cached.filter, o.FilterPrefix) {
			if debugPrint {
				console.Info("cache %s cannot be used because of filter %s", cached.id, cached.filter)
			}
			continue
		}

		if o.Recursive && !cached.recursive {
			if debugPrint {
				console.Info("cache %s not recursive", cached.id)
			}
			// If this is recursive the cached listing must be as well.
			continue
		}
		if o.Separator != slashSeparator && !cached.recursive {
			if debugPrint {
				console.Info("cache %s not slashsep and not recursive", cached.id)
			}
			// Non slash separator requires recursive.
			continue
		}
		if !cached.finished() && time.Since(cached.lastUpdate) > metacacheMaxRunningAge {
			if debugPrint {
				console.Info("cache %s not running, time: %v", cached.id, time.Since(cached.lastUpdate))
			}
			// Abandoned
			continue
		}

		if cached.finished() && cached.endedCycle <= o.OldestCycle {
			if extend <= 0 {
				// If scan has ended the oldest requested must be less.
				if debugPrint {
					console.Info("cache %s ended and cycle (%v) <= oldest allowed (%v)", cached.id, cached.endedCycle, o.OldestCycle)
				}
				continue
			}
			if time.Since(cached.lastUpdate) > metacacheMaxRunningAge+extend {
				// Cache ended within bloom cycle, but we can extend the life.
				if debugPrint {
					console.Info("cache %s ended (%v) and beyond extended life (%v)", cached.id, cached.lastUpdate, extend+metacacheMaxRunningAge)
				}
				continue
			}
		}
		if cached.started.Before(best.started) {
			if debugPrint {
				console.Info("cache %s disregarded - we have a better", cached.id)
			}
			// If we already have a newer, keep that.
			continue
		}
		best = cached
	}
	if !best.started.IsZero() {
		if o.Create {
			best.lastHandout = UTCNow()
			b.caches[best.id] = best
			b.updated = true
		}
		if debugPrint {
			console.Info("returning cached %s, status: %v, ended: %v", best.id, best.status, best.ended)
		}
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
	if debugPrint {
		console.Info("returning new cache %s, bucket: %v", best.id, best.bucket)
	}
	return best
}

// cleanup removes redundant and outdated entries.
func (b *bucketMetacache) cleanup() {
	// Entries to remove.
	remove := make(map[string]struct{})
	currentCycle := intDataUpdateTracker.current()

	const debugPrint = false

	// Test on a copy
	// cleanup is the only one deleting caches.
	caches, rootIdx := b.cloneCaches()

	for id, cache := range caches {
		if b.transient && time.Since(cache.lastUpdate) > 15*time.Minute && time.Since(cache.lastHandout) > 15*time.Minute {
			// Keep transient caches only for 15 minutes.
			remove[id] = struct{}{}
			continue
		}
		if !cache.worthKeeping(currentCycle) {
			if debugPrint {
				logger.Info("cache %s not worth keeping", id)
			}
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
				if debugPrint {
					logger.Info("cache %s can be replaced by %s", id, cache2.id)
				}
				remove[id] = struct{}{}
				break
			} else {
				if debugPrint {
					logger.Info("cache %s can be NOT replaced by %s", id, cache2.id)
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
		logger.Info("updateCacheEntry: bucket %s list id %v not found", b.bucket, update.id)
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

// deleteAll will delete all on disk data for ALL caches.
// Deletes are performed concurrently.
func (b *bucketMetacache) deleteAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx := context.Background()
	ez, ok := newObjectLayerFn().(*erasureServerPools)
	if !ok {
		logger.LogIf(ctx, errors.New("bucketMetacache: expected objAPI to be *erasureZones"))
		return
	}

	b.updated = true
	if !b.transient {
		// Delete all.
		ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(b.bucket, slashSeparator))
		b.caches = make(map[string]metacache, 10)
		b.cachesRoot = make(map[string][]string, 10)
		return
	}

	// Transient are in different buckets.
	var wg sync.WaitGroup
	for id := range b.caches {
		wg.Add(1)
		go func(cache metacache) {
			defer wg.Done()
			ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(cache.bucket, cache.id))
		}(b.caches[id])
	}
	wg.Wait()
	b.caches = make(map[string]metacache, 10)
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
	if ok {
		c.delete(context.Background())
	}
}
