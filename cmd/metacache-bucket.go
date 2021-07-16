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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
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
	mu      sync.RWMutex `msg:"-"`
	updated bool         `msg:"-"`
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
			ez.renameAll(ctx, minioMetaBucket, metacachePrefixForID(bucket, slashSeparator))
		}
	}
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

// loadBucketMetaCache will load the cache from the object layer.
// If the cache cannot be found a new one is created.
func loadBucketMetaCache(ctx context.Context, bucket string) (*bucketMetacache, error) {
	objAPI := newObjectLayerFn()
	for objAPI == nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			time.Sleep(250 * time.Millisecond)
		}
		objAPI = newObjectLayerFn()
		if objAPI == nil {
			logger.LogIf(ctx, fmt.Errorf("loadBucketMetaCache: object layer not ready. bucket: %q", bucket))
		}
	}

	var meta bucketMetacache
	var decErr error
	// Use global context for this.
	r, err := objAPI.GetObjectNInfo(GlobalContext, minioMetaBucket, pathJoin("buckets", bucket, ".metacache", "index.s2"), nil, http.Header{}, readLock, ObjectOptions{})
	if err == nil {
		dec := s2DecPool.Get().(*s2.Reader)
		dec.Reset(r)
		decErr = meta.DecodeMsg(msgp.NewReader(dec))
		dec.Reset(nil)
		r.Close()
		s2DecPool.Put(dec)
	}
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

	hr, err := hash.NewReader(tmp, int64(tmp.Len()), "", "", int64(tmp.Len()))
	if err != nil {
		return err
	}
	_, err = objAPI.PutObject(ctx, minioMetaBucket, pathJoin("buckets", b.bucket, ".metacache", "index.s2"), NewPutObjReader(hr), ObjectOptions{})
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

	if o.Bucket != b.bucket {
		logger.Info("bucketMetacache.findCache: bucket %s does not match this bucket %s", o.Bucket, b.bucket)
		debug.PrintStack()
		return metacache{}
	}

	// Grab a write lock, since we create one if we cannot find one.
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if exists already.
	if c, ok := b.caches[o.ID]; ok {
		c.lastHandout = time.Now()
		b.caches[o.ID] = c
		b.debugf("returning existing %v", o.ID)
		return c
	}

	if !o.Create {
		return metacache{
			id:     o.ID,
			bucket: o.Bucket,
			status: scanStateNone,
		}
	}

	// Create new and add.
	best := o.newMetacache()
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

	// Test on a copy
	// cleanup is the only one deleting caches.
	caches, _ := b.cloneCaches()

	for id, cache := range caches {
		if !cache.worthKeeping() {
			b.debugf("cache %s not worth keeping", id)
			remove[id] = struct{}{}
			continue
		}
		if cache.id != id {
			logger.Info("cache ID mismatch %s != %s", id, cache.id)
			remove[id] = struct{}{}
			continue
		}
		if cache.bucket != b.bucket {
			logger.Info("cache bucket mismatch %s != %s", b.bucket, cache.bucket)
			remove[id] = struct{}{}
			continue
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

// deleteAll will delete all on disk data for ALL caches.
// Deletes are performed concurrently.
func (b *bucketMetacache) deleteAll() {
	ctx := context.Background()
	ez, ok := newObjectLayerFn().(*erasureServerPools)
	if !ok {
		logger.LogIf(ctx, errors.New("bucketMetacache: expected objAPI to be *erasurePools"))
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.updated = true
	// Delete all.
	ez.renameAll(ctx, minioMetaBucket, metacachePrefixForID(b.bucket, slashSeparator))
	b.caches = make(map[string]metacache, 10)
	b.cachesRoot = make(map[string][]string, 10)
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
