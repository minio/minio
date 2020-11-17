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
	"errors"
	"io"
	"path"
	"sync"

	"github.com/minio/minio/cmd/logger"
)

// listPath will return the requested entries.
// If no more entries are in the listing io.EOF is returned,
// otherwise nil or an unexpected error is returned.
// The listPathOptions given will be checked and modified internally.
// Required important fields are Bucket, Prefix, Separator.
// Other important fields are Limit, Marker.
// List ID always derived from the Marker.
func (z *erasureServerSets) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	if err := checkListObjsArgs(ctx, o.Bucket, o.Prefix, o.Marker, z); err != nil {
		return entries, err
	}

	// Marker is set validate pre-condition.
	if o.Marker != "" && o.Prefix != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(o.Marker, o.Prefix) {
			return entries, io.EOF
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if o.Limit == 0 {
		return entries, io.EOF
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if o.Separator == SlashSeparator && o.Prefix == SlashSeparator {
		return entries, io.EOF
	}

	// Over flowing count - reset to maxObjectList.
	if o.Limit < 0 || o.Limit > maxObjectList {
		o.Limit = maxObjectList
	}

	// If delimiter is slashSeparator we must return directories of
	// the non-recursive scan unless explicitly requested.
	o.IncludeDirectories = o.Separator == slashSeparator
	if (o.Separator == slashSeparator || o.Separator == "") && !o.Recursive {
		o.Recursive = o.Separator != slashSeparator
		o.Separator = slashSeparator
	} else {
		// Default is recursive, if delimiter is set then list non recursive.
		o.Recursive = true
	}

	// Decode and get the optional list id from the marker.
	o.Marker, o.ID = parseMarker(o.Marker)
	o.Create = o.ID == ""
	if o.ID == "" {
		o.ID = mustGetUUID()
	}
	o.BaseDir = baseDirFromPrefix(o.Prefix)
	if o.singleObject {
		// Override for single object.
		o.BaseDir = o.Prefix
	}

	var cache metacache
	// If we don't have a list id we must ask the server if it has a cache or create a new.
	if o.Create {
		o.CurrentCycle = intDataUpdateTracker.current()
		o.OldestCycle = globalNotificationSys.findEarliestCleanBloomFilter(ctx, path.Join(o.Bucket, o.BaseDir))
		var cache metacache
		rpc := globalNotificationSys.restClientFromHash(o.Bucket)
		if isReservedOrInvalidBucket(o.Bucket, false) {
			rpc = nil
			o.Transient = true
		}
		if rpc == nil || o.Transient {
			// Local
			cache = localMetacacheMgr.findCache(ctx, o)
		} else {
			c, err := rpc.GetMetacacheListing(ctx, o)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// Context is canceled, return at once.
					// request canceled, no entries to return
					return entries, io.EOF
				}
				logger.LogIf(ctx, err)
				o.Transient = true
				cache = localMetacacheMgr.findCache(ctx, o)
			} else {
				cache = *c
			}
		}
		if cache.fileNotFound {
			// No cache found, no entries found.
			return entries, io.EOF
		}
		// Only create if we created a new.
		o.Create = o.ID == cache.id
		o.ID = cache.id
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	allAtEOF := true
	asked := 0
	mu.Lock()
	// Ask all sets and merge entries.
	for _, zone := range z.serverSets {
		for _, set := range zone.sets {
			wg.Add(1)
			asked++
			go func(i int, set *erasureObjects) {
				defer wg.Done()
				e, err := set.listPath(ctx, o)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					allAtEOF = false
				}
				errs[i] = err
				entries.merge(e, -1)

				// Resolve non-trivial conflicts
				entries.deduplicate(func(existing, other *metaCacheEntry) (replace bool) {
					if existing.isDir() {
						return false
					}
					eFIV, err := existing.fileInfo(o.Bucket)
					if err != nil {
						return true
					}
					oFIV, err := existing.fileInfo(o.Bucket)
					if err != nil {
						return false
					}
					return oFIV.ModTime.After(eFIV.ModTime)
				})
				if entries.len() > o.Limit {
					allAtEOF = false
					entries.truncate(o.Limit)
				}
			}(len(errs), set)
			errs = append(errs, nil)
		}
	}
	mu.Unlock()
	wg.Wait()

	if isAllNotFound(errs) {
		// All sets returned not found.
		// Update master cache with that information.
		cache.status = scanStateSuccess
		cache.fileNotFound = true
		_, err := o.updateMetacacheListing(cache, globalNotificationSys.restClientFromHash(o.Bucket))
		logger.LogIf(ctx, err)
		// cache returned not found, entries truncated.
		return entries, io.EOF
	}

	for _, err := range errs {
		if err == nil {
			allAtEOF = false
			continue
		}
		if err.Error() == io.EOF.Error() {
			continue
		}
		logger.LogIf(ctx, err)
		return entries, err
	}
	truncated := entries.len() > o.Limit || !allAtEOF
	entries.truncate(o.Limit)
	entries.listID = o.ID
	if !truncated {
		return entries, io.EOF
	}
	return entries, nil
}
