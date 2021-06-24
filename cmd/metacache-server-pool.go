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
	"errors"
	"fmt"
	"io"
	"os"
	pathutil "path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/logger"
)

func renameAllBucketMetacache(epPath string) error {
	// Rename all previous `.minio.sys/buckets/<bucketname>/.metacache` to
	// to `.minio.sys/tmp/` for deletion.
	return readDirFn(pathJoin(epPath, minioMetaBucket, bucketMetaPrefix), func(name string, typ os.FileMode) error {
		if typ == os.ModeDir {
			tmpMetacacheOld := pathutil.Join(epPath, minioMetaTmpDeletedBucket, mustGetUUID())
			if err := renameAll(pathJoin(epPath, minioMetaBucket, metacachePrefixForID(name, slashSeparator)),
				tmpMetacacheOld); err != nil && err != errFileNotFound {
				return fmt.Errorf("unable to rename (%s -> %s) %w",
					pathJoin(epPath, minioMetaBucket+metacachePrefixForID(minioMetaBucket, slashSeparator)),
					tmpMetacacheOld,
					osErrToFileErr(err))
			}
		}
		return nil
	})
}

// listPath will return the requested entries.
// If no more entries are in the listing io.EOF is returned,
// otherwise nil or an unexpected error is returned.
// The listPathOptions given will be checked and modified internally.
// Required important fields are Bucket, Prefix, Separator.
// Other important fields are Limit, Marker.
// List ID always derived from the Marker.
func (z *erasureServerPools) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
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
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if strings.HasPrefix(o.Prefix, SlashSeparator) {
		return entries, io.EOF
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
	o.BaseDir = o.Prefix

	var cache metacache
	// If we don't have a list id we must ask the server if it has a cache or create a new.
	if o.Create {
		var cache metacache
		rpc := globalNotificationSys.restClientFromHash(o.ID)
		if isReservedOrInvalidBucket(o.Bucket, false) {
			rpc = nil
			o.Transient = true
		}
		// Apply prefix filter if enabled.
		o.SetFilter()
		if rpc == nil || o.Transient {
			// Local
			cache = localMetacacheMgr.findCache(ctx, o)
		} else {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			c, err := rpc.GetMetacacheListing(ctx, o)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					// Context is canceled, return at once.
					// request canceled, no entries to return
					return entries, io.EOF
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					logger.LogIf(ctx, err)
				}
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

	// Create output for our results.
	// Create filter for results.
	filterCh := make(chan metaCacheEntry, o.Limit)
	filteredResults := o.gatherResults(filterCh)
	listCtx, cancelList := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var listErr error

	go func() {
		defer wg.Done()
		listErr = z.listMerged(listCtx, o, filterCh)
	}()

	entries, err = filteredResults()
	cancelList()
	wg.Done()
	if listErr != nil && !errors.Is(listErr, context.Canceled) {
		return entries, listErr
	}
	truncated := entries.len() > o.Limit || err == nil
	entries.truncate(o.Limit)
	if !o.discardResult {
		entries.listID = o.ID
	}
	if !truncated {
		return entries, io.EOF
	}
	return entries, nil
}

// listMerged will list across all sets and return a merged results stream.
func (z *erasureServerPools) listMerged(ctx context.Context, o listPathOptions, results chan<- metaCacheEntry) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	allAtEOF := true
	var inputs []chan metaCacheEntry
	mu.Lock()
	// Ask all sets and merge entries.
	for _, pool := range z.serverPools {
		for _, set := range pool.sets {
			wg.Add(1)
			results := make(chan metaCacheEntry, 100)
			inputs = append(inputs, results)
			go func(i int, set *erasureObjects) {
				defer wg.Done()
				err := set.listPath(ctx, o, results)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					allAtEOF = false
				}
				errs[i] = err
			}(len(errs), set)
			errs = append(errs, nil)
		}
	}
	mu.Unlock()

	// Gather results to a single channel.
	err := mergeEntryChannels(ctx, inputs, results, func(existing, other *metaCacheEntry) (replace bool) {
		if existing.isDir() {
			// We don't care...
			return false
		}

		eFIV, err := existing.fileInfo(o.Bucket)
		if err != nil {
			return true
		}
		oFIV, err := other.fileInfo(o.Bucket)
		if err != nil {
			return false
		}
		// Replace if modtime is newer
		if !oFIV.ModTime.Equal(eFIV.ModTime) {
			return oFIV.ModTime.After(eFIV.ModTime)
		}
		// Use NumVersions as a final tiebreaker.
		return oFIV.NumVersions > eFIV.NumVersions
	})

	wg.Wait()
	if err != nil {
		return err
	}
	if contextCanceled(ctx) {
		return ctx.Err()
	}

	if isAllNotFound(errs) {
		// All sets returned not found.
		go func() {
			// Update master cache with that information.
			//cache.status = scanStateSuccess
			//cache.fileNotFound = true
			//o.updateMetacacheListing(cache, globalNotificationSys.restClientFromHash(o.Bucket))
		}()
		// cache returned not found, entries truncated.
		return nil
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
		return err
	}
	return nil
}
