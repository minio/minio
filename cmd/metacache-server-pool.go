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
	o.parseMarker()
	o.BaseDir = baseDirFromPrefix(o.Prefix)
	o.Transient = o.Transient || isMinioReservedBucket(o.Bucket)
	if o.Transient {
		o.Create = false
	}

	// We have 2 cases:
	// 1) Cold listing, just list.
	// 2) Returning, but with no id. Start async listing.
	// 3) Returning, with ID, stream from list.
	//
	// If we don't have a list id we must ask the server if it has a cache or create a new.
	if o.ID != "" && !o.Transient {
		// Create or ping with handout...
		var cache metacache
		rpc := globalNotificationSys.restClientFromHash(o.Bucket)
		if isReservedOrInvalidBucket(o.Bucket, false) {
			o.Transient = true
		}
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
				// TODO: Remove, not really informational.
				logger.LogIf(ctx, err)
			}
			o.Transient = true
			o.Create = false
			o.ID = mustGetUUID()
		} else {
			if cache.fileNotFound {
				// No cache found, no entries found.
				return entries, io.EOF
			}
			if cache.status == scanStateError || cache.status == scanStateNone {
				o.ID = mustGetUUID()
				o.Create = false
			} else {
				// Continue listing
				o.ID = c.id
			}
		}
	}

	if o.ID != "" && !o.Transient {
		// We have an existing list ID, continue streaming.
		if o.Create {
			entries, err = z.listAndSave(ctx, o)
			if err == nil {
				return entries, nil
			}
		} else {
			entries, err = z.serverPools[o.pool].sets[o.set].streamMetadataParts(ctx, o)
			if err == nil {
				return entries, nil
			}
		}
		if IsErr(err, []error{
			nil,
			context.Canceled,
			context.DeadlineExceeded,
			// io.EOF is expected and should be returned but no need to log it.
			io.EOF,
		}...) {
			// Expected good errors we don't need to return error.
			return entries, err
		}
		entries.truncate(0)
		// TODO: Cancel existing, bad listing...
		o.Create = true
		o.ID = mustGetUUID()
	}

	// Do listing in-place.
	// Create output for our results.
	// Create filter for results.
	filterCh := make(chan metaCacheEntry, o.Limit)
	filteredResults := o.gatherResults(filterCh)
	listCtx, cancelList := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	var listErr error

	go func() {
		defer wg.Done()
		listErr = z.listMerged(listCtx, o, filterCh)
	}()

	entries, err = filteredResults()
	cancelList()
	wg.Wait()
	if listErr != nil && !errors.Is(listErr, context.Canceled) {
		return entries, listErr
	}
	truncated := entries.len() > o.Limit || err == nil
	entries.truncate(o.Limit)
	if !o.Transient {
		entries.listID = o.ID
	}
	if !truncated {
		return entries, io.EOF
	}
	return entries, nil
}

// listMerged will list across all sets and return a merged results stream.
// The result channel is closed when no more results are expected.
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
	if allAtEOF {
		// TODO" Maybe, maybe not
		return io.EOF
	}
	return nil
}

func (z *erasureServerPools) listAndSave(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Use ID as the object name...
	o.pool = z.getAvailablePoolIdx(ctx, minioMetaBucket, o.ID, 10<<20)
	if o.pool < 0 {
		// No space or similar, don't persist the listing.
		o.pool = 0
		o.Create = false
		o.ID = ""
		return entries, errDiskFull
	}
	o.set = z.serverPools[o.pool].getHashedSetIndex(o.ID)
	saver := z.serverPools[o.pool].sets[o.set]

	// Disconnect from call above, but cancel on exit.
	listCtx, cancel := context.WithCancel(GlobalContext)
	saveCh := make(chan metaCacheEntry, metacacheBlockSize)
	inCh := make(chan metaCacheEntry, metacacheBlockSize)
	outCh := make(chan metaCacheEntry, o.Limit)

	filteredResults := o.gatherResults(outCh)

	mc := o.newMetacache()
	meta := metaCacheRPC{meta: &mc, cancel: cancel, rpc: globalNotificationSys.restClientFromHash(o.Bucket), o: o}

	// Save listing...
	go func() {
		if err := saver.saveMetaCacheStream(listCtx, &meta, saveCh); err != nil {
			meta.setErr(err.Error())
		}
		cancel()
	}()

	// Do listing...
	go func() {
		err := z.listMerged(listCtx, o, inCh)
		if err != nil {
			meta.setErr(err.Error())
		}
		// Indicate end...
		close(inCh)
	}()

	// Write listing to results and saver.
	go func() {
		for entry := range inCh {
			select {
			case outCh <- entry:
			default:
			}
			saveCh <- entry
		}
		close(outCh)
		close(saveCh)
	}()

	return filteredResults()
}
