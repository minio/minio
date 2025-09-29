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

	"github.com/minio/minio/internal/grid"
	xioutil "github.com/minio/minio/internal/ioutil"
)

func renameAllBucketMetacache(epPath string) error {
	// Rename all previous `.minio.sys/buckets/<bucketname>/.metacache` to
	// to `.minio.sys/tmp/` for deletion.
	return readDirFn(pathJoin(epPath, minioMetaBucket, bucketMetaPrefix), func(name string, typ os.FileMode) error {
		if typ == os.ModeDir {
			tmpMetacacheOld := pathutil.Join(epPath, minioMetaTmpDeletedBucket, mustGetUUID())
			if err := renameAll(pathJoin(epPath, minioMetaBucket, metacachePrefixForID(name, slashSeparator)),
				tmpMetacacheOld, epPath); err != nil && err != errFileNotFound {
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
func (z *erasureServerPools) listPath(ctx context.Context, o *listPathOptions) (entries metaCacheEntriesSorted, err error) {
	if err := checkListObjsArgs(ctx, o.Bucket, o.Prefix, o.Marker); err != nil {
		return entries, err
	}

	// Marker points to before the prefix, just ignore it.
	if o.Marker < o.Prefix {
		o.Marker = ""
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
	if o.BaseDir == "" {
		o.BaseDir = baseDirFromPrefix(o.Prefix)
	}
	o.Transient = o.Transient || isReservedOrInvalidBucket(o.Bucket, false)
	o.SetFilter()
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
		rpc := globalNotificationSys.restClientFromHash(pathJoin(o.Bucket, o.Prefix))
		var c *metacache
		if rpc == nil {
			resp := localMetacacheMgr.getBucket(ctx, o.Bucket).findCache(*o)
			c = &resp
		} else {
			rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			c, err = rpc.GetMetacacheListing(rctx, *o)
			cancel()
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Context is canceled, return at once.
				// request canceled, no entries to return
				return entries, io.EOF
			}
			if !IsErr(err, context.DeadlineExceeded, grid.ErrDisconnected) {
				// Report error once per bucket, but continue listing.x
				storageLogOnceIf(ctx, err, "GetMetacacheListing:"+o.Bucket)
			}
			o.Transient = true
			o.Create = false
			o.ID = mustGetUUID()
		} else {
			if c.fileNotFound {
				// No cache found, no entries found.
				return entries, io.EOF
			}
			if c.status == scanStateError || c.status == scanStateNone {
				o.ID = ""
				o.Create = false
				o.debugln("scan status", c.status, " - waiting a roundtrip to create")
			} else {
				// Continue listing
				o.ID = c.id
				go c.keepAlive(ctx, rpc)
			}
		}
	}

	if o.ID != "" && !o.Transient {
		// We have an existing list ID, continue streaming.
		if o.Create {
			o.debugln("Creating", o)
			entries, err = z.listAndSave(ctx, o)
			if err == nil || err == io.EOF {
				return entries, err
			}
			entries.truncate(0)
		} else {
			if o.pool < len(z.serverPools) && o.set < len(z.serverPools[o.pool].sets) {
				o.debugln("Resuming", o)
				entries, err = z.serverPools[o.pool].sets[o.set].streamMetadataParts(ctx, *o)
				entries.reuse = true // We read from stream and are not sharing results.
				if err == nil {
					return entries, nil
				}
			} else {
				err = fmt.Errorf("invalid pool/set")
				o.pool, o.set = 0, 0
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
		go func() {
			rpc := globalNotificationSys.restClientFromHash(pathJoin(o.Bucket, o.Prefix))
			if rpc != nil {
				ctx, cancel := context.WithTimeout(GlobalContext, 5*time.Second)
				defer cancel()
				c, err := rpc.GetMetacacheListing(ctx, *o)
				if err == nil {
					c.error = "no longer used"
					c.status = scanStateError
					rpc.UpdateMetacacheListing(ctx, *c)
				}
			}
		}()
		o.ID = ""
	}

	if contextCanceled(ctx) {
		return entries, ctx.Err()
	}
	// Do listing in-place.
	// Create output for our results.
	// Create filter for results.
	o.debugln("Raw List", o)
	filterCh := make(chan metaCacheEntry, o.Limit)
	listCtx, cancelList := context.WithCancel(ctx)
	filteredResults := o.gatherResults(listCtx, filterCh)
	var wg sync.WaitGroup
	wg.Add(1)
	var listErr error

	go func(o listPathOptions) {
		defer wg.Done()
		if o.Lifecycle == nil {
			// No filtering ahead, ask drives to stop
			// listing exactly at a specific limit.
			o.StopDiskAtLimit = true
		}
		listErr = z.listMerged(listCtx, o, filterCh)
		o.debugln("listMerged returned with", listErr)
	}(*o)

	entries, err = filteredResults()
	cancelList()
	wg.Wait()
	if listErr != nil && !errors.Is(listErr, context.Canceled) {
		return entries, listErr
	}
	entries.reuse = true
	truncated := entries.len() > o.Limit || err == nil
	entries.truncate(o.Limit)
	if !o.Transient && truncated {
		if o.ID == "" {
			entries.listID = mustGetUUID()
		} else {
			entries.listID = o.ID
		}
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
	listCtx, cancelList := context.WithCancel(ctx)
	defer cancelList()
	for _, pool := range z.serverPools {
		for _, set := range pool.sets {
			wg.Add(1)
			innerResults := make(chan metaCacheEntry, 100)
			inputs = append(inputs, innerResults)
			go func(i int, set *erasureObjects) {
				defer wg.Done()
				err := set.listPath(listCtx, o, innerResults)
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
	// Quorum is one since we are merging across sets.
	err := mergeEntryChannels(ctx, inputs, results, 1)

	cancelList()
	wg.Wait()

	// we should return 'errs' from per disk
	if isAllNotFound(errs) {
		if isAllVolumeNotFound(errs) {
			return errVolumeNotFound
		}
		return nil
	}

	if err != nil {
		return err
	}

	if contextCanceled(ctx) {
		return ctx.Err()
	}

	for _, err := range errs {
		if errors.Is(err, io.EOF) {
			continue
		}
		if err == nil || contextCanceled(ctx) || errors.Is(err, context.Canceled) {
			allAtEOF = false
			continue
		}
		storageLogIf(ctx, err)
		return err
	}
	if allAtEOF {
		return io.EOF
	}
	return nil
}

// triggerExpiryAndRepl applies lifecycle and replication actions on the listing
// It returns true if the listing is non-versioned and the given object is expired.
func triggerExpiryAndRepl(ctx context.Context, o listPathOptions, obj metaCacheEntry) (skip bool) {
	versioned := o.Versioning != nil && o.Versioning.Versioned(obj.name)

	// skip latest object from listing only for regular
	// listObjects calls, versioned based listing cannot
	// filter out between versions 'obj' cannot be truncated
	// in such a manner, so look for skipping an object only
	// for regular ListObjects() call only.
	if !o.Versioned && !o.V1 {
		fi, err := obj.fileInfo(o.Bucket)
		if err != nil {
			return skip
		}
		objInfo := fi.ToObjectInfo(o.Bucket, obj.name, versioned)
		if o.Lifecycle != nil {
			act := evalActionFromLifecycle(ctx, *o.Lifecycle, o.Retention, o.Replication.Config, objInfo).Action
			skip = act.Delete() && !act.DeleteRestored()
		}
	}

	fiv, err := obj.fileInfoVersions(o.Bucket)
	if err != nil {
		return skip
	}

	// Expire all versions if needed, if not attempt to queue for replication.
	for _, version := range fiv.Versions {
		objInfo := version.ToObjectInfo(o.Bucket, obj.name, versioned)

		if o.Lifecycle != nil {
			evt := evalActionFromLifecycle(ctx, *o.Lifecycle, o.Retention, o.Replication.Config, objInfo)
			if evt.Action.Delete() {
				globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_s3ListObjects)
				if !evt.Action.DeleteRestored() {
					continue
				} // queue version for replication upon expired restored copies if needed.
			}
		}

		queueReplicationHeal(ctx, o.Bucket, objInfo, o.Replication, 0)
	}
	return skip
}

func (z *erasureServerPools) listAndSave(ctx context.Context, o *listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Use ID as the object name...
	o.pool = z.getAvailablePoolIdx(ctx, minioMetaBucket, o.ID, 10<<20)
	if o.pool < 0 {
		// No space or similar, don't persist the listing.
		o.pool = 0
		o.Create = false
		o.ID = ""
		o.Transient = true
		return entries, errDiskFull
	}
	o.set = z.serverPools[o.pool].getHashedSetIndex(o.ID)
	saver := z.serverPools[o.pool].sets[o.set]

	// Disconnect from call above, but cancel on exit.
	listCtx, cancel := context.WithCancel(GlobalContext)
	saveCh := make(chan metaCacheEntry, metacacheBlockSize)
	inCh := make(chan metaCacheEntry, metacacheBlockSize)
	outCh := make(chan metaCacheEntry, o.Limit)

	filteredResults := o.gatherResults(ctx, outCh)

	mc := o.newMetacache()
	meta := metaCacheRPC{meta: &mc, cancel: cancel, rpc: globalNotificationSys.restClientFromHash(pathJoin(o.Bucket, o.Prefix)), o: *o}

	// Save listing...
	go func() {
		if err := saver.saveMetaCacheStream(listCtx, &meta, saveCh); err != nil {
			meta.setErr(err.Error())
		}
		cancel()
	}()

	// Do listing...
	go func(o listPathOptions) {
		err := z.listMerged(listCtx, o, inCh)
		if err != nil {
			meta.setErr(err.Error())
		}
		o.debugln("listAndSave: listing", o.ID, "finished with ", err)
	}(*o)

	// Keep track of when we return since we no longer have to send entries to output.
	var funcReturned bool
	var funcReturnedMu sync.Mutex
	defer func() {
		funcReturnedMu.Lock()
		funcReturned = true
		funcReturnedMu.Unlock()
	}()
	// Write listing to results and saver.
	go func() {
		var returned bool
		for entry := range inCh {
			if o.shouldSkip(ctx, entry) {
				continue
			}
			if !returned {
				funcReturnedMu.Lock()
				returned = funcReturned
				funcReturnedMu.Unlock()
				outCh <- entry
				if returned {
					xioutil.SafeClose(outCh)
				}
			}
			entry.reusable = returned
			saveCh <- entry
		}
		if !returned {
			xioutil.SafeClose(outCh)
		}
		xioutil.SafeClose(saveCh)
	}()

	entries, err = filteredResults()
	if err == nil {
		// Check if listing recorded an error.
		err = meta.getErr()
	}
	return entries, err
}
