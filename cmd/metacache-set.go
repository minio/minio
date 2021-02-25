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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/console"
)

type listPathOptions struct {
	// ID of the listing.
	// This will be used to persist the list.
	ID string

	// Bucket of the listing.
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Scan/return only content with prefix.
	Prefix string

	// FilterPrefix will return only results with this prefix when scanning.
	// Should never contain a slash.
	// Prefix should still be set.
	FilterPrefix string

	// Marker to resume listing.
	// The response will be the first entry AFTER this object name.
	Marker string

	// Limit the number of results.
	Limit int

	// The number of disks to ask. Special values:
	// 0 uses default number of disks.
	// -1 use at least 50% of disks or at least the default number.
	AskDisks int

	// InclDeleted will keep all entries where latest version is a delete marker.
	InclDeleted bool

	// Scan recursively.
	// If false only main directory will be scanned.
	// Should always be true if Separator is n SlashSeparator.
	Recursive bool

	// Separator to use.
	Separator string

	// Create indicates that the lister should not attempt to load an existing cache.
	Create bool

	// CurrentCycle indicates the current bloom cycle.
	// Will be used if a new scan is started.
	CurrentCycle uint64

	// OldestCycle indicates the oldest cycle acceptable.
	OldestCycle uint64

	// Include pure directories.
	IncludeDirectories bool

	// discardResult will not persist the cache to storage.
	// When the initial results are returned listing will be canceled.
	discardResult bool
}

func init() {
	gob.Register(listPathOptions{})
}

// newMetacache constructs a new metacache from the options.
func (o listPathOptions) newMetacache() metacache {
	return metacache{
		id:           o.ID,
		bucket:       o.Bucket,
		root:         o.BaseDir,
		recursive:    o.Recursive,
		status:       scanStateStarted,
		error:        "",
		started:      UTCNow(),
		lastHandout:  UTCNow(),
		lastUpdate:   UTCNow(),
		ended:        time.Time{},
		startedCycle: o.CurrentCycle,
		endedCycle:   0,
		dataVersion:  metacacheStreamVersion,
		filter:       o.FilterPrefix,
	}
}

func (o *listPathOptions) debugf(format string, data ...interface{}) {
	if serverDebugLog {
		console.Debugf(format+"\n", data...)
	}
}

func (o *listPathOptions) debugln(data ...interface{}) {
	if serverDebugLog {
		console.Debugln(data...)
	}
}

// gatherResults will collect all results on the input channel and filter results according to the options.
// Caller should close the channel when done.
// The returned function will return the results once there is enough or input is closed.
func (o *listPathOptions) gatherResults(in <-chan metaCacheEntry) func() (metaCacheEntriesSorted, error) {
	var resultsDone = make(chan metaCacheEntriesSorted)
	// Copy so we can mutate
	resCh := resultsDone
	resErr := io.EOF

	go func() {
		var results metaCacheEntriesSorted
		for entry := range in {
			if resCh == nil {
				// past limit
				continue
			}
			if !o.IncludeDirectories && entry.isDir() {
				continue
			}
			o.debugln("gather got:", entry.name)
			if o.Marker != "" && entry.name <= o.Marker {
				o.debugln("pre marker")
				continue
			}
			if !strings.HasPrefix(entry.name, o.Prefix) {
				o.debugln("not in prefix")
				continue
			}
			if !o.Recursive && !entry.isInDir(o.Prefix, o.Separator) {
				o.debugln("not in dir", o.Prefix, o.Separator)
				continue
			}
			if !o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() {
				o.debugln("latest is delete marker")
				continue
			}
			if o.Limit > 0 && results.len() >= o.Limit {
				// We have enough and we have more.
				// Do not return io.EOF
				if resCh != nil {
					resErr = nil
					resCh <- results
					resCh = nil
				}
				continue
			}
			o.debugln("adding...")
			results.o = append(results.o, entry)
		}
		if resCh != nil {
			resErr = io.EOF
			resCh <- results
		}
	}()
	return func() (metaCacheEntriesSorted, error) {
		return <-resultsDone, resErr
	}
}

// updateMetacacheListing will update the metacache listing.
func (o *listPathOptions) updateMetacacheListing(m metacache, rpc *peerRESTClient) (metacache, error) {
	if rpc == nil {
		return localMetacacheMgr.updateCacheEntry(m)
	}
	return rpc.UpdateMetacacheListing(context.Background(), m)
}

const metacachePrefix = ".metacache"

func metacachePrefixForID(bucket, id string) string {
	return pathJoin(bucketMetaPrefix, bucket, metacachePrefix, id)
}

// objectPath returns the object path of the cache.
func (o *listPathOptions) objectPath() string {
	return pathJoin(metacachePrefixForID(o.Bucket, o.ID), "block.s2")
}

func (o *listPathOptions) SetFilter() {
	switch {
	case metacacheSharePrefix:
		return
	case o.CurrentCycle != o.OldestCycle:
		// We have a clean bloom filter
		return
	case o.Prefix == o.BaseDir:
		// No additional prefix
		return
	}
	// Remove basedir.
	o.FilterPrefix = strings.TrimPrefix(o.Prefix, o.BaseDir)
	// Remove leading and trailing slashes.
	o.FilterPrefix = strings.Trim(o.FilterPrefix, slashSeparator)

	if strings.Contains(o.FilterPrefix, slashSeparator) {
		// Sanity check, should not happen.
		o.FilterPrefix = ""
	}
}

// filter will apply the options and return the number of objects requested by the limit.
// Will return io.EOF if there are no more entries with the same filter.
// The last entry can be used as a marker to resume the listing.
func (r *metacacheReader) filter(o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Forward to prefix, if any
	err = r.forwardTo(o.Prefix)
	if err != nil {
		return entries, err
	}
	if o.Marker != "" {
		err = r.forwardTo(o.Marker)
		if err != nil {
			return entries, err
		}
		next, err := r.peek()
		if err != nil {
			return entries, err
		}
		if next.name == o.Marker {
			err := r.skip(1)
			if err != nil {
				return entries, err
			}
		}
	}
	o.debugln("forwarded to ", o.Prefix, "marker:", o.Marker, "sep:", o.Separator)

	// Filter
	if !o.Recursive {
		entries.o = make(metaCacheEntries, 0, o.Limit)
		pastPrefix := false
		err := r.readFn(func(entry metaCacheEntry) bool {
			if o.Prefix != "" && !strings.HasPrefix(entry.name, o.Prefix) {
				// We are past the prefix, don't continue.
				pastPrefix = true
				return false
			}
			if !o.IncludeDirectories && entry.isDir() {
				return true
			}
			if !entry.isInDir(o.Prefix, o.Separator) {
				return true
			}
			if !o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() {
				return entries.len() < o.Limit
			}
			entries.o = append(entries.o, entry)
			return entries.len() < o.Limit
		})
		if (err != nil && err.Error() == io.EOF.Error()) || pastPrefix || r.nextEOF() {
			return entries, io.EOF
		}
		return entries, err
	}

	// We should not need to filter more.
	return r.readN(o.Limit, o.InclDeleted, o.IncludeDirectories, o.Prefix)
}

func (er *erasureObjects) streamMetadataParts(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	r, err := er.metaCache.Open(pathJoin(minioMetaBucket, o.objectPath()), time.Now().Add(-time.Hour))
	if err != nil {
		return entries, io.EOF
	}

	tmp, err := newMetacacheReader(r)
	if err != nil {
		return entries, err
	}

	e, err := tmp.filter(o)
	entries.o = append(entries.o, e.o...)
	if o.Limit > 0 && entries.len() > o.Limit {
		entries.truncate(o.Limit)
		return entries, nil
	}

	if err == nil {
		// We stopped within the listing, we are done for now...
		return entries, nil
	}

	if !errors.Is(err, io.EOF) {
		logger.LogIf(ctx, err)
	}

	return entries, err
}

// Will return io.EOF if continuing would not yield more results.
func (er *erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	o.debugf(color.Green("listPath:")+" with options: %#v", o)

	// See if we have the listing stored.
	if !o.Create && !o.discardResult {
		entries, err := er.streamMetadataParts(ctx, o)
		if IsErr(err, []error{
			nil,
			context.Canceled,
			context.DeadlineExceeded,
		}...) {
			// Expected good errors we don't need to return error.
			return entries, nil
		}

		if !errors.Is(err, io.EOF) { // io.EOF is expected and should be returned but no need to log it.
			// Log an return errors on unexpected errors.
			logger.LogIf(ctx, err)
		}

		return entries, err
	}

	meta := o.newMetacache()
	var rpc *peerRESTClient
	if !o.discardResult {
		rpc = globalNotificationSys.restClientFromHash(o.Bucket)
	}
	var metaMu sync.Mutex

	o.debugln(color.Green("listPath:")+" scanning bucket:", o.Bucket, "basedir:", o.BaseDir, "prefix:", o.Prefix, "marker:", o.Marker)

	// Disconnect from call above, but cancel on exit.
	ctx, cancel := context.WithCancel(GlobalContext)
	// We need to ask disks.
	disks := er.getOnlineDisks()

	defer func() {
		o.debugln(color.Green("listPath:")+" returning:", entries.len(), "err:", err)
		if err != nil && !errors.Is(err, io.EOF) {
			go func(err string) {
				metaMu.Lock()
				if meta.status != scanStateError {
					meta.error = err
					meta.status = scanStateError
				}
				meta, _ = o.updateMetacacheListing(meta, rpc)
				metaMu.Unlock()
			}(err.Error())
			cancel()
		}
	}()

	askDisks := o.AskDisks
	listingQuorum := askDisks - 1
	// Special case: ask all disks if the drive count is 4
	if askDisks == -1 || er.setDriveCount == 4 {
		askDisks = len(disks) // with 'strict' quorum list on all online disks.
		listingQuorum = getReadQuorum(er.setDriveCount)
	}

	if len(disks) < askDisks {
		err = InsufficientReadQuorum{}
		logger.LogIf(ctx, fmt.Errorf("listPath: Insufficient disks, %d of %d needed are available", len(disks), askDisks))
		cancel()
		return
	}

	// Select askDisks random disks.
	if len(disks) > askDisks {
		disks = disks[:askDisks]
	}

	// Create output for our results.
	var cacheCh chan metaCacheEntry
	if !o.discardResult {
		cacheCh = make(chan metaCacheEntry, metacacheBlockSize)
	}

	// Create filter for results.
	filterCh := make(chan metaCacheEntry, 100)
	filteredResults := o.gatherResults(filterCh)
	closeChannels := func() {
		if !o.discardResult {
			close(cacheCh)
		}
		close(filterCh)
	}

	// Cancel listing on return if non-saved list.
	if o.discardResult {
		defer cancel()
	}

	go func() {
		defer cancel()
		// Save continuous updates
		go func() {
			var err error
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			var exit bool
			for !exit {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					exit = true
				}
				metaMu.Lock()
				meta.endedCycle = intDataUpdateTracker.current()
				meta, err = o.updateMetacacheListing(meta, rpc)
				if meta.status == scanStateError {
					logger.LogIf(ctx, err)
					cancel()
					exit = true
				}
				metaMu.Unlock()
			}
		}()

		wc := er.metaCache.Create(pathJoin(minioMetaBucket, o.objectPath()))

		var bw *metacacheBlockWriter
		// Don't save single object listings.
		if !o.discardResult {
			// Write results to disk.
			bw = newMetacacheBlockWriter(cacheCh, func(b *metacacheBlock) error {
				n, err := wc.Write(b.data)
				if err != nil {
					metaMu.Lock()
					if meta.error != "" {
						meta.status = scanStateError
						meta.error = err.Error()
					}
					metaMu.Unlock()
					cancel()
					return err
				}
				if n != len(b.data) {
					metaMu.Lock()
					if meta.error != "" {
						meta.status = scanStateError
						meta.error = io.ErrShortWrite.Error()
					}
					metaMu.Unlock()
					cancel()
					return io.ErrShortWrite
				}
				o.debugln(color.Green("listPath:")+" saving block to", o.objectPath())
				return nil
			})
		}

		// How to resolve results.
		resolver := metadataResolutionParams{
			dirQuorum: listingQuorum,
			objQuorum: listingQuorum,
			bucket:    o.Bucket,
		}

		err := listPathRaw(ctx, listPathRawOptions{
			disks:        disks,
			bucket:       o.Bucket,
			path:         o.BaseDir,
			recursive:    o.Recursive,
			filterPrefix: o.FilterPrefix,
			minDisks:     listingQuorum,
			agreed: func(entry metaCacheEntry) {
				if !o.discardResult {
					cacheCh <- entry
				}
				filterCh <- entry
			},
			partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
				// Results Disagree :-(
				entry, ok := entries.resolve(&resolver)
				if ok {
					if !o.discardResult {
						cacheCh <- *entry
					}
					filterCh <- *entry
				}
			},
		})

		metaMu.Lock()
		if err != nil {
			meta.status = scanStateError
			meta.error = err.Error()
		}
		// Save success
		if meta.error == "" {
			meta.status = scanStateSuccess
			meta.endedCycle = intDataUpdateTracker.current()
		}

		meta, _ = o.updateMetacacheListing(meta, rpc)
		metaMu.Unlock()

		closeChannels()
		if !o.discardResult {
			if err := bw.Close(); err != nil {
				metaMu.Lock()
				meta.error = err.Error()
				meta.status = scanStateError
				meta, _ = o.updateMetacacheListing(meta, rpc)
				metaMu.Unlock()
			}
			if err := wc.Close(); err != nil {
				metaMu.Lock()
				meta.error = err.Error()
				meta.status = scanStateError
				meta, _ = o.updateMetacacheListing(meta, rpc)
				metaMu.Unlock()
			}
		}
	}()

	return filteredResults()
}

type listPathRawOptions struct {
	disks        []StorageAPI
	bucket, path string
	recursive    bool

	// Only return results with this prefix.
	filterPrefix string

	// Forward to this prefix before returning results.
	forwardTo string

	// Minimum number of good disks to continue.
	// An error will be returned if this many disks returned an error.
	minDisks       int
	reportNotFound bool

	// Callbacks with results:
	// If set to nil, it will not be called.

	// agreed is called if all disks agreed.
	agreed func(entry metaCacheEntry)

	// partial will be returned when there is disagreement between disks.
	// if disk did not return any result, but also haven't errored
	// the entry will be empty and errs will
	partial func(entries metaCacheEntries, nAgreed int, errs []error)

	// finished will be called when all streams have finished and
	// more than one disk returned an error.
	// Will not be called if everything operates as expected.
	finished func(errs []error)
}

// listPathRaw will list a path on the provided drives.
// See listPathRawOptions on how results are delivered.
// Directories are always returned.
// Cache will be bypassed.
// Context cancellation will be respected but may take a while to effectuate.
func listPathRaw(ctx context.Context, opts listPathRawOptions) (err error) {
	disks := opts.disks
	if len(disks) == 0 {
		return fmt.Errorf("listPathRaw: 0 drives provided")
	}

	// Disconnect from call above, but cancel on exit.
	ctx, cancel := context.WithCancel(GlobalContext)
	defer cancel()

	askDisks := len(disks)
	readers := make([]*metacacheReader, askDisks)
	for i := range disks {
		r, w := io.Pipe()
		d := disks[i]
		readers[i], err = newMetacacheReader(r)
		if err != nil {
			return err
		}
		// Send request to each disk.
		go func() {
			werr := d.WalkDir(ctx, WalkDirOptions{
				Bucket:         opts.bucket,
				BaseDir:        opts.path,
				Recursive:      opts.recursive,
				ReportNotFound: opts.reportNotFound,
				FilterPrefix:   opts.filterPrefix,
				ForwardTo:      opts.forwardTo,
			}, w)
			w.CloseWithError(werr)
			if werr != io.EOF && werr != nil && werr.Error() != errFileNotFound.Error() && werr.Error() != errVolumeNotFound.Error() {
				logger.LogIf(ctx, werr)
			}
		}()
	}

	topEntries := make(metaCacheEntries, len(readers))
	errs := make([]error, len(readers))
	for {
		// Get the top entry from each
		var current metaCacheEntry
		var atEOF, fnf, hasErr, agree int
		for i := range topEntries {
			topEntries[i] = metaCacheEntry{}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for i, r := range readers {
			if errs[i] != nil {
				hasErr++
				continue
			}
			entry, err := r.peek()
			switch err {
			case io.EOF:
				atEOF++
				continue
			case nil:
			default:
				if err.Error() == errFileNotFound.Error() {
					atEOF++
					fnf++
					continue
				}
				if err.Error() == errVolumeNotFound.Error() {
					atEOF++
					fnf++
					continue
				}
				hasErr++
				errs[i] = err
				continue
			}
			// If no current, add it.
			if current.name == "" {
				topEntries[i] = entry
				current = entry
				agree++
				continue
			}
			// If exact match, we agree.
			if current.matches(&entry, opts.bucket) {
				topEntries[i] = entry
				agree++
				continue
			}
			// If only the name matches we didn't agree, but add it for resolution.
			if entry.name == current.name {
				topEntries[i] = entry
				continue
			}
			// We got different entries
			if entry.name > current.name {
				continue
			}
			// We got a new, better current.
			// Clear existing entries.
			for i := range topEntries[:i] {
				topEntries[i] = metaCacheEntry{}
			}
			agree = 1
			current = entry
			topEntries[i] = entry
		}

		// Stop if we exceed number of bad disks
		if hasErr > len(disks)-opts.minDisks && hasErr > 0 {
			if opts.finished != nil {
				opts.finished(errs)
			}
			var combinedErr []string
			for i, err := range errs {
				if err != nil {
					combinedErr = append(combinedErr, fmt.Sprintf("disk %d returned: %s", i, err))
				}
			}
			return errors.New(strings.Join(combinedErr, ", "))
		}

		// Break if all at EOF or error.
		if atEOF+hasErr == len(readers) {
			if hasErr > 0 && opts.finished != nil {
				opts.finished(errs)
			}
			break
		}
		if fnf == len(readers) {
			return errFileNotFound
		}
		if agree == len(readers) {
			// Everybody agreed
			for _, r := range readers {
				r.skip(1)
			}
			if opts.agreed != nil {
				opts.agreed(current)
			}
			continue
		}
		if opts.partial != nil {
			opts.partial(topEntries, agree, errs)
		}
		// Skip the inputs we used.
		for i, r := range readers {
			if topEntries[i].name != "" {
				r.skip(1)
			}
		}
	}
	return nil
}
