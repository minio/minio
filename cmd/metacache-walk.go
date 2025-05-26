// Copyright (c) 2015-2023 MinIO, Inc.
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
	"io"
	"sort"
	"strings"

	"github.com/minio/minio/internal/grid"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/valyala/bytebufferpool"
)

//go:generate msgp -file $GOFILE

// WalkDirOptions provides options for WalkDir operations.
type WalkDirOptions struct {
	// Bucket to scanner
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Do a full recursive scan.
	Recursive bool

	// ReportNotFound will return errFileNotFound if all disks reports the BaseDir cannot be found.
	ReportNotFound bool

	// FilterPrefix will only return results with given prefix within folder.
	// Should never contain a slash.
	FilterPrefix string

	// ForwardTo will forward to the given object path.
	ForwardTo string

	// Limit the number of returned objects if > 0.
	Limit int

	// DiskID contains the disk ID of the disk.
	// Leave empty to not check disk ID.
	DiskID string
}

// supported FS for Nlink optimization in readdir.
const (
	xfs  = "XFS"
	ext4 = "EXT4"
)

// WalkDir will traverse a directory and return all entries found.
// On success a sorted meta cache stream will be returned.
// Metadata has data stripped, if any.
// The function tries to quit as fast as the context is canceled to avoid further drive IO
func (s *xlStorage) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) (err error) {
	legacyFS := s.fsType != xfs && s.fsType != ext4

	s.RLock()
	legacy := s.formatLegacy
	s.RUnlock()

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(opts.Bucket)
	if err != nil {
		return err
	}

	if !skipAccessChecks(opts.Bucket) {
		// Stat a volume entry.
		if err = Access(volumeDir); err != nil {
			return convertAccessError(err, errVolumeAccessDenied)
		}
	}

	// Use a small block size to start sending quickly
	w := newMetacacheWriter(wr, 16<<10)
	w.reuseBlocks = true // We are not sharing results, so reuse buffers.
	defer w.Close()
	out, err := w.stream()
	if err != nil {
		return err
	}
	defer xioutil.SafeClose(out)
	var objsReturned int

	objReturned := func(metadata []byte) {
		if opts.Limit <= 0 {
			return
		}
		if m, _, _ := isIndexedMetaV2(metadata); m != nil && !m.AllHidden(true) {
			objsReturned++
		}
	}
	send := func(entry metaCacheEntry) error {
		objReturned(entry.metadata)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- entry:
		}
		return nil
	}

	// Fast exit track to check if we are listing an object with
	// a trailing slash, this will avoid to list the object content.
	if HasSuffix(opts.BaseDir, SlashSeparator) {
		metadata, err := s.readMetadata(ctx, pathJoin(volumeDir,
			opts.BaseDir[:len(opts.BaseDir)-1]+globalDirSuffix,
			xlStorageFormatFile))
		diskHealthCheckOK(ctx, err)
		if err == nil {
			// if baseDir is already a directory object, consider it
			// as part of the list call, this is AWS S3 specific
			// behavior.
			if err := send(metaCacheEntry{
				name:     opts.BaseDir,
				metadata: metadata,
			}); err != nil {
				return err
			}
		} else {
			st, sterr := Lstat(pathJoin(volumeDir, opts.BaseDir, xlStorageFormatFile))
			if sterr == nil && st.Mode().IsRegular() {
				return errFileNotFound
			}
		}
	}

	prefix := opts.FilterPrefix
	var scanDir func(path string) error

	scanDir = func(current string) error {
		if contextCanceled(ctx) {
			return ctx.Err()
		}
		if opts.Limit > 0 && objsReturned >= opts.Limit {
			return nil
		}

		// Skip forward, if requested...
		sb := bytebufferpool.Get()
		defer func() {
			sb.Reset()
			bytebufferpool.Put(sb)
		}()

		forward := ""
		if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, current) {
			forward = strings.TrimPrefix(opts.ForwardTo, current)
			// Trim further directories and trailing slash.
			if idx := strings.IndexByte(forward, '/'); idx > 0 {
				forward = forward[:idx]
			}
		}

		if s.walkMu != nil {
			s.walkMu.Lock()
		}
		entries, err := s.ListDir(ctx, "", opts.Bucket, current, -1)
		if s.walkMu != nil {
			s.walkMu.Unlock()
		}
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound && err != errFileNotFound {
				internalLogOnceIf(ctx, err, "metacache-walk-scan-dir")
			}
			if opts.ReportNotFound && err == errFileNotFound && current == opts.BaseDir {
				err = errFileNotFound
			} else {
				err = nil
			}
			diskHealthCheckOK(ctx, err)
			return err
		}
		diskHealthCheckOK(ctx, err)
		if len(entries) == 0 {
			return nil
		}
		dirObjects := make(map[string]struct{})

		// Avoid a bunch of cleanup when joining.
		current = strings.Trim(current, SlashSeparator)
		for i, entry := range entries {
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			if opts.Limit > 0 && objsReturned >= opts.Limit {
				return nil
			}
			if len(prefix) > 0 && !strings.HasPrefix(entry, prefix) {
				// Do not retain the file, since it doesn't
				// match the prefix.
				entries[i] = ""
				continue
			}
			if len(forward) > 0 && entry < forward {
				// Do not retain the file, since its
				// lexially smaller than 'forward'
				entries[i] = ""
				continue
			}
			if hasSuffixByte(entry, SlashSeparatorChar) {
				if strings.HasSuffix(entry, globalDirSuffixWithSlash) {
					// Add without extension so it is sorted correctly.
					entry = strings.TrimSuffix(entry, globalDirSuffixWithSlash) + slashSeparator
					dirObjects[entry] = struct{}{}
					entries[i] = entry
					continue
				}
				// Trim slash, since we don't know if this is folder or object.
				entries[i] = entries[i][:len(entry)-1]
				continue
			}
			// Do not retain the file.
			entries[i] = ""

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			if HasSuffix(entry, xlStorageFormatFile) {
				var meta metaCacheEntry
				if s.walkReadMu != nil {
					s.walkReadMu.Lock()
				}
				meta.metadata, err = s.readMetadata(ctx, pathJoinBuf(sb, volumeDir, current, entry))
				if s.walkReadMu != nil {
					s.walkReadMu.Unlock()
				}
				diskHealthCheckOK(ctx, err)
				if err != nil {
					// It is totally possible that xl.meta was overwritten
					// while being concurrently listed at the same time in
					// such scenarios the 'xl.meta' might get truncated
					if !IsErrIgnored(err, io.EOF, io.ErrUnexpectedEOF) {
						internalLogOnceIf(ctx, err, "metacache-walk-read-metadata")
					}
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFile)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoinBuf(sb, current, meta.name)
				meta.name = decodeDirObject(meta.name)

				return send(meta)
			}
			// Check legacy.
			if HasSuffix(entry, xlStorageFormatFileV1) && legacy {
				var meta metaCacheEntry
				meta.metadata, err = xioutil.ReadFile(pathJoinBuf(sb, volumeDir, current, entry))
				diskHealthCheckOK(ctx, err)
				if err != nil {
					if !IsErrIgnored(err, io.EOF, io.ErrUnexpectedEOF) {
						internalLogIf(ctx, err)
					}
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFileV1)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoinBuf(sb, current, meta.name)

				return send(meta)
			}
			// Skip all other files.
		}

		// Process in sort order.
		sort.Strings(entries)
		dirStack := make([]string, 0, 5)
		prefix = "" // Remove prefix after first level as we have already filtered the list.
		if len(forward) > 0 {
			// Conservative forwarding. Entries may be either objects or prefixes.
			for i, entry := range entries {
				if entry >= forward || strings.HasPrefix(forward, entry) {
					entries = entries[i:]
					break
				}
			}
		}

		for _, entry := range entries {
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			if opts.Limit > 0 && objsReturned >= opts.Limit {
				return nil
			}
			if entry == "" {
				continue
			}
			meta := metaCacheEntry{name: pathJoinBuf(sb, current, entry)}

			// If directory entry on stack before this, pop it now.
			for len(dirStack) > 0 && dirStack[len(dirStack)-1] < meta.name {
				pop := dirStack[len(dirStack)-1]
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- metaCacheEntry{name: pop}:
				}
				if opts.Recursive {
					// Scan folder we found. Should be in correct sort order where we are.
					err := scanDir(pop)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							return err
						}
						internalLogIf(ctx, err)
					}
				}
				dirStack = dirStack[:len(dirStack)-1]
			}

			// All objects will be returned as directories, there has been no object check yet.
			// Check it by attempting to read metadata.
			_, isDirObj := dirObjects[entry]
			if isDirObj {
				meta.name = meta.name[:len(meta.name)-1] + globalDirSuffixWithSlash
			}

			if s.walkReadMu != nil {
				s.walkReadMu.Lock()
			}
			meta.metadata, err = s.readMetadata(ctx, pathJoinBuf(sb, volumeDir, meta.name, xlStorageFormatFile))
			if s.walkReadMu != nil {
				s.walkReadMu.Unlock()
			}
			diskHealthCheckOK(ctx, err)
			switch {
			case err == nil:
				// It was an object
				if isDirObj {
					meta.name = strings.TrimSuffix(meta.name, globalDirSuffixWithSlash) + slashSeparator
				}
				if err := send(meta); err != nil {
					return err
				}
			case osIsNotExist(err), isSysErrIsDir(err):
				if legacy {
					meta.metadata, err = xioutil.ReadFile(pathJoinBuf(sb, volumeDir, meta.name, xlStorageFormatFileV1))
					diskHealthCheckOK(ctx, err)
					if err == nil {
						// It was an object
						if err := send(meta); err != nil {
							return err
						}
						continue
					}
				}

				// NOT an object, append to stack (with slash)
				// If dirObject, but no metadata (which is unexpected) we skip it.
				if !isDirObj {
					if !isDirEmpty(pathJoinBuf(sb, volumeDir, meta.name), legacyFS) {
						dirStack = append(dirStack, meta.name+slashSeparator)
					}
				}
			case isSysErrNotDir(err):
				// skip
			}
		}

		// If directory entry left on stack, pop it now.
		for len(dirStack) > 0 {
			if opts.Limit > 0 && objsReturned >= opts.Limit {
				return nil
			}
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			pop := dirStack[len(dirStack)-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- metaCacheEntry{name: pop}:
			}
			if opts.Recursive {
				// Scan folder we found. Should be in correct sort order where we are.
				internalLogIf(ctx, scanDir(pop))
			}
			dirStack = dirStack[:len(dirStack)-1]
		}
		return nil
	}

	// Stream output.
	return scanDir(opts.BaseDir)
}

func (p *xlStorageDiskIDCheck) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) (err error) {
	if err := p.checkID(opts.DiskID); err != nil {
		return err
	}
	ctx, done, err := p.TrackDiskHealth(ctx, storageMetricWalkDir, opts.Bucket, opts.BaseDir)
	if err != nil {
		return err
	}
	defer done(0, &err)

	return p.storage.WalkDir(ctx, opts, wr)
}

// WalkDir will traverse a directory and return all entries found.
// On success a meta cache stream will be returned, that should be closed when done.
func (client *storageRESTClient) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	// Ensure remote has the same disk ID.
	opts.DiskID = *client.diskID.Load()
	b, err := opts.MarshalMsg(grid.GetByteBuffer()[:0])
	if err != nil {
		return toStorageErr(err)
	}

	st, err := client.gridConn.NewStream(ctx, grid.HandlerWalkDir, b)
	if err != nil {
		return toStorageErr(err)
	}
	return toStorageErr(st.Results(func(in []byte) error {
		_, err := wr.Write(in)
		return err
	}))
}

// WalkDirHandler - remote caller to list files and folders in a requested directory path.
func (s *storageRESTServer) WalkDirHandler(ctx context.Context, payload []byte, _ <-chan []byte, out chan<- []byte) (gerr *grid.RemoteErr) {
	var opts WalkDirOptions
	_, err := opts.UnmarshalMsg(payload)
	if err != nil {
		return grid.NewRemoteErr(err)
	}

	if !s.checkID(opts.DiskID) {
		return grid.NewRemoteErr(errDiskNotFound)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return grid.NewRemoteErr(s.getStorage().WalkDir(ctx, opts, grid.WriterToChannel(ctx, out)))
}
