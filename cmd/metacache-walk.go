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
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

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
}

// WalkDir will traverse a directory and return all entries found.
// On success a sorted meta cache stream will be returned.
// Metadata has data stripped, if any.
func (s *xlStorage) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) (err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(opts.Bucket)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	_, err = os.Lstat(volumeDir)
	if err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Use a small block size to start sending quickly
	w := newMetacacheWriter(wr, 16<<10)
	defer w.Close()
	out, err := w.stream()
	if err != nil {
		return err
	}
	defer close(out)

	// Fast exit track to check if we are listing an object with
	// a trailing slash, this will avoid to list the object content.
	if HasSuffix(opts.BaseDir, SlashSeparator) {
		metadata, err := s.readMetadata(pathJoin(volumeDir,
			opts.BaseDir[:len(opts.BaseDir)-1]+globalDirSuffix,
			xlStorageFormatFile))
		if err == nil {
			// if baseDir is already a directory object, consider it
			// as part of the list call, this is a AWS S3 specific
			// behavior.
			out <- metaCacheEntry{
				name:     opts.BaseDir,
				metadata: metadata,
			}
		} else {
			if st, err := Lstat(pathJoin(volumeDir, opts.BaseDir, xlStorageFormatFile)); err == nil && st.Mode().IsRegular() {
				return errFileNotFound
			}
		}
	}

	prefix := opts.FilterPrefix
	forward := opts.ForwardTo
	var scanDir func(path string) error
	scanDir = func(current string) error {
		if contextCanceled(ctx) {
			return ctx.Err()
		}
		entries, err := s.ListDir(ctx, opts.Bucket, current, -1)
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound && err != errFileNotFound {
				logger.LogIf(ctx, err)
			}
			if opts.ReportNotFound && err == errFileNotFound && current == opts.BaseDir {
				return errFileNotFound
			}
			// Forward some errors?
			return nil
		}
		dirObjects := make(map[string]struct{})
		for i, entry := range entries {
			if len(prefix) > 0 && !strings.HasPrefix(entry, prefix) {
				continue
			}
			if len(forward) > 0 && entry < forward {
				continue
			}
			if strings.HasSuffix(entry, slashSeparator) {
				if strings.HasSuffix(entry, globalDirSuffixWithSlash) {
					// Add without extension so it is sorted correctly.
					entry = strings.TrimSuffix(entry, globalDirSuffixWithSlash) + slashSeparator
					dirObjects[entry] = struct{}{}
					entries[i] = entry
					continue
				}
				// Trim slash, maybe compiler is clever?
				entries[i] = entries[i][:len(entry)-1]
				continue
			}
			// Do do not retain the file.
			entries[i] = ""

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			if HasSuffix(entry, xlStorageFormatFile) {
				var meta metaCacheEntry
				meta.metadata, err = s.readMetadata(pathJoin(volumeDir, current, entry))
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFile)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoin(current, meta.name)
				meta.name = decodeDirObject(meta.name)
				out <- meta
				return nil
			}
			// Check legacy.
			if HasSuffix(entry, xlStorageFormatFileV1) {
				var meta metaCacheEntry
				meta.metadata, err = xioutil.ReadFile(pathJoin(volumeDir, current, entry))
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}
				meta.name = strings.TrimSuffix(entry, xlStorageFormatFileV1)
				meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
				meta.name = pathJoin(current, meta.name)
				out <- meta
				return nil
			}
			// Skip all other files.
		}

		// Process in sort order.
		sort.Strings(entries)
		dirStack := make([]string, 0, 5)
		prefix = "" // Remove prefix after first level.

		for _, entry := range entries {
			if entry == "" {
				continue
			}
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			meta := metaCacheEntry{name: PathJoin(current, entry)}

			// If directory entry on stack before this, pop it now.
			for len(dirStack) > 0 && dirStack[len(dirStack)-1] < meta.name {
				pop := dirStack[len(dirStack)-1]
				out <- metaCacheEntry{name: pop}
				if opts.Recursive {
					// Scan folder we found. Should be in correct sort order where we are.
					forward = ""
					if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, pop) {
						forward = strings.TrimPrefix(opts.ForwardTo, pop)
					}
					logger.LogIf(ctx, scanDir(pop))
				}
				dirStack = dirStack[:len(dirStack)-1]
			}

			// All objects will be returned as directories, there has been no object check yet.
			// Check it by attempting to read metadata.
			_, isDirObj := dirObjects[entry]
			if isDirObj {
				meta.name = meta.name[:len(meta.name)-1] + globalDirSuffixWithSlash
			}

			meta.metadata, err = s.readMetadata(pathJoin(volumeDir, meta.name, xlStorageFormatFile))
			switch {
			case err == nil:
				// It was an object
				if isDirObj {
					meta.name = strings.TrimSuffix(meta.name, globalDirSuffixWithSlash) + slashSeparator
				}
				out <- meta
			case osIsNotExist(err):
				meta.metadata, err = xioutil.ReadFile(pathJoin(volumeDir, meta.name, xlStorageFormatFileV1))
				if err == nil {
					// Maybe rename? Would make it inconsistent across disks though.
					// os.Rename(pathJoin(volumeDir, meta.name, xlStorageFormatFileV1), pathJoin(volumeDir, meta.name, xlStorageFormatFile))
					// It was an object
					out <- meta
					continue
				}

				// NOT an object, append to stack (with slash)
				// If dirObject, but no metadata (which is unexpected) we skip it.
				if !isDirObj {
					if !isDirEmpty(pathJoin(volumeDir, meta.name+slashSeparator)) {
						dirStack = append(dirStack, meta.name+slashSeparator)
					}
				}
			case isSysErrNotDir(err):
				// skip
			default:
				logger.LogIf(ctx, err)
			}
		}
		// If directory entry left on stack, pop it now.
		for len(dirStack) > 0 {
			pop := dirStack[len(dirStack)-1]
			out <- metaCacheEntry{name: pop}
			if opts.Recursive {
				// Scan folder we found. Should be in correct sort order where we are.
				forward = ""
				if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, pop) {
					forward = strings.TrimPrefix(opts.ForwardTo, pop)
				}
				logger.LogIf(ctx, scanDir(pop))
			}
			dirStack = dirStack[:len(dirStack)-1]
		}
		return nil
	}

	// Stream output.
	return scanDir(opts.BaseDir)
}

func (p *xlStorageDiskIDCheck) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	defer p.updateStorageMetrics(storageMetricWalkDir, opts.Bucket, opts.BaseDir)()
	if err := p.checkDiskStale(); err != nil {
		return err
	}
	return p.storage.WalkDir(ctx, opts, wr)
}

// WalkDir will traverse a directory and return all entries found.
// On success a meta cache stream will be returned, that should be closed when done.
func (client *storageRESTClient) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, opts.Bucket)
	values.Set(storageRESTDirPath, opts.BaseDir)
	values.Set(storageRESTRecursive, strconv.FormatBool(opts.Recursive))
	values.Set(storageRESTReportNotFound, strconv.FormatBool(opts.ReportNotFound))
	values.Set(storageRESTPrefixFilter, opts.FilterPrefix)
	values.Set(storageRESTForwardFilter, opts.ForwardTo)
	respBody, err := client.call(ctx, storageRESTMethodWalkDir, values, nil, -1)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer xhttp.DrainBody(respBody)
	return waitForHTTPStream(respBody, wr)
}

// WalkDirHandler - remote caller to list files and folders in a requested directory path.
func (s *storageRESTServer) WalkDirHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	dirPath := vars[storageRESTDirPath]
	recursive, err := strconv.ParseBool(vars[storageRESTRecursive])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	var reportNotFound bool
	if v := vars[storageRESTReportNotFound]; v != "" {
		reportNotFound, err = strconv.ParseBool(v)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
	}

	prefix := r.URL.Query().Get(storageRESTPrefixFilter)
	forward := r.URL.Query().Get(storageRESTForwardFilter)
	writer := streamHTTPResponse(w)
	writer.CloseWithError(s.storage.WalkDir(r.Context(), WalkDirOptions{
		Bucket:         volume,
		BaseDir:        dirPath,
		Recursive:      recursive,
		ReportNotFound: reportNotFound,
		FilterPrefix:   prefix,
		ForwardTo:      forward,
	}, writer))
}
