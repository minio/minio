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
	"os"
	pathutil "path"
	"sync"

	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
)

// fsIOPool represents a protected list to keep track of all
// the concurrent readers at a given path.
type fsIOPool struct {
	sync.Mutex
	readersMap map[string]*lock.RLockedFile
}

// lookupToRead - looks up an fd from readers map and
// returns read locked fd for caller to read from, if
// fd found increments the reference count. If the fd
// is found to be closed then purges it from the
// readersMap and returns nil instead.
//
// NOTE: this function is not protected and it is callers
// responsibility to lock this call to be thread safe. For
// implementation ideas look at the usage inside Open() call.
func (fsi *fsIOPool) lookupToRead(path string) (*lock.RLockedFile, bool) {
	rlkFile, ok := fsi.readersMap[path]
	// File reference exists on map, validate if its
	// really closed and we are safe to purge it.
	if ok && rlkFile != nil {
		// If the file is closed and not removed from map is a bug.
		if rlkFile.IsClosed() {
			// Log this as an error.
			reqInfo := (&logger.ReqInfo{}).AppendTags("path", path)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, errUnexpected)

			// Purge the cached lock path from map.
			delete(fsi.readersMap, path)

			// Indicate that we can populate the new fd.
			ok = false
		} else {
			// Increment the lock ref, since the file is not closed yet
			// and caller requested to read the file again.
			rlkFile.IncLockRef()
		}
	}
	return rlkFile, ok
}

// Open is a wrapper call to read locked file which
// returns a ReadAtCloser.
//
// ReaderAt is provided so that the fd is non seekable, since
// we are sharing fd's with concurrent threads, we don't want
// all readers to change offsets on each other during such
// concurrent operations. Using ReadAt allows us to read from
// any offsets.
//
// Closer is implemented to track total readers and to close
// only when there no more readers, the fd is purged if the lock
// count has reached zero.
func (fsi *fsIOPool) Open(path string) (*lock.RLockedFile, error) {
	if err := checkPathLength(path); err != nil {
		return nil, err
	}

	fsi.Lock()
	rlkFile, ok := fsi.lookupToRead(path)
	fsi.Unlock()
	// Locked path reference doesn't exist, acquire a read lock again on the file.
	if !ok {
		// Open file for reading with read lock.
		newRlkFile, err := lock.RLockedOpenFile(path)
		if err != nil {
			switch {
			case osIsNotExist(err):
				return nil, errFileNotFound
			case osIsPermission(err):
				return nil, errFileAccessDenied
			case isSysErrIsDir(err):
				return nil, errIsNotRegular
			case isSysErrNotDir(err):
				return nil, errFileAccessDenied
			case isSysErrPathNotFound(err):
				return nil, errFileNotFound
			default:
				return nil, err
			}
		}

		/// Save new reader on the map.

		// It is possible by this time due to concurrent
		// i/o we might have another lock present. Lookup
		// again to check for such a possibility. If no such
		// file exists save the newly opened fd, if not
		// reuse the existing fd and close the newly opened
		// file
		fsi.Lock()
		rlkFile, ok = fsi.lookupToRead(path)
		if ok {
			// Close the new fd, since we already seem to have
			// an active reference.
			newRlkFile.Close()
		} else {
			// Save the new rlk file.
			rlkFile = newRlkFile
		}

		// Save the new fd on the map.
		fsi.readersMap[path] = rlkFile
		fsi.Unlock()

	}

	// Success.
	return rlkFile, nil
}

// Write - Attempt to lock the file if it exists,
// - if the file exists. Then we try to get a write lock this
//   will block if we can't get a lock perhaps another write
//   or read is in progress. Concurrent calls are protected
//   by the global namspace lock within the same process.
func (fsi *fsIOPool) Write(path string) (wlk *lock.LockedFile, err error) {
	if err = checkPathLength(path); err != nil {
		return nil, err
	}

	wlk, err = lock.LockedOpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			return nil, errFileNotFound
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		default:
			if isSysErrPathNotFound(err) {
				return nil, errFileNotFound
			}
			return nil, err
		}
	}
	return wlk, nil
}

// Create - creates a new write locked file instance.
// - if the file doesn't exist. We create the file and hold lock.
func (fsi *fsIOPool) Create(path string) (wlk *lock.LockedFile, err error) {
	if err = checkPathLength(path); err != nil {
		return nil, err
	}

	// Creates parent if missing.
	if err = mkdirAll(pathutil.Dir(path), 0777); err != nil {
		return nil, err
	}

	// Attempt to create the file.
	wlk, err = lock.LockedOpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		switch {
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case isSysErrPathNotFound(err):
			return nil, errFileAccessDenied
		default:
			return nil, err
		}
	}

	// Success.
	return wlk, nil
}

// Close implements closing the path referenced by the reader in such
// a way that it makes sure to remove entry from the map immediately
// if no active readers are present.
func (fsi *fsIOPool) Close(path string) error {
	fsi.Lock()
	defer fsi.Unlock()

	if err := checkPathLength(path); err != nil {
		return err
	}

	// Pop readers from path.
	rlkFile, ok := fsi.readersMap[path]
	if !ok {
		return nil
	}

	// Close the reader.
	rlkFile.Close()

	// If the file is closed, remove it from the reader pool map.
	if rlkFile.IsClosed() {

		// Purge the cached lock path from map.
		delete(fsi.readersMap, path)
	}

	// Success.
	return nil
}
