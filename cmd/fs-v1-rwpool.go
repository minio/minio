/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"os"
	pathutil "path"
	"sync"

	"github.com/minio/minio/pkg/lock"
)

// fsIOPool represents a protected list to keep track of all
// the concurrent readers at a given path.
type fsIOPool struct {
	sync.Mutex
	readersMap map[string]*lock.RLockedFile
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
	rlkFile, ok := fsi.readersMap[path]
	// File reference exists on map, validate if its
	// really closed and we are safe to purge it.
	if ok && rlkFile != nil {
		// If the file is closed and not removed from map is a bug.
		if rlkFile.IsClosed() {
			// Log this as an error.
			errorIf(errUnexpected, "Unexpected entry found on the map %s", path)

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
	fsi.Unlock()

	// Locked path reference doesn't exist, freshly open the file in
	// read lock mode.
	if !ok {
		var err error
		var newRlkFile *lock.RLockedFile
		// Open file for reading.
		newRlkFile, err = lock.RLockedOpenFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, errFileNotFound
			} else if os.IsPermission(err) {
				return nil, errFileAccessDenied
			} else if isSysErrIsDir(err) {
				return nil, errIsNotRegular
			} else if isSysErrNotDir(err) {
				return nil, errFileAccessDenied
			} else if isSysErrPathNotFound(err) {
				return nil, errFileNotFound
			}
			return nil, err
		}

		// Save new reader on the map.
		fsi.Lock()
		rlkFile, ok = fsi.readersMap[path]
		if ok && rlkFile != nil {
			// If the file is closed and not removed from map is a bug.
			if rlkFile.IsClosed() {
				// Log this as an error.
				errorIf(errUnexpected, "Unexpected entry found on the map %s", path)

				// Purge the cached lock path from map.
				delete(fsi.readersMap, path)

				// Save the newly acquired read locked file.
				rlkFile = newRlkFile
			} else {
				// Increment the lock ref, since the file is not closed yet
				// and caller requested to read the file again.
				rlkFile.IncLockRef()
				newRlkFile.Close()
			}
		} else {
			// Save the newly acquired read locked file.
			rlkFile = newRlkFile
		}

		// Save the rlkFile back on the map.
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
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		} else if os.IsPermission(err) {
			return nil, errFileAccessDenied
		} else if isSysErrIsDir(err) {
			return nil, errIsNotRegular
		}
		return nil, err
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
	if err = os.MkdirAll(pathutil.Dir(path), 0777); err != nil {
		if os.IsPermission(err) {
			return nil, errFileAccessDenied
		} else if isSysErrNotDir(err) {
			return nil, errFileAccessDenied
		}
		return nil, err
	}

	// Attempt to create the file.
	wlk, err = lock.LockedOpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		if os.IsPermission(err) {
			return nil, errFileAccessDenied
		} else if isSysErrIsDir(err) {
			return nil, errIsNotRegular
		} else if isSysErrPathNotFound(err) {
			return nil, errFileAccessDenied
		}
		return nil, err
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
