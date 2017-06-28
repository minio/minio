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

// Package lock - implements filesystem locking wrappers around an
// open file descriptor.
package lock

import (
	"errors"
	"os"
	"sync"
)

var (
	// ErrAlreadyLocked is returned if the underlying fd is already locked.
	ErrAlreadyLocked = errors.New("file already locked")
)

// RLockedFile represents a read locked file, implements a special
// closer which only closes the associated *os.File when the ref count.
// has reached zero, i.e when all the readers have given up their locks.
type RLockedFile struct {
	*LockedFile
	mutex sync.Mutex
	refs  int // Holds read lock refs.
}

// IsClosed - Check if the rlocked file is already closed.
func (r *RLockedFile) IsClosed() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.refs == 0
}

// IncLockRef - is used by called to indicate lock refs.
func (r *RLockedFile) IncLockRef() {
	r.mutex.Lock()
	r.refs++
	r.mutex.Unlock()
}

// Close - this closer implements a special closer
// closes the underlying fd only when the refs
// reach zero.
func (r *RLockedFile) Close() (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.refs == 0 {
		return os.ErrInvalid
	}

	r.refs--
	if r.refs == 0 {
		err = r.File.Close()
	}

	return err
}

// Provides a new initialized read locked struct from *os.File
func newRLockedFile(lkFile *LockedFile) (*RLockedFile, error) {
	if lkFile == nil {
		return nil, os.ErrInvalid
	}

	return &RLockedFile{
		LockedFile: lkFile,
		refs:       1,
	}, nil
}

// RLockedOpenFile - returns a wrapped read locked file, if the file
// doesn't exist at path returns an error.
func RLockedOpenFile(path string) (*RLockedFile, error) {
	lkFile, err := LockedOpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	return newRLockedFile(lkFile)

}

// LockedFile represents a locked file
type LockedFile struct {
	*os.File
}
