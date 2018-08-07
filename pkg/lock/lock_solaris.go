// +build solaris

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package lock

import (
	"fmt"
	"os"
	"syscall"
)

// lockedOpenFile is an internal function.
func lockedOpenFile(path string, flag int, perm os.FileMode, rlockType int) (*LockedFile, error) {
	var lockType int16
	switch flag {
	case syscall.O_RDONLY:
		lockType = syscall.F_RDLCK
	case syscall.O_WRONLY:
		fallthrough
	case syscall.O_RDWR:
		fallthrough
	case syscall.O_WRONLY | syscall.O_CREAT:
		fallthrough
	case syscall.O_RDWR | syscall.O_CREAT:
		lockType = syscall.F_WRLCK
	default:
		return nil, fmt.Errorf("Unsupported flag (%d)", flag)
	}

	var lock = syscall.Flock_t{
		Start:  0,
		Len:    0,
		Pid:    0,
		Type:   lockType,
		Whence: 0,
	}

	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	if err = syscall.FcntlFlock(f.Fd(), rlockType, &lock); err != nil {
		f.Close()
		if err == syscall.EAGAIN {
			err = ErrAlreadyLocked
		}
		return nil, err
	}

	st, err := os.Stat(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	if st.IsDir() {
		f.Close()
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  syscall.EISDIR,
		}
	}

	return &LockedFile{f}, nil
}

// TryLockedOpenFile - tries a new write lock, functionality
// it is similar to LockedOpenFile with with syscall.LOCK_EX
// mode but along with syscall.LOCK_NB such that the function
// doesn't wait forever but instead returns if it cannot
// acquire a write lock.
func TryLockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, syscall.F_SETLK)
}

// LockedOpenFile - initializes a new lock and protects
// the file from concurrent access across mount points.
// This implementation doesn't support all the open
// flags and shouldn't be considered as replacement
// for os.OpenFile().
func LockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, syscall.F_SETLKW)
}

// Open - Call os.OpenFile
func Open(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
