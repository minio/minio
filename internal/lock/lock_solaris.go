// +build solaris

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

package lock

import (
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
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  syscall.EINVAL,
		}
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
