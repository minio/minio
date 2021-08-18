//go:build !windows && !plan9 && !solaris
// +build !windows,!plan9,!solaris

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

// Internal function implements support for both
// blocking and non blocking lock type.
func lockedOpenFile(path string, flag int, perm os.FileMode, lockType int) (*LockedFile, error) {
	switch flag {
	case syscall.O_RDONLY:
		lockType |= syscall.LOCK_SH
	case syscall.O_WRONLY:
		fallthrough
	case syscall.O_RDWR:
		fallthrough
	case syscall.O_WRONLY | syscall.O_CREAT:
		fallthrough
	case syscall.O_RDWR | syscall.O_CREAT:
		lockType |= syscall.LOCK_EX
	default:
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  syscall.EINVAL,
		}
	}

	f, err := os.OpenFile(path, flag|syscall.O_SYNC, perm)
	if err != nil {
		return nil, err
	}

	if err = syscall.Flock(int(f.Fd()), lockType); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
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

	return &LockedFile{File: f}, nil
}

// TryLockedOpenFile - tries a new write lock, functionality
// it is similar to LockedOpenFile with with syscall.LOCK_EX
// mode but along with syscall.LOCK_NB such that the function
// doesn't wait forever but instead returns if it cannot
// acquire a write lock.
func TryLockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, syscall.LOCK_NB)
}

// LockedOpenFile - initializes a new lock and protects
// the file from concurrent access across mount points.
// This implementation doesn't support all the open
// flags and shouldn't be considered as replacement
// for os.OpenFile().
func LockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, 0)
}

// Open - Call os.OpenFile
func Open(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
