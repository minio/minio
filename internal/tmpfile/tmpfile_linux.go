// Copyright (c) 2015-2024 MinIO, Inc.
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

//go:build linux
// +build linux

package tmpfile

import (
	"errors"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var notmpfile atomic.Bool

// TempFile creates a new temporary file in the directory dir, opens
// the file for reading and writing, and returns the resulting
// *os.File.
//
// Multiple programs calling TempFile simultaneously will not choose
// the same file.
//
// If remove is true, it is the caller's responsibility to remove the
// file when no longer needed. In that case, the caller can use
// f.Name() to find the pathname of the file. This will be true, if
// the kernel or filesystem does not support O_TMPFILE. In this case,
// os.CreateTemp is used as a fallback,
func TempFile(dir string, mode int) (f *os.File, remove bool, err error) {
	if dir == "" {
		return nil, false, errors.New("dir cannot be empty")
	}

	if notmpfile.Load() {
		f, err := os.CreateTemp(dir, "")
		return f, err == nil, err
	}

	fd, err := unix.Open(dir, unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC|mode, 0o600)

	switch err {
	case nil:
	case syscall.EISDIR:
		notmpfile.Store(true)
		fallthrough
	case syscall.EOPNOTSUPP:
		f, err := os.CreateTemp(dir, "")
		return f, err == nil, err
	default:
		return nil, false, &os.PathError{
			Op:   "open",
			Path: dir,
			Err:  err,
		}
	}

	path := "/proc/self/fd/" + strconv.FormatUint(uint64(fd), 10)
	return os.NewFile(uintptr(fd), path), false, nil
}

// Link links the *os.File returned by TempFile into the filesystem
// at the given path, making it permanent.
//
// If TempFile was forced to fallback to os.CreateTemp, this calls
// os.Rename with the file path.
//
// If f was not returned by TempFile, the behavior of Link is
// undefined.
func Link(f *os.File, newpath string) error {
	if notmpfile.Load() {
		return os.Rename(f.Name(), newpath)
	}

	r0, _, errno := unix.Syscall(unix.SYS_FCNTL, f.Fd(), unix.F_GETFL, 0)
	if errno != 0 {
		return &os.LinkError{
			Op:  "link",
			Old: f.Name(),
			New: newpath,
			Err: errno,
		}
	}

	if r0&unix.O_TMPFILE != unix.O_TMPFILE {
		return os.Rename(f.Name(), newpath)
	}

	err := unix.Linkat(unix.AT_FDCWD, f.Name(), unix.AT_FDCWD, newpath,
		unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		return &os.LinkError{
			Op:  "link",
			Old: f.Name(),
			New: newpath,
			Err: err,
		}
	}

	runtime.KeepAlive(f)
	return nil
}
