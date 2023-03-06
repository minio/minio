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
	"errors"
	"os"
	"runtime"
	"syscall"
)

// No space left on device error
func isSysErrNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

// Invalid argument, unsupported flags such as O_DIRECT
func isSysErrInvalidArg(err error) bool {
	return errors.Is(err, syscall.EINVAL)
}

// Input/output error
func isSysErrIO(err error) bool {
	return errors.Is(err, syscall.EIO)
}

// Check if the given error corresponds to EISDIR (is a directory).
func isSysErrIsDir(err error) bool {
	return errors.Is(err, syscall.EISDIR)
}

// Check if the given error corresponds to ENOTDIR (is not a directory).
func isSysErrNotDir(err error) bool {
	return errors.Is(err, syscall.ENOTDIR)
}

// Check if the given error corresponds to the ENAMETOOLONG (name too long).
func isSysErrTooLong(err error) bool {
	return errors.Is(err, syscall.ENAMETOOLONG)
}

// Check if the given error corresponds to the ELOOP (too many symlinks).
func isSysErrTooManySymlinks(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}

// Check if the given error corresponds to ENOTEMPTY for unix,
// EEXIST for solaris variants,
// and ERROR_DIR_NOT_EMPTY for windows (directory not empty).
func isSysErrNotEmpty(err error) bool {
	if errors.Is(err, syscall.ENOTEMPTY) {
		return true
	}
	if errors.Is(err, syscall.EEXIST) && runtime.GOOS == "solaris" {
		return true
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		if runtime.GOOS == globalWindowsOSName {
			var errno syscall.Errno
			if errors.As(pathErr.Err, &errno) {
				// ERROR_DIR_NOT_EMPTY
				return errno == 0x91
			}
		}
	}
	return false
}

// Check if the given error corresponds to the specific ERROR_PATH_NOT_FOUND for windows
func isSysErrPathNotFound(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return pathErr.Err == syscall.ENOENT
		}
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x03
		}
	}
	return false
}

// Check if the given error corresponds to the specific ERROR_INVALID_HANDLE for windows
func isSysErrHandleInvalid(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		return false
	}
	// Check if err contains ERROR_INVALID_HANDLE errno
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x6
		}
	}
	return false
}

func isSysErrCrossDevice(err error) bool {
	return errors.Is(err, syscall.EXDEV)
}

// Check if given error corresponds to too many open files
func isSysErrTooManyFiles(err error) bool {
	return errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.EMFILE)
}

func osIsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

func osIsPermission(err error) bool {
	return errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EROFS)
}

func osIsExist(err error) bool {
	return errors.Is(err, os.ErrExist)
}
