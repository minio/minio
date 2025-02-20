//go:build windows
// +build windows

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
	"path/filepath"
	"syscall"
)

func access(name string) error {
	_, err := os.Lstat(name)
	return err
}

func osMkdirAll(dirPath string, perm os.FileMode, _ string) error {
	// baseDir is not honored in windows platform
	return os.MkdirAll(dirPath, perm)
}

// readDirFn applies the fn() function on each entries at dirPath, doesn't recurse into
// the directory itself, if the dirPath doesn't exist this function doesn't return
// an error.
func readDirFn(dirPath string, filter func(name string, typ os.FileMode) error) error {
	// Ensure we don't pick up files as directories.
	globAll := filepath.Clean(dirPath) + `\*`
	globAllP, err := syscall.UTF16PtrFromString(globAll)
	if err != nil {
		return errInvalidArgument
	}
	data := &syscall.Win32finddata{}
	handle, err := syscall.FindFirstFile(globAllP, data)
	if err != nil {
		if err = syscallErrToFileErr(dirPath, err); err == errFileNotFound {
			return nil
		}
		return err
	}
	defer syscall.FindClose(handle)

	for ; ; err = syscall.FindNextFile(handle, data) {
		if err != nil {
			if err == syscall.ERROR_NO_MORE_FILES {
				break
			}
			if isSysErrPathNotFound(err) {
				return nil
			}
			err = osErrToFileErr(&os.PathError{
				Op:   "FindNextFile",
				Path: dirPath,
				Err:  err,
			})
			if err == errFileNotFound {
				return nil
			}
			return err
		}
		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "" || name == "." || name == ".." { // Useless names
			continue
		}

		var typ os.FileMode // regular file
		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// Reparse point is a symlink
			fi, err := os.Stat(pathJoin(dirPath, name))
			if err != nil {
				// It got deleted in the meantime, not found
				// or returns too many symlinks ignore this
				// file/directory.
				if osIsNotExist(err) || isSysErrPathNotFound(err) ||
					isSysErrTooManySymlinks(err) {
					continue
				}
				return err
			}

			if fi.IsDir() {
				// Ignore symlinked directories.
				continue
			}

			typ = fi.Mode()
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			typ = os.ModeDir
		}

		if err = filter(name, typ); err == errDoneForNow {
			// filtering requested to return by caller.
			return nil
		}
	}

	return nil
}

// Return N entries at the directory dirPath.
func readDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	// Ensure we don't pick up files as directories.
	globAll := filepath.Clean(dirPath) + `\*`
	globAllP, err := syscall.UTF16PtrFromString(globAll)
	if err != nil {
		return nil, errInvalidArgument
	}
	data := &syscall.Win32finddata{}
	handle, err := syscall.FindFirstFile(globAllP, data)
	if err != nil {
		return nil, syscallErrToFileErr(dirPath, err)
	}

	defer syscall.FindClose(handle)

	count := opts.count
	for ; count != 0; err = syscall.FindNextFile(handle, data) {
		if err != nil {
			if err == syscall.ERROR_NO_MORE_FILES {
				break
			}
			return nil, osErrToFileErr(&os.PathError{
				Op:   "FindNextFile",
				Path: dirPath,
				Err:  err,
			})
		}

		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "" || name == "." || name == ".." { // Useless names
			continue
		}

		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// Reparse point is a symlink
			fi, err := os.Stat(pathJoin(dirPath, name))
			if err != nil {
				// It got deleted in the meantime, not found
				// or returns too many symlinks ignore this
				// file/directory.
				if osIsNotExist(err) || isSysErrPathNotFound(err) ||
					isSysErrTooManySymlinks(err) {
					continue
				}
				return nil, err
			}

			if !opts.followDirSymlink && fi.IsDir() {
				// directory symlinks are ignored.
				continue
			}
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			name += SlashSeparator
		}

		count--
		entries = append(entries, name)
	}

	return entries, nil
}

func globalSync() {
	// no-op on windows
}

func syscallErrToFileErr(dirPath string, err error) error {
	switch err {
	case nil:
		return nil
	case syscall.ERROR_FILE_NOT_FOUND:
		return errFileNotFound
	case syscall.ERROR_ACCESS_DENIED:
		return errFileAccessDenied
	default:
		// Fails on file not found and when not a directory.
		return osErrToFileErr(&os.PathError{
			Op:   "FindNextFile",
			Path: dirPath,
			Err:  err,
		})
	}
}
