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
	"io"
	"os"
	"syscall"
)

func access(name string) error {
	_, err := os.Lstat(name)
	return err
}

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirN(dirPath, -1)
}

// readDirFn applies the fn() function on each entries at dirPath, doesn't recurse into
// the directory itself, if the dirPath doesn't exist this function doesn't return
// an error.
func readDirFn(dirPath string, filter func(name string, typ os.FileMode) error) error {
	f, err := os.Open(dirPath)
	if err != nil {
		if osErrToFileErr(err) == errFileNotFound {
			return nil
		}
		return osErrToFileErr(err)
	}
	defer f.Close()

	// Check if file or dir. This is the quickest way.
	// Do not remove this check, on windows syscall.FindNextFile
	// would throw an exception if Fd() points to a file
	// instead of a directory, we need to quickly fail
	// in such situations - this workadound is expected.
	if _, err = f.Seek(0, io.SeekStart); err == nil {
		return errFileNotFound
	}

	data := &syscall.Win32finddata{}
	for {
		e := syscall.FindNextFile(syscall.Handle(f.Fd()), data)
		if e != nil {
			if e == syscall.ERROR_NO_MORE_FILES {
				break
			} else {
				if isSysErrPathNotFound(e) {
					return nil
				}
				err = osErrToFileErr(&os.PathError{
					Op:   "FindNextFile",
					Path: dirPath,
					Err:  e,
				})
				if err == errFileNotFound {
					return nil
				}
				return err
			}
		}
		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "" || name == "." || name == ".." { // Useless names
			continue
		}

		var typ os.FileMode = 0 // regular file
		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// Reparse point is a symlink
			fi, err := os.Stat(pathJoin(dirPath, string(name)))
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

		if e = filter(name, typ); e == errDoneForNow {
			// filtering requested to return by caller.
			return nil
		}
	}

	return nil
}

// Return N entries at the directory dirPath. If count is -1, return all entries
func readDirN(dirPath string, count int) (entries []string, err error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer f.Close()

	// Check if file or dir. This is the quickest way.
	// Do not remove this check, on windows syscall.FindNextFile
	// would throw an exception if Fd() points to a file
	// instead of a directory, we need to quickly fail
	// in such situations - this workadound is expected.
	if _, err = f.Seek(0, io.SeekStart); err == nil {
		return nil, errFileNotFound
	}

	data := &syscall.Win32finddata{}
	handle := syscall.Handle(f.Fd())

	for count != 0 {
		e := syscall.FindNextFile(handle, data)
		if e != nil {
			if e == syscall.ERROR_NO_MORE_FILES {
				break
			} else {
				return nil, osErrToFileErr(&os.PathError{
					Op:   "FindNextFile",
					Path: dirPath,
					Err:  e,
				})
			}
		}

		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "" || name == "." || name == ".." { // Useless names
			continue
		}

		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// Reparse point is a symlink
			fi, err := os.Stat(pathJoin(dirPath, string(name)))
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

			if fi.IsDir() {
				// directory symlinks are ignored.
				continue
			}
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			name = name + SlashSeparator
		}

		count--
		entries = append(entries, name)

	}

	return entries, nil
}

func globalSync() {
	// no-op on windows
}
