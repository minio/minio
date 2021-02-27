// +build windows

/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"io"
	"os"
	"syscall"
)

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
