// +build windows

/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"path"
	"strings"
	"syscall"
)

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirN(dirPath, -1)
}

// Return N entries at the directory dirPath. If count is -1, return all entries
func readDirN(dirPath string, count int) (entries []string, err error) {
	d, err := os.Open(dirPath)
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return nil, errFileNotFound
		}
		return nil, err
	}
	defer d.Close()

	st, err := d.Stat()
	if err != nil {
		return nil, err
	}
	// Not a directory return error.
	if !st.IsDir() {
		return nil, errFileAccessDenied
	}

	data := &syscall.Win32finddata{}

	remaining := count
	done := false
	for !done {
		e := syscall.FindNextFile(syscall.Handle(d.Fd()), data)
		if e != nil {
			if e == syscall.ERROR_NO_MORE_FILES {
				break
			} else {
				err = &os.PathError{
					Op:   "FindNextFile",
					Path: dirPath,
					Err:  e,
				}
				return
			}
		}
		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "." || name == ".." { // Useless names
			continue
		}
		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// If its symbolic link, follow the link using os.Stat()
			var fi os.FileInfo
			fi, err = os.Stat(path.Join(dirPath, name))
			if err != nil {
				// If file does not exist, we continue and skip it.
				// Could happen if it was deleted in the middle while
				// this list was being performed.
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
			if fi.IsDir() {
				entries = append(entries, name+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, name)
			}
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			entries = append(entries, name+slashSeparator)
		default:
			entries = append(entries, name)
		}
		if remaining > 0 {
			remaining--
			done = remaining == 0
		}
	}
	return entries, nil
}
