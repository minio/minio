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
	"os"
	"syscall"
)

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirN(dirPath, -1)
}

// readDir applies the filter function on each entries at dirPath, doesn't recurse into
// the directory itself.
func readDirFilterFn(dirPath string, filter func(name string, typ os.FileMode) error) error {
	f, err := os.Open(dirPath)
	if err != nil {
		return osErrToFileErr(err)
	}
	defer f.Close()

	data := &syscall.Win32finddata{}
	for {
		e := syscall.FindNextFile(syscall.Handle(f.Fd()), data)
		if e != nil {
			if e == syscall.ERROR_NO_MORE_FILES {
				break
			} else {
				return osErrToFileErr(&os.PathError{
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
		if data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			continue
		}
		var typ os.FileMode = 0 // regular file
		if data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
			typ = os.ModeDir
		}
		if e = filter(name, typ); e == errDoneForNow {
			// filtering requested to return by caller.
			return nil
		}
	}

	return err
}

// Return N entries at the directory dirPath. If count is -1, return all entries
func readDirN(dirPath string, count int) (entries []string, err error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer f.Close()

	data := &syscall.Win32finddata{}

	for count != 0 {
		e := syscall.FindNextFile(syscall.Handle(f.Fd()), data)
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
			continue
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			entries = append(entries, name+SlashSeparator)
		default:
			entries = append(entries, name)
		}
		count--
	}

	return entries, nil
}

func globalSync() {
	// no-op on windows
}
