// +build plan9 solaris

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
	d, err := os.Open(dirPath)
	if err != nil {
		if osErrToFileErr(err) == errFileNotFound {
			return nil
		}
		return osErrToFileErr(err)
	}
	defer d.Close()

	maxEntries := 1000
	for {
		// Read up to max number of entries.
		fis, err := d.Readdir(maxEntries)
		if err != nil {
			if err == io.EOF {
				break
			}
			err = osErrToFileErr(err)
			if err == errFileNotFound {
				return nil
			}
			return err
		}
		for _, fi := range fis {
			if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
				fi, err = os.Stat(pathJoin(dirPath, fi.Name()))
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

				// Ignore symlinked directories.
				if fi.IsDir() {
					continue
				}
			}
			if err = filter(fi.Name(), fi.Mode()); err == errDoneForNow {
				// filtering requested to return by caller.
				return nil
			}
		}
	}
	return nil
}

// Return N entries at the directory dirPath. If count is -1, return all entries
func readDirN(dirPath string, count int) (entries []string, err error) {
	d, err := os.Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer d.Close()

	maxEntries := 1000
	if count > 0 && count < maxEntries {
		maxEntries = count
	}

	done := false
	remaining := count

	for !done {
		// Read up to max number of entries.
		fis, err := d.Readdir(maxEntries)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, osErrToFileErr(err)
		}
		if count > -1 {
			if remaining <= len(fis) {
				fis = fis[:remaining]
				done = true
			}
		}
		for _, fi := range fis {
			if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
				fi, err = os.Stat(pathJoin(dirPath, fi.Name()))
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

				// Ignore symlinked directories.
				if fi.IsDir() {
					continue
				}
			}

			if fi.IsDir() {
				// Append SlashSeparator instead of "\" so that sorting is achieved as expected.
				entries = append(entries, fi.Name()+SlashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, fi.Name())
			}
			if count > 0 {
				remaining--
			}
		}
	}
	return entries, nil
}

func globalSync() {
	// no-op not sure about plan9/solaris support for syscall support
	syscall.Sync()
}
