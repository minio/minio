//go:build plan9 || solaris
// +build plan9 solaris

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

func osMkdirAll(dirPath string, perm os.FileMode, _ string) error {
	// baseDir is not honored in plan9 and solaris platforms.
	return os.MkdirAll(dirPath, perm)
}

// readDirFn applies the fn() function on each entries at dirPath, doesn't recurse into
// the directory itself, if the dirPath doesn't exist this function doesn't return
// an error.
func readDirFn(dirPath string, filter func(name string, typ os.FileMode) error) error {
	d, err := Open(dirPath)
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
				fi, err = Stat(pathJoin(dirPath, fi.Name()))
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

// Return entries at the directory dirPath.
func readDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	d, err := Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer d.Close()

	maxEntries := 1000
	if opts.count > 0 && opts.count < maxEntries {
		maxEntries = opts.count
	}

	done := false
	remaining := opts.count

	for !done {
		// Read up to max number of entries.
		fis, err := d.Readdir(maxEntries)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, osErrToFileErr(err)
		}
		if opts.count > -1 {
			if remaining <= len(fis) {
				fis = fis[:remaining]
				done = true
			}
		}
		for _, fi := range fis {
			if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
				fi, err = Stat(pathJoin(dirPath, fi.Name()))
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

				// Ignore symlinked directories.
				if !opts.followDirSymlink && fi.IsDir() {
					continue
				}
			}

			if fi.IsDir() {
				// Append SlashSeparator instead of "\" so that sorting is achieved as expected.
				entries = append(entries, fi.Name()+SlashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, fi.Name())
			}
			if opts.count > 0 {
				remaining--
			}
		}
	}
	return entries, nil
}

func globalSync() {
	// no-op not sure about plan9/solaris support for syscall support
	defer globalOSMetrics.time(osMetricSync)()
	syscall.Sync()
}
