// +build !linux,!darwin,!openbsd,!freebsd,!netbsd,!dragonfly

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Read all directory entries, returns a list of lexically sorted entries.
func readDirAll(readDirPath, entryPrefixMatch string) ([]fsDirent, error) {
	f, err := os.Open(readDirPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var dirents []fsDirent
	for {
		fis, err := f.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, fi := range fis {
			dirent := fsDirent{
				name:    fi.Name(),
				modTime: fi.ModTime(),
				size:    fi.Size(),
				mode:    fi.Mode(),
			}
			if dirent.IsDir() {
				dirent.name += string(os.PathSeparator)
				dirent.size = 0
			}
			if strings.HasPrefix(fi.Name(), entryPrefixMatch) {
				dirents = append(dirents, dirent)
			}
		}
	}
	// Sort dirents.
	sort.Sort(byDirentName(dirents))
	return dirents, nil
}

// scans the directory dirPath, calling filter() on each directory
// entry.  Entries for which filter() returns true are stored, lexically
// sorted using sort.Sort(). If filter is NULL, all entries are selected.
// If namesOnly is true, dirPath is not appended into entry name.
func scandir(dirPath string, filter func(fsDirent) bool, namesOnly bool) ([]fsDirent, error) {
	d, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	var dirents []fsDirent
	for {
		fis, err := d.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, fi := range fis {
			dirent := fsDirent{
				name:    fi.Name(),
				modTime: fi.ModTime(),
				size:    fi.Size(),
				mode:    fi.Mode(),
			}
			if !namesOnly {
				dirent.name = filepath.Join(dirPath, dirent.name)
			}
			if dirent.IsDir() {
				dirent.name += string(os.PathSeparator)
			}
			if filter == nil || filter(dirent) {
				dirents = append(dirents, dirent)
			}
		}
	}

	sort.Sort(byDirentName(dirents))

	return dirents, nil
}
