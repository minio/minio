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
	"sort"
	"strings"
)

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
				name:         fi.Name(),
				size:         fi.Size(),
				modifiedTime: fi.ModTime(),
				isDir:        fi.IsDir(),
			}
			if dirent.isDir {
				dirent.name += string(os.PathSeparator)
				dirent.size = 0
			}
			if strings.HasPrefix(fi.Name(), entryPrefixMatch) {
				dirents = append(dirents, dirent)
			}
		}
	}
	// Sort dirents.
	sort.Sort(fsDirents(dirents))
	return dirents, nil
}
