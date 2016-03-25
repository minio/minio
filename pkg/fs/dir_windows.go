// +build windows

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

package fs

import (
	"os"
	"sort"
	"strings"
)

func readDirAll(readDirPath, entryPrefixMatch string) ([]Dirent, error) {
	buf := make([]byte, 100*1024)
	f, err := os.Open(readDirPath)
	if err != nil {
		return nil, err
	}
	fis, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}
	var dirents []Dirent
	for len(fis) > 0 {
		var fi os.FileInfo
		fi, fis = fis[0], fis[1:] // Pop out entries.
		if strings.HasPrefix(fi.Name(), entryPrefixMatch) {
			dirents = append(dirents, Dirent{
				Name:         fi.Name(),
				Size:         fi.Size(),
				ModifiedTime: fi.ModTime(),
				IsDir:        fi.IsDir(),
			})
		}
	}
	// Sort dirents.
	sort.Sort(Dirents(dirents))
	return dirents, nil
}

// Using sort.Search() internally to jump to the file entry containing the prefix.
func searchDirents(dirents []Dirent, x string) int {
	processFunc := func(i int) bool {
		return dirents[i].Name >= x
	}
	return sort.Search(len(dirents), processFunc)
}
