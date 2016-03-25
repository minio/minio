// +build darwin dragonfly freebsd linux netbsd openbsd

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
	"syscall"
	"unsafe"
)

// actual length of the byte array from the c - world.
func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}

func parseDirents(buf []byte) []Dirent {
	bufidx := 0
	dirents := []Dirent{}
	for bufidx < len(buf) {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[bufidx]))
		bufidx += int(dirent.Reclen)
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		if name == "." || name == ".." { // Useless names
			continue
		}
		dirents = append(dirents, Dirent{
			Name:  name,
			IsDir: dirent.Type == syscall.DT_DIR,
		})
	}
	return dirents
}

func readDirAll(readDirPath, entryPrefixMatch string) ([]Dirent, error) {
	buf := make([]byte, 100*1024)
	f, err := os.Open(readDirPath)
	if err != nil {
		return nil, err
	}
	dirents := []Dirent{}
	for {
		nbuf, err := syscall.ReadDirent(int(f.Fd()), buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			break
		}
		for _, dirent := range parseDirents(buf[:nbuf]) {
			if strings.HasPrefix(dirent.Name, entryPrefixMatch) {
				dirents = append(dirents, dirent)
			}
		}
	}
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
