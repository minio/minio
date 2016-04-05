// +build linux darwin dragonfly freebsd netbsd openbsd

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
	"os"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"unsafe"
)

const (
	// Large enough buffer size for ReadDirent() syscall
	readDirentBufSize = 4096 * 25
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

// parseDirents - inspired from
// https://golang.org/src/syscall/syscall_<os>.go
func parseDirents(buf []byte) []fsDirent {
	bufidx := 0
	dirents := []fsDirent{}
	for bufidx < len(buf) {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[bufidx]))
		// On non-Linux operating systems for rec length of zero means
		// we have reached EOF break out.
		if runtime.GOOS != "linux" && dirent.Reclen == 0 {
			break
		}
		bufidx += int(dirent.Reclen)
		// Skip dirents if they are absent in directory.
		if isEmptyDirent(dirent) {
			continue
		}
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		// Reserved names skip them.
		if name == "." || name == ".." {
			continue
		}
		dirents = append(dirents, fsDirent{
			name:  name,
			isDir: (dirent.Type == syscall.DT_DIR),
		})
	}
	return dirents
}

// Read all directory entries, returns a list of lexically sorted entries.
func readDirAll(readDirPath, entryPrefixMatch string) ([]fsDirent, error) {
	buf := make([]byte, readDirentBufSize)
	f, err := os.Open(readDirPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dirents := []fsDirent{}
	for {
		nbuf, err := syscall.ReadDirent(int(f.Fd()), buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			break
		}
		for _, dirent := range parseDirents(buf[:nbuf]) {
			if dirent.isDir {
				dirent.name += string(os.PathSeparator)
				dirent.size = 0
			}
			if strings.HasPrefix(dirent.name, entryPrefixMatch) {
				dirents = append(dirents, dirent)
			}
		}
	}
	sort.Sort(byDirentNames(dirents))
	return dirents, nil
}
