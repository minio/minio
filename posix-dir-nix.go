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
	"path"
	"runtime"
	"sort"
	"syscall"
	"unsafe"
)

const (
	// readDirentBufSize for syscall.ReadDirent() to hold multiple
	// directory entries in one buffer. golang source uses 4096 as
	// buffer size whereas we want 25 times larger to save lots of
	// entries to avoid multiple syscall.ReadDirent() call.
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
func parseDirents(dirPath string, buf []byte) []fsDirent {
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

		var mode os.FileMode
		switch dirent.Type {
		case syscall.DT_BLK, syscall.DT_WHT:
			mode = os.ModeDevice
		case syscall.DT_CHR:
			mode = os.ModeDevice | os.ModeCharDevice
		case syscall.DT_DIR:
			mode = os.ModeDir
		case syscall.DT_FIFO:
			mode = os.ModeNamedPipe
		case syscall.DT_LNK:
			mode = os.ModeSymlink
		case syscall.DT_REG:
			mode = 0
		case syscall.DT_SOCK:
			mode = os.ModeSocket
		case syscall.DT_UNKNOWN:
			// On Linux XFS does not implement d_type for on disk
			// format << v5. Fall back to Stat().
			if fi, err := os.Stat(path.Join(dirPath, name)); err == nil {
				mode = fi.Mode()
			} else {
				// Caller listing would fail, if Stat failed but we
				// won't crash the server.
				mode = 0xffffffff
			}
		}

		dirents = append(dirents, fsDirent{
			name: name,
			mode: mode,
		})
	}
	return dirents
}

// scans the directory dirPath, calling filter() on each directory
// entry.  Entries for which filter() returns true are stored, lexically
// sorted using sort.Sort(). If filter is NULL, all entries are selected.
// If namesOnly is true, dirPath is not appended into entry name.
func scandir(dirPath string, filter func(fsDirent) bool, namesOnly bool) ([]fsDirent, error) {
	buf := make([]byte, readDirentBufSize)
	d, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	fd := int(d.Fd())
	dirents := []fsDirent{}
	for {
		nbuf, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			break
		}
		for _, dirent := range parseDirents(dirPath, buf[:nbuf]) {
			if !namesOnly {
				dirent.name = path.Join(dirPath, dirent.name)
			}
			if dirent.IsDir() {
				dirent.name += string(os.PathSeparator)
				dirent.size = 0
			}
			if filter == nil || filter(dirent) {
				dirents = append(dirents, dirent)
			}
		}
	}

	sort.Sort(byDirentName(dirents))
	return dirents, nil
}
