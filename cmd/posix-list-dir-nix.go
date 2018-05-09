// +build linux darwin freebsd netbsd openbsd

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

package cmd

import (
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const (
	// readDirentBufSize for syscall.ReadDirent() to hold multiple
	// directory entries in one buffer. golang source uses 4096 as
	// buffer size whereas we want 64 times larger to save lots of
	// entries to avoid multiple syscall.ReadDirent() call.
	readDirentBufSize = 4096 * 64
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
func parseDirents(dirPath string, buf []byte) (entries []string, err error) {
	bufidx := 0
	for bufidx < len(buf) {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[bufidx]))
		// On non-Linux operating systems for rec length of zero means
		// we have reached EOF break out.
		if runtime.GOOS != "linux" && dirent.Reclen == 0 {
			break
		}
		bufidx += int(dirent.Reclen)
		// Skip if they are absent in directory.
		if isEmptyDirent(dirent) {
			continue
		}
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		// Reserved names skip them.
		if name == "." || name == ".." {
			continue
		}

		switch dirent.Type {
		case syscall.DT_DIR:
			entries = append(entries, name+slashSeparator)
		case syscall.DT_REG:
			entries = append(entries, name)
		case syscall.DT_LNK, syscall.DT_UNKNOWN:
			// If its symbolic link, follow the link using os.Stat()

			// On Linux XFS does not implement d_type for on disk
			// format << v5. Fall back to OsStat().
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
				entries = append(entries, fi.Name()+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, fi.Name())
			}
		default:
			// Skip entries which are not file or directory.
			continue
		}
	}
	return entries, nil
}

var readDirBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, readDirentBufSize)
		return &b
	},
}

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirN(dirPath, -1)
}

// Return count entries at the directory dirPath and all entries
// if count is set to -1
func readDirN(dirPath string, count int) (entries []string, err error) {
	bufp := readDirBufPool.Get().(*[]byte)
	buf := *bufp
	defer readDirBufPool.Put(bufp)

	d, err := os.Open(dirPath)
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		}
		if os.IsPermission(err) {
			return nil, errFileAccessDenied
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return nil, errFileNotFound
		}
		return nil, err
	}
	defer d.Close()

	fd := int(d.Fd())

	remaining := count
	done := false

	for !done {
		nbuf, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			break
		}
		var tmpEntries []string
		if tmpEntries, err = parseDirents(dirPath, buf[:nbuf]); err != nil {
			return nil, err
		}
		if count > 0 {
			if remaining <= len(tmpEntries) {
				tmpEntries = tmpEntries[:remaining]
				done = true
			}
			remaining -= len(tmpEntries)
		}
		entries = append(entries, tmpEntries...)
	}
	return
}
