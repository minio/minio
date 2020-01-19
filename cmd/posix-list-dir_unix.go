// +build linux,!appengine darwin freebsd netbsd openbsd

/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// The buffer must be at least a block long.
// refer https://github.com/golang/go/issues/24015
const blockSize = 8 << 10

// unexpectedFileMode is a sentinel (and bogus) os.FileMode
// value used to represent a syscall.DT_UNKNOWN Dirent.Type.
const unexpectedFileMode os.FileMode = os.ModeNamedPipe | os.ModeSocket | os.ModeDevice

func parseDirEnt(buf []byte) (consumed int, name string, typ os.FileMode, err error) {
	// golang.org/issue/15653
	dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[0]))
	if v := unsafe.Offsetof(dirent.Reclen) + unsafe.Sizeof(dirent.Reclen); uintptr(len(buf)) < v {
		return consumed, name, typ, fmt.Errorf("buf size of %d smaller than dirent header size %d", len(buf), v)
	}
	if len(buf) < int(dirent.Reclen) {
		return consumed, name, typ, fmt.Errorf("buf size %d < record length %d", len(buf), dirent.Reclen)
	}
	consumed = int(dirent.Reclen)
	if direntInode(dirent) == 0 { // File absent in directory.
		return
	}
	switch dirent.Type {
	case syscall.DT_REG:
		typ = 0
	case syscall.DT_DIR:
		typ = os.ModeDir
	case syscall.DT_LNK:
		typ = os.ModeSymlink
	default:
		// Skip all other file types. Revisit if/when this code needs
		// to handle such files, MinIO is only interested in
		// files and directories.
		typ = unexpectedFileMode
	}

	nameBuf := (*[unsafe.Sizeof(dirent.Name)]byte)(unsafe.Pointer(&dirent.Name[0]))
	nameLen, err := direntNamlen(dirent)
	if err != nil {
		return consumed, name, typ, err
	}

	name = string(nameBuf[:nameLen])
	return consumed, name, typ, nil
}

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirN(dirPath, -1)
}

// Return count entries at the directory dirPath and all entries
// if count is set to -1
func readDirN(dirPath string, count int) (entries []string, err error) {
	fd, err := syscall.Open(dirPath, 0, 0)
	if err != nil {
		if os.IsNotExist(err) || isSysErrNotDir(err) {
			return nil, errFileNotFound
		}
		if os.IsPermission(err) {
			return nil, errFileAccessDenied
		}
		return nil, err
	}
	defer syscall.Close(fd)

	buf := make([]byte, blockSize) // stack-allocated; doesn't escape
	boff := 0                      // starting read position in buf
	nbuf := 0                      // end valid data in buf

	for count != 0 {
		if boff >= nbuf {
			boff = 0
			nbuf, err = syscall.ReadDirent(fd, buf)
			if err != nil {
				if isSysErrNotDir(err) {
					return nil, errFileNotFound
				}
				return nil, err
			}
			if nbuf <= 0 {
				break
			}
		}
		consumed, name, typ, err := parseDirEnt(buf[boff:nbuf])
		if err != nil {
			return nil, err
		}
		boff += consumed
		if name == "" || name == "." || name == ".." {
			continue
		}
		// Fallback for filesystems (like old XFS) that don't
		// support Dirent.Type and have DT_UNKNOWN (0) there
		// instead.
		if typ == unexpectedFileMode || typ&os.ModeSymlink == os.ModeSymlink {
			fi, err := os.Stat(pathJoin(dirPath, name))
			if err != nil {
				// It got deleted in the meantime, not found
				// or returns too many symlinks ignore this
				// file/directory.
				if os.IsNotExist(err) || isSysErrPathNotFound(err) ||
					isSysErrTooManySymlinks(err) {
					continue
				}
				return nil, err
			}
			typ = fi.Mode() & os.ModeType
		}
		if typ.IsRegular() {
			entries = append(entries, name)
		} else if typ.IsDir() {
			entries = append(entries, name+SlashSeparator)
		}
		count--
	}
	return
}
