// +build linux,!appengine darwin freebsd netbsd openbsd

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
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// The buffer must be at least a block long.
// refer https://github.com/golang/go/issues/24015
const blockSize = 8 << 10 // 8192

// By default atleast 1000 entries in single getdents call
var direntPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, blockSize*1000)
		return &buf
	},
}

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

// readDir applies the filter function on each entries at dirPath, doesn't recurse into
// the directory itself.
func readDirFilterFn(dirPath string, filter func(name string, typ os.FileMode) error) error {
	f, err := os.Open(dirPath)
	if err != nil {
		return osErrToFileErr(err)
	}
	defer f.Close()

	buf := make([]byte, blockSize)
	boff := 0 // starting read position in buf
	nbuf := 0 // end valid data in buf

	for {
		if boff >= nbuf {
			boff = 0
			nbuf, err = syscall.ReadDirent(int(f.Fd()), buf)
			if err != nil {
				if isSysErrNotDir(err) {
					return errFileNotFound
				}
				return err
			}
			if nbuf <= 0 {
				break // EOF
			}
		}
		consumed, name, typ, err := parseDirEnt(buf[boff:nbuf])
		if err != nil {
			return err
		}
		boff += consumed
		if name == "" || name == "." || name == ".." {
			continue
		}
		if typ&os.ModeSymlink == os.ModeSymlink {
			continue
		}
		if err = filter(name, typ); err == errDoneForNow {
			// filtering requested to return by caller.
			return nil
		}
	}

	return err
}

// Return count entries at the directory dirPath and all entries
// if count is set to -1
func readDirN(dirPath string, count int) (entries []string, err error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer f.Close()

	bufp := direntPool.Get().(*[]byte)
	defer direntPool.Put(bufp)

	boff := 0 // starting read position in buf
	nbuf := 0 // end valid data in buf

	for count != 0 {
		if boff >= nbuf {
			boff = 0
			nbuf, err = syscall.ReadDirent(int(f.Fd()), *bufp)
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
		consumed, name, typ, err := parseDirEnt((*bufp)[boff:nbuf])
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
		if typ == unexpectedFileMode {
			fi, err := os.Lstat(pathJoin(dirPath, name))
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
		if typ&os.ModeSymlink == os.ModeSymlink {
			continue
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

func globalSync() {
	syscall.Sync()
}
