//go:build (linux && !appengine) || darwin || freebsd || netbsd || openbsd
// +build linux,!appengine darwin freebsd netbsd openbsd

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
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"

	"github.com/minio/minio/internal/bpool"
	"golang.org/x/sys/unix"
)

func access(name string) error {
	if err := unix.Access(name, unix.F_OK); err != nil {
		return &os.PathError{Op: "lstat", Path: name, Err: err}
	}
	return nil
}

// openFileWithFD return 'fd' based file descriptor
func openFileWithFD(name string, flag int, perm os.FileMode) (fd int, err error) {
	switch flag & writeMode {
	case writeMode:
		defer updateOSMetrics(osMetricOpenFileWFd, name)(err)
	default:
		defer updateOSMetrics(osMetricOpenFileRFd, name)(err)
	}
	var e error
	fd, e = syscall.Open(name, flag|syscall.O_CLOEXEC, uint32(perm))
	if e != nil {
		return -1, &os.PathError{Op: "open", Path: name, Err: e}
	}
	return fd, nil
}

// Forked from Golang but chooses to avoid performing lookup
//
// osMkdirAll creates a directory named path,
// along with any necessary parents, and returns nil,
// or else returns an error.
// The permission bits perm (before umask) are used for all
// directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing
// and returns nil.
func osMkdirAll(dirPath string, perm os.FileMode, baseDir string) error {
	if baseDir != "" {
		if strings.HasPrefix(baseDir, dirPath) {
			return nil
		}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(dirPath)
	for i > 0 && os.IsPathSeparator(dirPath[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(dirPath[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		if err := osMkdirAll(dirPath[:j-1], perm, baseDir); err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	if err := Mkdir(dirPath, perm); err != nil {
		if osIsExist(err) {
			return nil
		}
		return err
	}

	return nil
}

// The buffer must be at least a block long.
// refer https://github.com/golang/go/issues/24015
const blockSize = 8 << 10 // 8192

// By default at least 128 entries in single getdents call (1MiB buffer)
var (
	direntPool = bpool.Pool[*[]byte]{
		New: func() *[]byte {
			buf := make([]byte, blockSize*128)
			return &buf
		},
	}

	direntNamePool = bpool.Pool[*[]byte]{
		New: func() *[]byte {
			buf := make([]byte, blockSize)
			return &buf
		},
	}
)

// unexpectedFileMode is a sentinel (and bogus) os.FileMode
// value used to represent a syscall.DT_UNKNOWN Dirent.Type.
const unexpectedFileMode os.FileMode = os.ModeNamedPipe | os.ModeSocket | os.ModeDevice

func parseDirEnt(buf []byte) (consumed int, name []byte, typ os.FileMode, err error) {
	// golang.org/issue/15653
	dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[0]))
	if v := unsafe.Offsetof(dirent.Reclen) + unsafe.Sizeof(dirent.Reclen); uintptr(len(buf)) < v {
		return consumed, nil, typ, fmt.Errorf("buf size of %d smaller than dirent header size %d", len(buf), v)
	}
	if len(buf) < int(dirent.Reclen) {
		return consumed, nil, typ, fmt.Errorf("buf size %d < record length %d", len(buf), dirent.Reclen)
	}
	consumed = int(dirent.Reclen)
	if direntInode(dirent) == 0 { // File absent in directory.
		return consumed, name, typ, err
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
		return consumed, nil, typ, err
	}

	return consumed, nameBuf[:nameLen], typ, nil
}

// readDirFn applies the fn() function on each entries at dirPath, doesn't recurse into
// the directory itself, if the dirPath doesn't exist this function doesn't return
// an error.
func readDirFn(dirPath string, fn func(name string, typ os.FileMode) error) error {
	fd, err := openFileWithFD(dirPath, readMode, 0o666)
	if err != nil {
		if osErrToFileErr(err) == errFileNotFound {
			return nil
		}
		if !osIsPermission(err) {
			return osErrToFileErr(err)
		}
		// There may be permission error when dirPath
		// is at the root of the disk mount that may
		// not have the permissions to avoid 'noatime'
		fd, err = openFileWithFD(dirPath, os.O_RDONLY, 0o666)
		if err != nil {
			if osErrToFileErr(err) == errFileNotFound {
				return nil
			}
			return osErrToFileErr(err)
		}
	}
	defer syscall.Close(fd)

	bufp := direntPool.Get()
	defer direntPool.Put(bufp)
	buf := *bufp

	boff := 0 // starting read position in buf
	nbuf := 0 // end valid data in buf

	for {
		if boff >= nbuf {
			boff = 0
			stop := globalOSMetrics.time(osMetricReadDirent)
			nbuf, err = syscall.ReadDirent(fd, buf)
			stop()
			if err != nil {
				if isSysErrNotDir(err) {
					return nil
				}
				err = osErrToFileErr(err)
				if err == errFileNotFound {
					return nil
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
		if len(name) == 0 || bytes.Equal(name, []byte{'.'}) || bytes.Equal(name, []byte{'.', '.'}) {
			continue
		}

		// Fallback for filesystems (like old XFS) that don't
		// support Dirent.Type and have DT_UNKNOWN (0) there
		// instead.
		if typ == unexpectedFileMode || typ&os.ModeSymlink == os.ModeSymlink {
			fi, err := Stat(pathJoin(dirPath, string(name)))
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
			if typ&os.ModeSymlink == os.ModeSymlink && fi.IsDir() {
				continue
			}

			typ = fi.Mode() & os.ModeType
		}
		if err = fn(string(name), typ); err == errDoneForNow {
			// fn() requested to return by caller.
			return nil
		}
	}

	return err
}

// Return count entries at the directory dirPath and all entries
// if count is set to -1
func readDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	fd, err := openFileWithFD(dirPath, readMode, 0o666)
	if err != nil {
		if !osIsPermission(err) {
			return nil, osErrToFileErr(err)
		}
		// There may be permission error when dirPath
		// is at the root of the disk mount that may
		// not have the permissions to avoid 'noatime'
		fd, err = openFileWithFD(dirPath, os.O_RDONLY, 0o666)
		if err != nil {
			return nil, osErrToFileErr(err)
		}
	}
	defer syscall.Close(fd)

	bufp := direntPool.Get()
	defer direntPool.Put(bufp)
	buf := *bufp

	nameTmp := direntNamePool.Get()
	defer direntNamePool.Put(nameTmp)
	tmp := *nameTmp

	boff := 0 // starting read position in buf
	nbuf := 0 // end valid data in buf

	count := opts.count

	for count != 0 {
		if boff >= nbuf {
			boff = 0
			stop := globalOSMetrics.time(osMetricReadDirent)
			nbuf, err = syscall.ReadDirent(fd, buf)
			stop()
			if err != nil {
				if isSysErrNotDir(err) {
					return nil, errFileNotFound
				}
				return nil, osErrToFileErr(err)
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
		if len(name) == 0 || bytes.Equal(name, []byte{'.'}) || bytes.Equal(name, []byte{'.', '.'}) {
			continue
		}

		// Fallback for filesystems (like old XFS) that don't
		// support Dirent.Type and have DT_UNKNOWN (0) there
		// instead.
		if typ == unexpectedFileMode || typ&os.ModeSymlink == os.ModeSymlink {
			fi, err := Stat(pathJoin(dirPath, string(name)))
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
			if !opts.followDirSymlink && typ&os.ModeSymlink == os.ModeSymlink && fi.IsDir() {
				continue
			}

			typ = fi.Mode() & os.ModeType
		}

		var nameStr string
		if typ.IsRegular() {
			nameStr = string(name)
		} else if typ.IsDir() {
			// Use temp buffer to append a slash to avoid string concat.
			tmp = tmp[:len(name)+1]
			copy(tmp, name)
			tmp[len(tmp)-1] = '/' // SlashSeparator
			nameStr = string(tmp)
		}

		count--
		entries = append(entries, nameStr)
	}

	return entries, err
}

func globalSync() {
	defer globalOSMetrics.time(osMetricSync)()
	syscall.Sync()
}
