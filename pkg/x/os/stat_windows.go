// +build windows

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// FIXME: Once we have a go version released with the
// following fix https://go-review.googlesource.com/#/c/41834/.
// We should actively purge this block.

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package os implements extended safe functions
// for stdlib "os".
package os

import (
	os1 "os"
	"path/filepath"
	"syscall"
	"time"
)

const errSharingViolation syscall.Errno = 32

// fixLongPath returns the extended-length (\\?\-prefixed) form of
// path when needed, in order to avoid the default 260 character file
// path limit imposed by Windows. If path is not easily converted to
// the extended-length form (for example, if path is a relative path
// or contains .. elements), or is short enough, fixLongPath returns
// path unmodified.
//
// See https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx#maxpath
func fixLongPath(path string) string {
	// Do nothing (and don't allocate) if the path is "short".
	// Empirically (at least on the Windows Server 2013 builder),
	// the kernel is arbitrarily okay with < 248 bytes. That
	// matches what the docs above say:
	// "When using an API to create a directory, the specified
	// path cannot be so long that you cannot append an 8.3 file
	// name (that is, the directory name cannot exceed MAX_PATH
	// minus 12)." Since MAX_PATH is 260, 260 - 12 = 248.
	//
	// The MSDN docs appear to say that a normal path that is 248 bytes long
	// will work; empirically the path must be less then 248 bytes long.
	if len(path) < 248 {
		// Don't fix. (This is how Go 1.7 and earlier worked,
		// not automatically generating the \\?\ form)
		return path
	}

	// The extended form begins with \\?\, as in
	// \\?\c:\windows\foo.txt or \\?\UNC\server\share\foo.txt.
	// The extended form disables evaluation of . and .. path
	// elements and disables the interpretation of / as equivalent
	// to \. The conversion here rewrites / to \ and elides
	// . elements as well as trailing or duplicate separators. For
	// simplicity it avoids the conversion entirely for relative
	// paths or paths containing .. elements. For now,
	// \\server\share paths are not converted to
	// \\?\UNC\server\share paths because the rules for doing so
	// are less well-specified.
	if len(path) >= 2 && path[:2] == `\\` {
		// Don't canonicalize UNC paths.
		return path
	}
	if !filepath.IsAbs(path) {
		// Relative path
		return path
	}

	const prefix = `\\?`

	pathbuf := make([]byte, len(prefix)+len(path)+len(`\`))
	copy(pathbuf, prefix)
	n := len(path)
	r, w := 0, len(prefix)
	for r < n {
		switch {
		case os1.IsPathSeparator(path[r]):
			// empty block
			r++
		case path[r] == '.' && (r+1 == n || os1.IsPathSeparator(path[r+1])):
			// /./
			r++
		case r+1 < n && path[r] == '.' && path[r+1] == '.' && (r+2 == n || os1.IsPathSeparator(path[r+2])):
			// /../ is currently unhandled
			return path
		default:
			pathbuf[w] = '\\'
			w++
			for ; r < n && !os1.IsPathSeparator(path[r]); r++ {
				pathbuf[w] = path[r]
				w++
			}
		}
	}
	// A drive's root directory needs a trailing \
	if w == len(`\\?\c:`) {
		pathbuf[w] = '\\'
		w++
	}
	return string(pathbuf[:w])
}

// Stat returns a FileInfo structure describing the
// named file. If there is an error, it will be of type
// *PathError.
func Stat(name string) (os1.FileInfo, error) {
	if len(name) == 0 {
		return nil, &os1.PathError{
			Op:   "Stat",
			Path: name,
			Err:  syscall.Errno(syscall.ERROR_PATH_NOT_FOUND),
		}
	}
	if name == os1.DevNull {
		return &devNullStat, nil
	}
	namep, err := syscall.UTF16PtrFromString(fixLongPath(name))
	if err != nil {
		return nil, &os1.PathError{Op: "Stat", Path: name, Err: err}
	}

	// Use Windows I/O manager to dereference the symbolic link, as per
	// https://blogs.msdn.microsoft.com/oldnewthing/20100212-00/?p=14963/
	h, err := syscall.CreateFile(namep, 0, 0, nil,
		syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		if err == errSharingViolation {
			// try FindFirstFile now that CreateFile failed
			return statWithFindFirstFile(name, namep)
		}
		return nil, &os1.PathError{Op: "CreateFile", Path: name, Err: err}
	}
	defer syscall.CloseHandle(h)

	var d syscall.ByHandleFileInformation
	if err = syscall.GetFileInformationByHandle(h, &d); err != nil {
		return nil, &os1.PathError{
			Op:   "GetFileInformationByHandle",
			Path: name,
			Err:  err,
		}
	}
	return &fileStat{
		name: filepath.Base(name),
		sys: syscall.Win32FileAttributeData{
			FileAttributes: d.FileAttributes,
			CreationTime:   d.CreationTime,
			LastAccessTime: d.LastAccessTime,
			LastWriteTime:  d.LastWriteTime,
			FileSizeHigh:   d.FileSizeHigh,
			FileSizeLow:    d.FileSizeLow,
		},
		vol:   d.VolumeSerialNumber,
		idxhi: d.FileIndexHigh,
		idxlo: d.FileIndexLow,
		// fileStat.path is used by os1.SameFile to decide, if it needs
		// to fetch vol, idxhi and idxlo. But these are already set,
		// so set fileStat.path to "" to prevent os1.SameFile doing it again.
		// Also do not set fileStat.filetype, because it is only used for
		// console and stdin/stdout. But you cannot call os1.Stat for these.
	}, nil
}

// statWithFindFirstFile is used by Stat to handle special case of stating
// c:\pagefile.sys. We might discovered other files need similar treatment.
func statWithFindFirstFile(name string, namep *uint16) (os1.FileInfo, error) {
	var fd syscall.Win32finddata
	h, err := syscall.FindFirstFile(namep, &fd)
	if err != nil {
		return nil, &os1.PathError{Op: "FindFirstFile", Path: name, Err: err}
	}
	syscall.FindClose(h)

	fullpath := name
	if !filepath.IsAbs(fullpath) {
		fullpath, err = syscall.FullPath(fullpath)
		if err != nil {
			return nil, &os1.PathError{Op: "FullPath", Path: name, Err: err}
		}
	}
	return &fileStat{
		name: filepath.Base(name),
		path: fullpath,
		sys: syscall.Win32FileAttributeData{
			FileAttributes: fd.FileAttributes,
			CreationTime:   fd.CreationTime,
			LastAccessTime: fd.LastAccessTime,
			LastWriteTime:  fd.LastWriteTime,
			FileSizeHigh:   fd.FileSizeHigh,
			FileSizeLow:    fd.FileSizeLow,
		},
	}, nil
}

// A fileStat is the implementation of os1.FileInfo returned by stat.
type fileStat struct {
	name     string
	sys      syscall.Win32FileAttributeData
	filetype uint32 // what syscall.GetFileType returns

	path  string
	vol   uint32
	idxhi uint32
	idxlo uint32
}

func (fs *fileStat) Name() string { return fs.name }
func (fs *fileStat) IsDir() bool  { return fs.Mode().IsDir() }

func (fs *fileStat) Size() int64 {
	return int64(fs.sys.FileSizeHigh)<<32 + int64(fs.sys.FileSizeLow)
}

// devNullStat is fileStat structure describing DevNull file ("NUL").
var devNullStat = fileStat{
	name:  os1.DevNull,
	vol:   0,
	idxhi: 0,
	idxlo: 0,
}

func (fs *fileStat) Mode() (m os1.FileMode) {
	if fs == &devNullStat {
		return os1.ModeDevice | os1.ModeCharDevice | 0666
	}
	if fs.sys.FileAttributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		m |= 0444
	} else {
		m |= 0666
	}
	if fs.sys.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return m | os1.ModeSymlink
	}
	if fs.sys.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m |= os1.ModeDir | 0111
	}
	switch fs.filetype {
	case syscall.FILE_TYPE_PIPE:
		m |= os1.ModeNamedPipe
	case syscall.FILE_TYPE_CHAR:
		m |= os1.ModeCharDevice
	}
	return m
}

func (fs *fileStat) ModTime() time.Time {
	return time.Unix(0, fs.sys.LastWriteTime.Nanoseconds())
}

// Sys returns syscall.Win32FileAttributeData for file fs.
func (fs *fileStat) Sys() interface{} { return &fs.sys }
