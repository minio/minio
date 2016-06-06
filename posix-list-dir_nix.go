// +build linux darwin dragonfly freebsd netbsd openbsd nacl

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
	"path"
	"strings"
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

// readInt returns the size-bytes unsigned integer in native byte order at offset off.
// typecasting leads to bugs the problem is that a Dirent is potentially larger than
// the buffer, since it includes a byte array sized for the largest possible filename.
// Through type casting it makes it easy to accidentally read past the end of the buffer.
// readInt ensures that all reads are within the bounds of the input buffer.
func readInt(buf []byte, off, size uintptr) (u uint64, ok bool) {
	if len(buf) < int(off+size) {
		return 0, false
	}
	switch size {
	case 1:
		u = uint64(buf[off])
	case 2:
		var u16 uint16
		b1 := (*[2]byte)(unsafe.Pointer(&u16))
		b2 := (*[2]byte)(unsafe.Pointer(&buf[off]))
		copy((*b1)[:], (*b2)[:])
		u = uint64(u16)
	case 4:
		var u32 uint32
		b1 := (*[4]byte)(unsafe.Pointer(&u32))
		b2 := (*[4]byte)(unsafe.Pointer(&buf[off]))
		copy((*b1)[:], (*b2)[:])
		u = uint64(u32)
	case 8:
		b1 := (*[8]byte)(unsafe.Pointer(&u))
		b2 := (*[8]byte)(unsafe.Pointer(&buf[off]))
		copy((*b1)[:], (*b2)[:])
	default:
		panic("syscall: readInt with unsupported size")
	}
	return u, true
}

func direntReclen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(syscall.Dirent{}.Reclen), unsafe.Sizeof(syscall.Dirent{}.Reclen))
}

// parseDirents - parses input directory entries from dirent
// buffer. Returns back parsed slice of entries.
//
// This implementation is based on golang's syscall.ParseDirent
// modified to return back directories with a trailing '/'
// separator.
func parseDirents(dirPath string, buf []byte) (entries []string) {
	for len(buf) > 0 {
		dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[0]))
		reclen, ok := direntReclen(buf)
		if !ok || reclen > uint64(len(buf)) {
			break
		}
		rec := buf[:reclen]
		buf = buf[reclen:]
		ino, ok := direntIno(rec)
		if !ok {
			break
		}
		if ino == 0 { // File absent in directory.
			continue
		}
		const namoff = uint64(unsafe.Offsetof(syscall.Dirent{}.Name))
		namlen, ok := direntNamlen(rec)
		if !ok || namoff+namlen > uint64(len(rec)) {
			break
		}
		n := rec[namoff : namoff+namlen]
		for i, c := range n {
			if c == 0 {
				n = n[:i]
				break
			}
		}
		name := string(n)
		if name == "." || name == ".." { // Useless names
			continue
		}
		// Skip special files.
		if hasPosixReservedPrefix(name) {
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
			// format << v5. Fall back to Stat().
			fi, err := os.Stat(path.Join(dirPath, name))
			if err != nil {
				// If file does not exist, we continue and skip it.
				// Could happen if it was deleted in the middle while
				// this list was being performed.
				if os.IsNotExist(err) {
					err = nil
					continue
				}
				errorIf(err, "Unable to stat path at %s/%s", dirPath, name)
				// Explicitly not returning any entries upon
				// unrecognized error from Stat. So that
				// we can investigate.
				return nil
			}
			if fi.IsDir() {
				entries = append(entries, fi.Name()+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, fi.Name())
			}
		default:
			// We do not support any other file types.
			continue
		}
	}
	return entries
}

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	buf := make([]byte, readDirentBufSize)
	d, err := os.Open(dirPath)
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return nil, errFileNotFound
		}

		// Return any other errors.
		return nil, err
	}
	defer d.Close()

	fd := int(d.Fd())
	// List all entries at dirPath.
	for {
		var nbuf int
		nbuf, err = syscall.ReadDirent(fd, buf)
		if err != nil {
			return nil, err
		}
		if nbuf <= 0 {
			err = io.EOF // EOF.
			break
		}
		newEntries := parseDirents(dirPath, buf[:nbuf])
		if len(newEntries) == 0 {
			break // We couldn't find any entries in buf.
		}
		entries = append(entries, newEntries...)
	}
	return entries, err
}
