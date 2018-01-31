// +build windows

/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
)

const (
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
	lockFileExclusiveLock   = 2
	lockFileFailImmediately = 1

	// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382(v=vs.85).aspx
	errLockViolation syscall.Errno = 0x21
)

// lockedOpenFile is an internal function.
func lockedOpenFile(path string, flag int, perm os.FileMode, lockType uint32) (*LockedFile, error) {
	f, err := Open(path, flag, perm)
	if err != nil {
		return nil, err
	}

	if err = lockFile(syscall.Handle(f.Fd()), lockType); err != nil {
		f.Close()
		return nil, err
	}

	st, err := os.Stat(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	if st.IsDir() {
		f.Close()
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  syscall.EISDIR,
		}
	}

	return &LockedFile{File: f}, nil
}

// TryLockedOpenFile - tries a new write lock, functionality
// it is similar to LockedOpenFile with with syscall.LOCK_EX
// mode but along with syscall.LOCK_NB such that the function
// doesn't wait forever but instead returns if it cannot
// acquire a write lock.
func TryLockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, lockFileFailImmediately)
}

// LockedOpenFile - initializes a new lock and protects
// the file from concurrent access.
func LockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return lockedOpenFile(path, flag, perm, 0)
}

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
		case os.IsPathSeparator(path[r]):
			// empty block
			r++
		case path[r] == '.' && (r+1 == n || os.IsPathSeparator(path[r+1])):
			// /./
			r++
		case r+1 < n && path[r] == '.' && path[r+1] == '.' && (r+2 == n || os.IsPathSeparator(path[r+2])):
			// /../ is currently unhandled
			return path
		default:
			pathbuf[w] = '\\'
			w++
			for ; r < n && !os.IsPathSeparator(path[r]); r++ {
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

// perm param is ignored, on windows file perms/NT acls
// are not octet combinations. Providing access to NT
// acls is out of scope here.
func Open(path string, flag int, perm os.FileMode) (*os.File, error) {
	if path == "" {
		return nil, syscall.ERROR_FILE_NOT_FOUND
	}

	pathp, err := syscall.UTF16PtrFromString(fixLongPath(path))
	if err != nil {
		return nil, err
	}

	var access uint32
	switch flag {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		fallthrough
	case syscall.O_RDWR | syscall.O_CREAT:
		fallthrough
	case syscall.O_WRONLY | syscall.O_CREAT:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	case syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND:
		access = syscall.FILE_APPEND_DATA
	default:
		return nil, fmt.Errorf("Unsupported flag (%d)", flag)
	}

	var createflag uint32
	switch {
	case flag&syscall.O_CREAT == syscall.O_CREAT:
		createflag = syscall.OPEN_ALWAYS
	default:
		createflag = syscall.OPEN_EXISTING
	}

	shareflag := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)
	accessAttr := uint32(syscall.FILE_ATTRIBUTE_NORMAL | 0x80000000)

	fd, err := syscall.CreateFile(pathp, access, shareflag, nil, createflag, accessAttr, 0)
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), path), nil
}

func lockFile(fd syscall.Handle, flags uint32) error {
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
	var flag uint32 = lockFileExclusiveLock // Lockfile exlusive.
	flag |= flags

	if fd == syscall.InvalidHandle {
		return nil
	}

	err := lockFileEx(fd, flag, 1, 0, &syscall.Overlapped{})
	if err == nil {
		return nil
	} else if err.Error() == "The process cannot access the file because another process has locked a portion of the file." {
		return ErrAlreadyLocked
	} else if err != errLockViolation {
		return err
	}

	return nil
}

func lockFileEx(h syscall.Handle, flags, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	var reserved = uint32(0)
	r1, _, e1 := syscall.Syscall6(procLockFileEx.Addr(), 6, uintptr(h), uintptr(flags),
		uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}
