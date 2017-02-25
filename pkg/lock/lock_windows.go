// +build windows

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

package lock

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")

	errLocked = errors.New("The process cannot access the file because another process has locked a portion of the file.")
)

const (
	// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382(v=vs.85).aspx
	errLockViolation syscall.Errno = 0x21
)

// LockedOpenFile - initializes a new lock and protects
// the file from concurrent access.
func LockedOpenFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := open(path, flag, perm)
	if err != nil {
		return nil, err
	}

	if err = lockFile(syscall.Handle(f.Fd()), 0); err != nil {
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

func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	return &sa
}

// perm param is ignored, on windows file perms/NT acls
// are not octet combinations. Providing access to NT
// acls is out of scope here.
func open(path string, flag int, perm os.FileMode) (*os.File, error) {
	if path == "" {
		return nil, syscall.ERROR_FILE_NOT_FOUND
	}

	pathp, err := syscall.UTF16PtrFromString(path)
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
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	case syscall.O_RDWR | syscall.O_CREAT:
		access = syscall.GENERIC_ALL
	case syscall.O_WRONLY | syscall.O_CREAT:
		access = syscall.GENERIC_ALL
	}

	if flag&syscall.O_APPEND != 0 {
		access &^= syscall.GENERIC_WRITE
		access |= syscall.FILE_APPEND_DATA
	}

	var sa *syscall.SecurityAttributes
	if flag&syscall.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}

	var createflag uint32
	switch {
	case flag&(syscall.O_CREAT|syscall.O_EXCL) == (syscall.O_CREAT | syscall.O_EXCL):
		createflag = syscall.CREATE_NEW
	case flag&(syscall.O_CREAT|syscall.O_TRUNC) == (syscall.O_CREAT | syscall.O_TRUNC):
		createflag = syscall.CREATE_ALWAYS
	case flag&syscall.O_CREAT == syscall.O_CREAT:
		createflag = syscall.OPEN_ALWAYS
	case flag&syscall.O_TRUNC == syscall.O_TRUNC:
		createflag = syscall.TRUNCATE_EXISTING
	default:
		createflag = syscall.OPEN_EXISTING
	}

	shareflag := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)
	accessAttr := uint32(syscall.FILE_ATTRIBUTE_NORMAL | 0x80000000)

	fd, err := syscall.CreateFile(pathp, access, shareflag, sa, createflag, accessAttr, 0)
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), path), nil
}

func lockFile(fd syscall.Handle, flags uint32) error {
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
	var flag uint32 = 2 // Lockfile exlusive.
	flag |= flags

	if fd == syscall.InvalidHandle {
		return nil
	}

	err := lockFileEx(fd, flag, 1, 0, &syscall.Overlapped{})
	if err == nil {
		return nil
	} else if err.Error() == errLocked.Error() {
		return errors.New("lock already acquired")
	} else if err != errLockViolation {
		return err
	}

	return nil
}

func lockFileEx(h syscall.Handle, flags, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	var reserved = uint32(0)
	r1, _, e1 := syscall.Syscall6(procLockFileEx.Addr(), 6, uintptr(h), uintptr(flags), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
	if r1 == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}
