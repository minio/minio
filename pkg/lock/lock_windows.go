// +build windows

package lock

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")

	errLocked = errors.New("The process cannot access the file because another process has locked a portion of the file.")
)

// see https://msdn.microsoft.com/en-us/library/windows/desktop/ms681382(v=vs.85).aspx
const errLockViolation syscall.Errno = 0x21

func lock(f *os.File) error {
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203(v=vs.85).aspx
	return lockFileEx(syscall.Handle(f.Fd()), 2)
}

func rlock(f *os.File) error {
	return lockFileEx(syscall.Handle(f.Fd()), 0)
}

func runlock(f *os.File) error {
	return unlockFileEx(syscall.Handle(f.Fd()))
}

func unlock(f *os.File) error {
	return unlockFileEx(syscall.Handle(f.Fd()))
}

func open(path string, flag int, perm os.FileMode) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("cannot open empty filename")
	}
	var access uint32
	switch flag {
	case syscall.O_RDONLY:
		access = syscall.GENERIC_READ
	case syscall.O_WRONLY:
		access = syscall.GENERIC_WRITE
	case syscall.O_RDWR:
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	case syscall.O_WRONLY | syscall.O_CREAT:
		access = syscall.GENERIC_ALL
	default:
		panic(fmt.Errorf("flag %v is not supported", flag))
	}
	fd, err := syscall.CreateFile(&(syscall.StringToUTF16(path)[0]),
		access,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func unlockFileEx(fd syscall.Handle) error {
	if fd == syscall.InvalidHandle {
		return nil
	}
	err := unlockFileExSyscall(fd, 1, 0, &syscall.Overlapped{})
	if err == nil {
		return nil
	} else if err.Error() == errLocked.Error() {
		return err
	} else if err != errLockViolation {
		return err
	}
	return nil

}

func lockFileEx(fd syscall.Handle, flag uint32) error {
	if fd == syscall.InvalidHandle {
		return nil
	}
	err := lockFileExSyscall(fd, flag, 1, 0, &syscall.Overlapped{})
	if err == nil {
		return nil
	} else if err.Error() == errLocked.Error() {
		return err
	} else if err != errLockViolation {
		return err
	}
	return nil
}

func unlockFileExSyscall(h syscall.Handle, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	var reserved uint32
	_, _, err = procUnlockFileEx.Call(uintptr(h), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
	return err
}

func lockFileExSyscall(h syscall.Handle, flags, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	var reserved uint32
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
