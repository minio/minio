// +build !windows,!plan9

package lock

import (
	"os"
	"syscall"
)

var (
	pid   = int32(os.Getpid())
	wrlck = syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
		Pid:    pid,
	}
	rdlck = syscall.Flock_t{
		Type:   syscall.F_RDLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
		Pid:    pid,
	}
	unlck = syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
		Pid:    pid,
	}
)

func open(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}

func lock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	//	lock := wrlck
	//	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &lock)
}

func unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	//	lock := unlck
	//	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock)
}

func rlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_SH)
	//	lock := rdlck
	//	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &lock)
}

func runlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	//	lock := unlck
	//	return syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock)
}
