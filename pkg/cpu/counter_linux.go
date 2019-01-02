package cpu

import (
	"syscall"
	"time"
	"unsafe"
)

const CLOCK_PROCESS_CPUTIME_ID = 2

func newCounter() (counter, error) {
	return counter{}, nil
}

func (c counter) now() time.Time {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, CLOCK_PROCESS_CPUTIME_ID, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec)
}
