// +build !windows

package mgo_test

import (
	"syscall"
)

func stop(pid int) (err error) {
	return syscall.Kill(pid, syscall.SIGSTOP)
}

func cont(pid int) (err error) {
	return syscall.Kill(pid, syscall.SIGCONT)
}
