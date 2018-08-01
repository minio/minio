package tcplisten

import (
	"fmt"
	"syscall"
)

func newSocketCloexecOld(domain, typ, proto int) (int, error) {
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(domain, typ, proto)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, fmt.Errorf("cannot create listening socket: %s", err)
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return -1, fmt.Errorf("cannot make non-blocked listening socket: %s", err)
	}
	return fd, nil
}
