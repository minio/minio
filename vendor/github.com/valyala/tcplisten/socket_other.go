// +build !darwin

package tcplisten

import (
	"fmt"
	"syscall"
)

func newSocketCloexec(domain, typ, proto int) (int, error) {
	fd, err := syscall.Socket(domain, typ|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, proto)
	if err == nil {
		return fd, nil
	}

	if err == syscall.EPROTONOSUPPORT || err == syscall.EINVAL {
		return newSocketCloexecOld(domain, typ, proto)
	}

	return -1, fmt.Errorf("cannot create listening unblocked socket: %s", err)
}
