// +build linux darwin freebsd netbsd openbsd dragonfly
// +build !appengine

package pb

import "syscall"

const sysIoctl = syscall.SYS_IOCTL
