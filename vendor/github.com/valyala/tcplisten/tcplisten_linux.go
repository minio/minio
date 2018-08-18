// +build linux

package tcplisten

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const (
	soReusePort = 0x0F
	tcpFastOpen = 0x17
)

func enableDeferAccept(fd int) error {
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_DEFER_ACCEPT, 1); err != nil {
		return fmt.Errorf("cannot enable TCP_DEFER_ACCEPT: %s", err)
	}
	return nil
}

func enableFastOpen(fd int) error {
	if err := syscall.SetsockoptInt(fd, syscall.SOL_TCP, tcpFastOpen, fastOpenQlen); err != nil {
		return fmt.Errorf("cannot enable TCP_FASTOPEN(qlen=%d): %s", fastOpenQlen, err)
	}
	return nil
}

const fastOpenQlen = 16 * 1024

func soMaxConn() (int, error) {
	data, err := ioutil.ReadFile(soMaxConnFilePath)
	if err != nil {
		// This error may trigger on travis build. Just use SOMAXCONN
		if os.IsNotExist(err) {
			return syscall.SOMAXCONN, nil
		}
		return -1, err
	}
	s := strings.TrimSpace(string(data))
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return -1, fmt.Errorf("cannot parse somaxconn %q read from %s: %s", s, soMaxConnFilePath, err)
	}

	// Linux stores the backlog in a uint16.
	// Truncate number to avoid wrapping.
	// See https://github.com/golang/go/issues/5030 .
	if n > 1<<16-1 {
		n = 1<<16 - 1
	}
	return n, nil
}

const soMaxConnFilePath = "/proc/sys/net/core/somaxconn"
