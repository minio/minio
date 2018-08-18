// +build linux darwin dragonfly freebsd netbsd openbsd rumprun

// Package tcplisten provides customizable TCP net.Listener with various
// performance-related options:
//
//   - SO_REUSEPORT. This option allows linear scaling server performance
//     on multi-CPU servers.
//     See https://www.nginx.com/blog/socket-sharding-nginx-release-1-9-1/ for details.
//
//   - TCP_DEFER_ACCEPT. This option expects the server reads from the accepted
//     connection before writing to them.
//
//   - TCP_FASTOPEN. See https://lwn.net/Articles/508865/ for details.
//
// The package is derived from https://github.com/kavu/go_reuseport .
package tcplisten

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

// Config provides options to enable on the returned listener.
type Config struct {
	// ReusePort enables SO_REUSEPORT.
	ReusePort bool

	// DeferAccept enables TCP_DEFER_ACCEPT.
	DeferAccept bool

	// FastOpen enables TCP_FASTOPEN.
	FastOpen bool

	// Backlog is the maximum number of pending TCP connections the listener
	// may queue before passing them to Accept.
	// See man 2 listen for details.
	//
	// By default system-level backlog value is used.
	Backlog int
}

// NewListener returns TCP listener with options set in the Config.
//
// The function may be called many times for creating distinct listeners
// with the given config.
//
// Only tcp4 and tcp6 networks are supported.
func (cfg *Config) NewListener(network, addr string) (net.Listener, error) {
	sa, soType, err := getSockaddr(network, addr)
	if err != nil {
		return nil, err
	}

	fd, err := newSocketCloexec(soType, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return nil, err
	}

	if err = cfg.fdSetup(fd, sa, addr); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	name := fmt.Sprintf("reuseport.%d.%s.%s", os.Getpid(), network, addr)
	file := os.NewFile(uintptr(fd), name)
	ln, err := net.FileListener(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	if err = file.Close(); err != nil {
		ln.Close()
		return nil, err
	}

	return ln, nil
}

func (cfg *Config) fdSetup(fd int, sa syscall.Sockaddr, addr string) error {
	var err error

	if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return fmt.Errorf("cannot enable SO_REUSEADDR: %s", err)
	}

	// This should disable Nagle's algorithm in all accepted sockets by default.
	// Users may enable it with net.TCPConn.SetNoDelay(false).
	if err = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
		return fmt.Errorf("cannot disable Nagle's algorithm: %s", err)
	}

	if cfg.ReusePort {
		if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1); err != nil {
			return fmt.Errorf("cannot enable SO_REUSEPORT: %s", err)
		}
	}

	if cfg.DeferAccept {
		if err = enableDeferAccept(fd); err != nil {
			return err
		}
	}

	if cfg.FastOpen {
		if err = enableFastOpen(fd); err != nil {
			return err
		}
	}

	if err = syscall.Bind(fd, sa); err != nil {
		return fmt.Errorf("cannot bind to %q: %s", addr, err)
	}

	backlog := cfg.Backlog
	if backlog <= 0 {
		if backlog, err = soMaxConn(); err != nil {
			return fmt.Errorf("cannot determine backlog to pass to listen(2): %s", err)
		}
	}
	if err = syscall.Listen(fd, backlog); err != nil {
		return fmt.Errorf("cannot listen on %q: %s", addr, err)
	}

	return nil
}

func getSockaddr(network, addr string) (sa syscall.Sockaddr, soType int, err error) {
	if network != "tcp4" && network != "tcp6" {
		return nil, -1, errors.New("only tcp4 and tcp6 network is supported")
	}

	tcpAddr, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, -1, err
	}

	switch network {
	case "tcp4":
		var sa4 syscall.SockaddrInet4
		sa4.Port = tcpAddr.Port
		copy(sa4.Addr[:], tcpAddr.IP.To4())
		return &sa4, syscall.AF_INET, nil
	case "tcp6":
		var sa6 syscall.SockaddrInet6
		sa6.Port = tcpAddr.Port
		copy(sa6.Addr[:], tcpAddr.IP.To16())
		if tcpAddr.Zone != "" {
			ifi, err := net.InterfaceByName(tcpAddr.Zone)
			if err != nil {
				return nil, -1, err
			}
			sa6.ZoneId = uint32(ifi.Index)
		}
		return &sa6, syscall.AF_INET6, nil
	default:
		return nil, -1, errors.New("Unknown network type " + network)
	}
}
