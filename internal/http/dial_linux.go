//go:build linux
// +build linux

// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package http

import (
	"context"
	"net"
	"syscall"
	"time"

	"github.com/minio/minio/internal/deadlineconn"
	"golang.org/x/sys/unix"
)

func setTCPParametersFn(opts TCPOptions) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		c.Control(func(fdPtr uintptr) {
			// got socket file descriptor to set parameters.
			fd := int(fdPtr)

			_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)

			_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)

			// Enable custom socket send/recv buffers.
			if opts.SendBufSize > 0 {
				_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, opts.SendBufSize)
			}

			if opts.RecvBufSize > 0 {
				_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, opts.RecvBufSize)
			}

			if opts.NoDelay {
				_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_NODELAY, 1)
				_ = syscall.SetsockoptInt(fd, syscall.SOL_TCP, unix.TCP_CORK, 0)
			}

			// Enable TCP open
			// https://lwn.net/Articles/508865/ - 32k queue size.
			_ = syscall.SetsockoptInt(fd, syscall.SOL_TCP, unix.TCP_FASTOPEN, 32*1024)

			// Enable TCP fast connect
			// TCPFastOpenConnect sets the underlying socket to use
			// the TCP fast open connect. This feature is supported
			// since Linux 4.11.
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_FASTOPEN_CONNECT, 1)

			// Enable TCP quick ACK, John Nagle says
			// "Set TCP_QUICKACK. If you find a case where that makes things worse, let me know."
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_QUICKACK, 1)

			/// Enable keep-alive
			{
				_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)

				// The time (in seconds) the connection needs to remain idle before
				// TCP starts sending keepalive probes
				_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 15)

				// Number of probes.
				// ~ cat /proc/sys/net/ipv4/tcp_keepalive_probes (defaults to 9, we reduce it to 5)
				_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 5)

				// Wait time after successful probe in seconds.
				// ~ cat /proc/sys/net/ipv4/tcp_keepalive_intvl (defaults to 75 secs, we reduce it to 15 secs)
				_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 15)
			}

			// Set tcp user timeout in addition to the keep-alive - tcp-keepalive is not enough to close a socket
			// with dead end because tcp-keepalive is not fired when there is data in the socket buffer.
			//    https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
			// This is a sensitive configuration, it is better to set it to high values, > 60 secs since it can
			// affect clients reading data with a very slow pace  (disappropriate with socket buffer sizes)
			if opts.UserTimeout > 0 {
				_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, opts.UserTimeout)
			}

			if opts.Interface != "" {
				if h, _, err := net.SplitHostPort(address); err == nil {
					address = h
				}
				// Create socket on specific vrf device.
				// To catch all kinds of special cases this filters specifically for loopback networks.
				if ip := net.ParseIP(address); ip != nil && !ip.IsLoopback() {
					_ = syscall.SetsockoptString(fd, syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, opts.Interface)
				}
			}
		})
		return nil
	}
}

// DialContext is a function to make custom Dial for internode communications
type DialContext func(ctx context.Context, network, address string) (net.Conn, error)

// NewInternodeDialContext setups a custom dialer for internode communication
func NewInternodeDialContext(dialTimeout time.Duration, opts TCPOptions) DialContext {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout: dialTimeout,
			Control: setTCPParametersFn(opts),
		}
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if opts.DriveOPTimeout != nil {
			// Read deadlines are sufficient for now as per various
			// scenarios of hung node detection, we may add Write deadlines
			// if needed later on.
			return deadlineconn.New(conn).WithReadDeadline(opts.DriveOPTimeout()), nil
		}
		return conn, nil
	}
}
