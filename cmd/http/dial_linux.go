// +build linux

/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"context"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func setInternalTCPParameters(c syscall.RawConn) error {
	return c.Control(func(fdPtr uintptr) {
		// got socket file descriptor to set parameters.
		fd := int(fdPtr)

		// Enable TCP fast connect
		// TCPFastOpenConnect sets the underlying socket to use
		// the TCP fast open connect. This feature is supported
		// since Linux 4.11.
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_FASTOPEN_CONNECT, 1)

		// The time (in seconds) the connection needs to remain idle before
		// TCP starts sending keepalive probes, set this to 5 secs
		// system defaults to 7200 secs!!!
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 5)

		// Number of probes.
		// ~ cat /proc/sys/net/ipv4/tcp_keepalive_probes (defaults to 9, we reduce it to 5)
		// 9
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 5)

		// Wait time after successful probe in seconds.
		// ~ cat /proc/sys/net/ipv4/tcp_keepalive_intvl (defaults to 75 secs, we reduce it to 3 secs)
		// 75
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3)
	})
}

// DialContext is a function to make custom Dial for internode communications
type DialContext func(ctx context.Context, network, address string) (net.Conn, error)

// NewInternodeDialContext setups a custom dialer for internode communication
func NewInternodeDialContext(dialTimeout time.Duration) DialContext {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout: dialTimeout,
			Control: func(network, address string, c syscall.RawConn) error {
				return setInternalTCPParameters(c)
			},
		}
		return dialer.DialContext(ctx, network, addr)
	}
}

// NewCustomDialContext setups a custom dialer for any external communication and proxies.
func NewCustomDialContext(dialTimeout time.Duration) DialContext {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout: dialTimeout,
			Control: func(network, address string, c syscall.RawConn) error {
				return c.Control(func(fdPtr uintptr) {
					// got socket file descriptor to set parameters.
					fd := int(fdPtr)

					// Enable TCP fast connect
					// TCPFastOpenConnect sets the underlying socket to use
					// the TCP fast open connect. This feature is supported
					// since Linux 4.11.
					_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, unix.TCP_FASTOPEN_CONNECT, 1)
				})
			},
		}
		return dialer.DialContext(ctx, network, addr)
	}
}
