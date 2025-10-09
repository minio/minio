// Copyright (c) 2015-2021 MinIO, Inc.
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
	"fmt"
	"net"
	"slices"
	"syscall"
	"time"

	"github.com/minio/minio/internal/deadlineconn"
)

type acceptResult struct {
	conn net.Conn
	err  error
	lidx int
}

// httpListener - HTTP listener capable of handling multiple server addresses.
type httpListener struct {
	opts        TCPOptions
	listeners   []net.Listener    // underlying TCP listeners.
	acceptCh    chan acceptResult // channel where all TCP listeners write accepted connection.
	ctxDoneCh   <-chan struct{}
	ctxCanceler context.CancelFunc
}

// start - starts separate goroutine for each TCP listener.  A valid new connection is passed to httpListener.acceptCh.
func (listener *httpListener) start() {
	// Closure to handle listener until httpListener.ctxDoneCh channel is closed.
	handleListener := func(idx int, ln net.Listener) {
		for {
			conn, err := ln.Accept()
			select {
			case listener.acceptCh <- acceptResult{conn, err, idx}:
			case <-listener.ctxDoneCh:
				return
			}
		}
	}

	// Start separate goroutine for each listener to handle connection.
	for idx, ln := range listener.listeners {
		go handleListener(idx, ln)
	}
}

// Accept - reads from httpListener.acceptCh for one of previously accepted TCP connection and returns the same.
func (listener *httpListener) Accept() (conn net.Conn, err error) {
	select {
	case result := <-listener.acceptCh:
		if result.err != nil {
			return nil, result.err
		}
		return deadlineconn.New(result.conn).WithReadDeadline(listener.opts.IdleTimeout).WithWriteDeadline(listener.opts.IdleTimeout), result.err
	case <-listener.ctxDoneCh:
	}
	return nil, syscall.EINVAL
}

// Close - closes underneath all TCP listeners.
func (listener *httpListener) Close() (err error) {
	listener.ctxCanceler()

	for i := range listener.listeners {
		listener.listeners[i].Close()
	}

	return nil
}

// Addr - net.Listener interface compatible method returns net.Addr.  In case of multiple TCP listeners, it returns '0.0.0.0' as IP address.
func (listener *httpListener) Addr() (addr net.Addr) {
	addr = listener.listeners[0].Addr()
	if len(listener.listeners) == 1 {
		return addr
	}

	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return &net.TCPAddr{
			IP:   net.IPv4zero,
			Port: tcpAddr.Port,
			Zone: tcpAddr.Zone,
		}
	}
	panic("unknown address type on listener")
}

// Addrs - returns all address information of TCP listeners.
func (listener *httpListener) Addrs() (addrs []net.Addr) {
	addrs = make([]net.Addr, 0, len(listener.listeners))
	for i := range listener.listeners {
		addrs = append(addrs, listener.listeners[i].Addr())
	}

	return addrs
}

// TCPOptions specify customizable TCP optimizations on raw socket
type TCPOptions struct {
	UserTimeout int // this value is expected to be in milliseconds

	// When the net.Conn is a remote drive this value is honored, we close the connection to remote peer proactively.
	DriveOPTimeout func() time.Duration

	SendBufSize int              // SO_SNDBUF size for the socket connection, NOTE: this sets server and client connection
	RecvBufSize int              // SO_RECVBUF size for the socket connection, NOTE: this sets server and client connection
	NoDelay     bool             // Indicates callers to enable TCP_NODELAY on the net.Conn
	Interface   string           // This is a VRF device passed via `--interface` flag
	Trace       func(msg string) // Trace when starting.
	IdleTimeout time.Duration    // Incoming TCP read/write timeout
}

// ForWebsocket returns TCPOptions valid for websocket net.Conn
func (t TCPOptions) ForWebsocket() TCPOptions {
	return TCPOptions{
		UserTimeout: t.UserTimeout,
		Interface:   t.Interface,
		SendBufSize: t.SendBufSize,
		RecvBufSize: t.RecvBufSize,
		NoDelay:     true,
	}
}

// newHTTPListener - creates new httpListener object which is interface compatible to net.Listener.
// httpListener is capable to
// * listen to multiple addresses
// * controls incoming connections only doing HTTP protocol
func newHTTPListener(ctx context.Context, serverAddrs []string, opts TCPOptions) (listener *httpListener, listenErrs []error) {
	listeners := make([]net.Listener, 0, len(serverAddrs))
	listenErrs = make([]error, len(serverAddrs))

	if opts.Trace == nil {
		opts.Trace = func(msg string) {} // Noop if not defined.
	}

	// Unix listener with special TCP options.
	listenCfg := net.ListenConfig{
		Control: setTCPParametersFn(opts),
	}

	for i, serverAddr := range serverAddrs {
		l, e := listenCfg.Listen(ctx, "tcp", serverAddr)
		if e != nil {
			opts.Trace("listenCfg.Listen: " + e.Error())

			listenErrs[i] = e
			continue
		}
		opts.Trace("adding listener to " + l.Addr().String())

		listeners = append(listeners, l)
	}

	if len(listeners) == 0 {
		// No listeners initialized, no need to continue
		return listener, listenErrs
	}
	listeners = slices.Clip(listeners)

	ctx, cancel := context.WithCancel(ctx)
	listener = &httpListener{
		listeners:   listeners,
		acceptCh:    make(chan acceptResult, len(listeners)),
		opts:        opts,
		ctxDoneCh:   ctx.Done(),
		ctxCanceler: cancel,
	}
	opts.Trace(fmt.Sprintf("opening %d listeners", len(listener.listeners)))
	listener.start()

	return listener, listenErrs
}
