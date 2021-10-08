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
	"syscall"
)

type acceptResult struct {
	conn net.Conn
	err  error
	lidx int
}

// httpListener - HTTP listener capable of handling multiple server addresses.
type httpListener struct {
	tcpListeners []*net.TCPListener // underlaying TCP listeners.
	acceptCh     chan acceptResult  // channel where all TCP listeners write accepted connection.
	ctx          context.Context
	ctxCanceler  context.CancelFunc
}

// start - starts separate goroutine for each TCP listener.  A valid new connection is passed to httpListener.acceptCh.
func (listener *httpListener) start() {
	// Closure to send acceptResult to acceptCh.
	// It returns true if the result is sent else false if returns when doneCh is closed.
	send := func(result acceptResult) bool {
		select {
		case listener.acceptCh <- result:
			// Successfully written to acceptCh
			return true
		case <-listener.ctx.Done():
			return false
		}
	}

	// Closure to handle TCPListener until done channel is closed.
	handleListener := func(idx int, tcpListener *net.TCPListener) {
		for {
			tcpConn, err := tcpListener.AcceptTCP()
			if tcpConn != nil {
				tcpConn.SetKeepAlive(true)
			}
			send(acceptResult{tcpConn, err, idx})
		}
	}

	// Start separate goroutine for each TCP listener to handle connection.
	for idx, tcpListener := range listener.tcpListeners {
		go handleListener(idx, tcpListener)
	}
}

// Accept - reads from httpListener.acceptCh for one of previously accepted TCP connection and returns the same.
func (listener *httpListener) Accept() (conn net.Conn, err error) {
	select {
	case result, ok := <-listener.acceptCh:
		if ok {
			return result.conn, result.err
		}
	case <-listener.ctx.Done():
	}
	return nil, syscall.EINVAL
}

// Close - closes underneath all TCP listeners.
func (listener *httpListener) Close() (err error) {
	listener.ctxCanceler()

	for i := range listener.tcpListeners {
		listener.tcpListeners[i].Close()
	}

	return nil
}

// Addr - net.Listener interface compatible method returns net.Addr.  In case of multiple TCP listeners, it returns '0.0.0.0' as IP address.
func (listener *httpListener) Addr() (addr net.Addr) {
	addr = listener.tcpListeners[0].Addr()
	if len(listener.tcpListeners) == 1 {
		return addr
	}

	tcpAddr := addr.(*net.TCPAddr)
	if ip := net.ParseIP("0.0.0.0"); ip != nil {
		tcpAddr.IP = ip
	}

	addr = tcpAddr
	return addr
}

// Addrs - returns all address information of TCP listeners.
func (listener *httpListener) Addrs() (addrs []net.Addr) {
	for i := range listener.tcpListeners {
		addrs = append(addrs, listener.tcpListeners[i].Addr())
	}

	return addrs
}

// newHTTPListener - creates new httpListener object which is interface compatible to net.Listener.
// httpListener is capable to
// * listen to multiple addresses
// * controls incoming connections only doing HTTP protocol
func newHTTPListener(ctx context.Context, serverAddrs []string) (listener *httpListener, err error) {

	var tcpListeners []*net.TCPListener

	// Close all opened listeners on error
	defer func() {
		if err == nil {
			return
		}

		for _, tcpListener := range tcpListeners {
			// Ignore error on close.
			tcpListener.Close()
		}
	}()

	for _, serverAddr := range serverAddrs {
		var l net.Listener
		if l, err = listenCfg.Listen(ctx, "tcp", serverAddr); err != nil {
			return nil, err
		}

		tcpListener, ok := l.(*net.TCPListener)
		if !ok {
			return nil, fmt.Errorf("unexpected listener type found %v, expected net.TCPListener", l)
		}

		tcpListeners = append(tcpListeners, tcpListener)
	}

	listener = &httpListener{
		tcpListeners: tcpListeners,
		acceptCh:     make(chan acceptResult, len(tcpListeners)),
	}
	listener.ctx, listener.ctxCanceler = context.WithCancel(ctx)
	listener.start()

	return listener, nil
}
