/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package minhttp

import (
	"net"
	"os"
	"sync"
)

// rateLimitedListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func rateLimitedListener(l net.Listener, nconn int) net.Listener {
	if nconn > 0 {
		return &rateLimitListener{l, make(chan struct{}, nconn)}
	}
	return l
}

type rateLimitListener struct {
	net.Listener
	sem chan struct{}
}

func (l *rateLimitListener) accept()  { l.sem <- struct{}{} }
func (l *rateLimitListener) release() { <-l.sem }

// File - necessary to expose underlying socket fd
func (l *rateLimitListener) File() (f *os.File, err error) {
	return l.Listener.(fileListener).File()
}

// Accept - accept method for accepting new connections
func (l *rateLimitListener) Accept() (net.Conn, error) {
	l.accept()

	c, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	return &rateLimitListenerConn{Conn: c, release: l.release}, nil
}

type rateLimitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (l *rateLimitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}
