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
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-xl/pkg/probe"
)

// This package is a fork https://github.com/facebookgo/grace
//
// Re-licensing with Apache License 2.0, with code modifications

// This package provides a family of Listen functions that either open a
// fresh connection or provide an inherited connection from when the process
// was started. This behaves like their counterparts in the net pacakge, but
// transparently provide support for graceful restarts without dropping
// connections. This is provided in a systemd socket activation compatible form
// to allow using socket activation.
//

const (
	// Used to indicate a graceful restart in the new process.
	envCountKey       = "LISTEN_FDS" // similar to systemd SDS_LISTEN_FDS
	envCountKeyPrefix = envCountKey + "="
)

// In order to keep the working directory the same as when we started we record
// it at startup.
var originalWD, _ = os.Getwd()

// minNet provides the family of Listen functions and maintains the associated
// state. Typically you will have only once instance of minNet per application.
type minNet struct {
	inheritedListeners []net.Listener
	activeListeners    []net.Listener
	connLimit          int
	mutex              sync.Mutex
	inheritOnce        sync.Once
}

// minAddr simple wrapper over net.Addr interface to implement IsEqual()
type minAddr struct {
	net.Addr
}

// fileListener simple interface to extract file pointers from different types of net.Listener's
type fileListener interface {
	File() (*os.File, error)
}

// getInheritedListeners - look for LISTEN_FDS in environment variables and populate listeners accordingly
func (n *minNet) getInheritedListeners() *probe.Error {
	var retErr *probe.Error
	n.inheritOnce.Do(func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()
		countStr := os.Getenv(envCountKey)
		if countStr == "" {
			return
		}
		count, err := strconv.Atoi(countStr)
		if err != nil {
			retErr = probe.NewError(fmt.Errorf("found invalid count value: %s=%s", envCountKey, countStr))
			return
		}

		fdStart := 3
		for i := fdStart; i < fdStart+count; i++ {
			file := os.NewFile(uintptr(i), "listener")
			l, err := net.FileListener(file)
			if err != nil {
				file.Close()
				retErr = probe.NewError(err)
				return
			}
			if err := file.Close(); err != nil {
				retErr = probe.NewError(err)
				return
			}
			n.inheritedListeners = append(n.inheritedListeners, l)
		}
	})
	if retErr != nil {
		return retErr.Trace()
	}
	return nil
}

// Listen announces on the local network address laddr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket". It
// returns an inherited net.Listener for the matching network and address, or
// creates a new one using net.Listen()
func (n *minNet) Listen(nett, laddr string) (net.Listener, *probe.Error) {
	switch nett {
	default:
		return nil, probe.NewError(net.UnknownNetworkError(nett))
	case "tcp", "tcp4", "tcp6":
		addr, err := net.ResolveTCPAddr(nett, laddr)
		if err != nil {
			return nil, probe.NewError(err)
		}
		return n.ListenTCP(nett, addr)
	case "unix", "unixpacket":
		addr, err := net.ResolveUnixAddr(nett, laddr)
		if err != nil {
			return nil, probe.NewError(err)
		}
		return n.ListenUnix(nett, addr)
	}
}

// ListenTCP announces on the local network address laddr. The network net must
// be: "tcp", "tcp4" or "tcp6". It returns an inherited net.Listener for the
// matching network and address, or creates a new one using net.ListenTCP.
func (n *minNet) ListenTCP(nett string, laddr *net.TCPAddr) (net.Listener, *probe.Error) {
	if err := n.getInheritedListeners(); err != nil {
		return nil, err.Trace()
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inheritedListeners {
		if l == nil { // we nil used inherited listeners
			continue
		}
		equal := minAddr{l.Addr()}.IsEqual(laddr)
		if equal {
			n.inheritedListeners[i] = nil
			n.activeListeners = append(n.activeListeners, l)
			return l.(*net.TCPListener), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenTCP(nett, laddr)
	if err != nil {
		return nil, probe.NewError(err)
	}
	n.activeListeners = append(n.activeListeners, rateLimitedListener(l, n.connLimit))
	return l, nil
}

// ListenUnix announces on the local network address laddr. The network net
// must be a: "unix" or "unixpacket". It returns an inherited net.Listener for
// the matching network and address, or creates a new one using net.ListenUnix.
func (n *minNet) ListenUnix(nett string, laddr *net.UnixAddr) (net.Listener, *probe.Error) {
	if err := n.getInheritedListeners(); err != nil {
		return nil, err.Trace()
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inheritedListeners {
		if l == nil { // we nil used inherited listeners
			continue
		}
		equal := minAddr{l.Addr()}.IsEqual(laddr)
		if equal {
			n.inheritedListeners[i] = nil
			n.activeListeners = append(n.activeListeners, l)
			return l.(*net.UnixListener), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenUnix(nett, laddr)
	if err != nil {
		return nil, probe.NewError(err)
	}
	n.activeListeners = append(n.activeListeners, rateLimitedListener(l, n.connLimit))
	return l, nil
}

// activeListeners returns a snapshot copy of the active listeners.
func (n *minNet) getActiveListeners() []net.Listener {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	ls := make([]net.Listener, len(n.activeListeners))
	copy(ls, n.activeListeners)
	return ls
}

// IsEqual is synonymous with IP.IsEqual() method, here IsEqual matches net.Addr instead of net.IP
func (n1 minAddr) IsEqual(n2 net.Addr) bool {
	if n1.Network() != n2.Network() {
		return false
	}
	a1h, a1p, _ := net.SplitHostPort(n1.String())
	a2h, a2p, _ := net.SplitHostPort(n2.String())
	// Special cases since Addr() from net.Listener will
	// add frivolous [::] ipv6 for no ":[PORT]" style addresses
	if a1h == "::" && a2h == "" && a1p == a2p {
		return true
	}
	if a2h == "::" && a1h == "" && a1p == a2p {
		return true
	}
	if net.ParseIP(a1h).Equal(net.ParseIP(a2h)) && a1p == a2p {
		return true
	}
	return false
}

// StartProcess starts a new process passing it the active listeners. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func (n *minNet) StartProcess() (int, *probe.Error) {
	listeners := n.getActiveListeners()
	// Extract the fds from the listeners.
	files := make([]*os.File, len(listeners))
	for i, l := range listeners {
		var err error
		files[i], err = l.(fileListener).File()
		if err != nil {
			return 0, probe.NewError(err)
		}
		defer files[i].Close()
	}

	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, probe.NewError(err)
	}

	// Pass on the environment and replace the old count key with the new one.
	var env []string
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, envCountKeyPrefix) {
			env = append(env, v)
		}
	}
	env = append(env, fmt.Sprintf("%s%d", envCountKeyPrefix, len(listeners)))

	allFiles := append([]*os.File{os.Stdin, os.Stdout, os.Stderr}, files...)
	process, err := os.StartProcess(argv0, os.Args, &os.ProcAttr{
		Dir:   originalWD,
		Env:   env,
		Files: allFiles,
	})
	if err != nil {
		return 0, probe.NewError(err)
	}
	return process.Pid, nil
}
