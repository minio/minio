/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package nimble

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/iodine"
)

// This package originally from https://github.com/facebookgo/grace
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

// nimbleNet provides the family of Listen functions and maintains the associated
// state. Typically you will have only once instance of nimbleNet per application.
type nimbleNet struct {
	inheritedListeners []net.Listener
	activeListeners    []net.Listener
	mutex              sync.Mutex
	inheritOnce        sync.Once
}

// inherit - lookg for LISTEN_FDS in environment variables and populate listeners
func (n *nimbleNet) getInheritedListeners() error {
	var retErr error
	n.inheritOnce.Do(func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()
		countStr := os.Getenv(envCountKey)
		if countStr == "" {
			return
		}
		count, err := strconv.Atoi(countStr)
		if err != nil {
			retErr = fmt.Errorf("found invalid count value: %s=%s", envCountKey, countStr)
			return
		}

		fdStart := 3
		for i := fdStart; i < fdStart+count; i++ {
			file := os.NewFile(uintptr(i), "listener")
			l, err := net.FileListener(file)
			if err != nil {
				file.Close()
				retErr = fmt.Errorf("error inheriting socket fd %d: %s", i, err)
				return
			}
			if err := file.Close(); err != nil {
				retErr = fmt.Errorf("error closing inherited socket fd %d: %s", i, err)
				return
			}
			n.inheritedListeners = append(n.inheritedListeners, l)
		}
	})
	return iodine.New(retErr, nil)
}

// Listen announces on the local network address laddr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket". It
// returns an inherited net.Listener for the matching network and address, or
// creates a new one using net.Listen.
func (n *nimbleNet) Listen(nett, laddr string) (net.Listener, error) {
	switch nett {
	default:
		return nil, net.UnknownNetworkError(nett)
	case "tcp", "tcp4", "tcp6":
		addr, err := net.ResolveTCPAddr(nett, laddr)
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		return n.ListenTCP(nett, addr)
	case "unix", "unixpacket", "invalid_unix_net_for_test":
		addr, err := net.ResolveUnixAddr(nett, laddr)
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		return n.ListenUnix(nett, addr)
	}
}

// ListenTCP announces on the local network address laddr. The network net must
// be: "tcp", "tcp4" or "tcp6". It returns an inherited net.Listener for the
// matching network and address, or creates a new one using net.ListenTCP.
func (n *nimbleNet) ListenTCP(nett string, laddr *net.TCPAddr) (*net.TCPListener, error) {
	if err := n.getInheritedListeners(); err != nil {
		return nil, iodine.New(err, nil)
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inheritedListeners {
		if l == nil { // we nil used inherited listeners
			continue
		}
		if isSameAddr(l.Addr(), laddr) {
			n.inheritedListeners[i] = nil
			n.activeListeners = append(n.activeListeners, l)
			return l.(*net.TCPListener), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenTCP(nett, laddr)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	n.activeListeners = append(n.activeListeners, l)
	return l, nil
}

// ListenUnix announces on the local network address laddr. The network net
// must be a: "unix" or "unixpacket". It returns an inherited net.Listener for
// the matching network and address, or creates a new one using net.ListenUnix.
func (n *nimbleNet) ListenUnix(nett string, laddr *net.UnixAddr) (*net.UnixListener, error) {
	if err := n.getInheritedListeners(); err != nil {
		return nil, iodine.New(err, nil)
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inheritedListeners {
		if l == nil { // we nil used inherited listeners
			continue
		}
		if isSameAddr(l.Addr(), laddr) {
			n.inheritedListeners[i] = nil
			n.activeListeners = append(n.activeListeners, l)
			return l.(*net.UnixListener), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenUnix(nett, laddr)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	n.activeListeners = append(n.activeListeners, l)
	return l, nil
}

// activeListeners returns a snapshot copy of the active listeners.
func (n *nimbleNet) getActiveListeners() ([]net.Listener, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	ls := make([]net.Listener, len(n.activeListeners))
	copy(ls, n.activeListeners)
	return ls, nil
}

func isSameAddr(a1, a2 net.Addr) bool {
	if a1.Network() != a2.Network() {
		return false
	}
	a1s := a1.String()
	a2s := a2.String()
	if a1s == a2s {
		return true
	}

	// This allows for ipv6 vs ipv4 local addresses to compare as equal. This
	// scenario is common when listening on localhost.
	const ipv6prefix = "[::]"
	a1s = strings.TrimPrefix(a1s, ipv6prefix)
	a2s = strings.TrimPrefix(a2s, ipv6prefix)
	const ipv4prefix = "0.0.0.0"
	a1s = strings.TrimPrefix(a1s, ipv4prefix)
	a2s = strings.TrimPrefix(a2s, ipv4prefix)
	return a1s == a2s
}

// StartProcess starts a new process passing it the active listeners. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func (n *nimbleNet) StartProcess() (int, error) {
	listeners, err := n.getActiveListeners()
	if err != nil {
		return 0, iodine.New(err, nil)
	}

	// Extract the fds from the listeners.
	files := make([]*os.File, len(listeners))
	for i, l := range listeners {
		files[i], err = l.(fileListener).File()
		if err != nil {
			return 0, iodine.New(err, nil)
		}
		defer files[i].Close()
	}

	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, iodine.New(err, nil)
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
		return 0, iodine.New(err, nil)
	}
	return process.Pid, nil
}

type fileListener interface {
	File() (*os.File, error)
}
