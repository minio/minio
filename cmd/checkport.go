/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"net"
	"os"
	"syscall"
)

// Make sure that none of the other processes are listening on the
// specified port on any of the interfaces.
//
// On linux if a process is listening on 127.0.0.1:9000 then Listen()
// on ":9000" fails with the error "port already in use".
// However on Mac OSX Listen() on ":9000" falls back to the IPv6 address.
// This causes confusion on Mac OSX that minio server is not reachable
// on 127.0.0.1 even though minio server is running. So before we start
// the minio server we make sure that the port is free on each tcp network.
//
// Port is string on purpose here.
// https://github.com/golang/go/issues/16142#issuecomment-245912773
//
// "Keep in mind that ports in Go are strings: https://play.golang.org/p/zk2WEri_E9"
//                 - @bradfitz
func checkPortAvailability(portStr string) error {
	network := [3]string{"tcp", "tcp4", "tcp6"}
	for _, n := range network {
		l, err := net.Listen(n, net.JoinHostPort("", portStr))
		if err != nil {
			if isAddrInUse(err) {
				// Return error if another process is listening on the
				// same port.
				return err
			}
			// Ignore any other error (ex. EAFNOSUPPORT)
			continue
		}

		// look for error so we don't have dangling connection
		if err = l.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Return true if err is "address already in use" error.
// syscall.EADDRINUSE is available on all OSes.
func isAddrInUse(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if sysErr, ok := opErr.Err.(*os.SyscallError); ok {
			if errno, ok := sysErr.Err.(syscall.Errno); ok {
				if errno == syscall.EADDRINUSE {
					return true
				}
			}
		}
	}
	return false
}
