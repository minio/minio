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

// checkPortAvailability - check if given port is already in use.
// Note: The check method tries to listen on given port and closes it.
// It is possible to have a disconnected client in this tiny window of time.
func checkPortAvailability(port string) error {
	network := [3]string{"tcp", "tcp4", "tcp6"}
	for _, n := range network {
		l, err := net.Listen(n, net.JoinHostPort("", port))
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
