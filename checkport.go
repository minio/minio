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

package main

import (
	"errors"
	"fmt"
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
// the minio server we make sure that the port is free on all the IPs.
func checkPortAvailability(port int) error {
	isAddrInUse := func(err error) bool {
		// Check if the syscall error is EADDRINUSE.
		// EADDRINUSE is the system call error if another process is
		// already listening at the specified port.
		neterr, ok := err.(*net.OpError)
		if !ok {
			return false
		}
		osErr, ok := neterr.Err.(*os.SyscallError)
		if !ok {
			return false
		}
		sysErr, ok := osErr.Err.(syscall.Errno)
		if !ok {
			return false
		}
		if sysErr != syscall.EADDRINUSE {
			return false
		}
		return true
	}

	ifcs, err := net.Interfaces()
	if err != nil {
		return err
	}

	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			return err
		}

		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				errorIf(errors.New(""), "Failed to assert type on (*net.IPNet) interface.")
				continue
			}

			ip := ipnet.IP
			network := "tcp4"
			if ip.To4() == nil {
				network = "tcp6"
			}

			l, err := net.Listen(network, fmt.Sprintf(":%d", port))
			if err != nil {
				if isAddrInUse(err) {
					// Fail if port is already in use.
					return err
				}

				// Ignore other errors.
				continue

			}

			if err = l.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}
