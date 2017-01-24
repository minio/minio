/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

const defaultPort = "9000"

// splitHostPort is wrapper to net.SplitHostPort(), but ignores "missing port in address".
// If port is missing, default port "9000" is returned.
func splitHostPort(hostPort string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(hostPort)
	if err != nil {
		// Ignore "missing port in address" error as we use default port.
		// This happens when hostPort = "" or hostPort = "server".
		if !strings.Contains(err.Error(), "missing port in address") {
			return "", "", err
		}

		host = hostPort
		port = defaultPort
		err = nil
	}

	p, err := strconv.Atoi(port)
	if err != nil {
		return "", "", fmt.Errorf("Invalid port number")
	} else if p < 1 {
		return "", "", fmt.Errorf("Port number should be greater than zero")
	}

	return host, port, err
}

func mustSplitHostPort(hostPort string) (host, port string) {
	host, port, err := splitHostPort(hostPort)
	if err != nil {
		fatalIf(err, "Unable to split host port %s", hostPort)
	}

	return host, port
}

// mustGetLocalIP4 returns IPv4 addresses of local host.  It panics on error.
func mustGetLocalIP4() (ipList set.StringSet) {
	ipList = set.NewStringSet()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fatalIf(err, "Unable to get IP addresses of this host.")
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip.To4() != nil {
			ipList.Add(ip.String())
		}
	}

	return ipList
}

// getHostIP4 returns IPv4 address of given host.
func getHostIP4(host string) (ipList set.StringSet, err error) {
	ipList = set.NewStringSet()
	ips, err := net.LookupIP(host)
	if err != nil {
		return ipList, err
	}

	for _, ip := range ips {
		if ip.To4() != nil {
			ipList.Add(ip.String())
		}
	}

	return ipList, err
}
