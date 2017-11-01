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
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/set"
	xnet "github.com/minio/minio/pkg/net"
)

// IPv4 addresses of local host.
var localIP4 = mustGetLocalIP4()

// mustSplitHostPort is a wrapper to net.SplitHostPort() where error is assumed to be a fatal.
func mustSplitHostPort(hostPort string) (host, port string) {
	host, port, err := net.SplitHostPort(hostPort)
	fatalIf(err, "Unable to split host port %s", hostPort)
	return host, port
}

// mustGetLocalIP4 returns IPv4 addresses of local host.  It panics on error.
func mustGetLocalIP4() (ipList set.StringSet) {
	ipList = set.NewStringSet()
	addrs, err := net.InterfaceAddrs()
	fatalIf(err, "Unable to get IP addresses of this host.")

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
	var ips []net.IP

	if ips, err = net.LookupIP(host); err != nil {
		// return err if not Docker or Kubernetes
		// We use IsDocker() method to check for Docker Swarm environment
		// as there is no reliable way to clearly identify Swarm from
		// Docker environment.
		if !IsDocker() && !IsKubernetes() {
			return ipList, err
		}

		// channel to indicate completion of host resolution
		doneCh := make(chan struct{})
		// Indicate retry routine to exit cleanly, upon this function return.
		defer close(doneCh)
		// Mark the starting time
		startTime := time.Now()
		// wait for hosts to resolve in exponentialbackoff manner
		for _ = range newRetryTimerSimple(doneCh) {
			// Retry infinitely on Kubernetes and Docker swarm.
			// This is needed as the remote hosts are sometime
			// not available immediately.
			if ips, err = net.LookupIP(host); err == nil {
				break
			}
			// time elapsed
			timeElapsed := time.Since(startTime)
			// log error only if more than 1s elapsed
			if timeElapsed > time.Second {
				// log the message to console about the host not being
				// resolveable.
				errorIf(err, "Unable to resolve host %s (%s)", host,
					humanize.RelTime(startTime, startTime.Add(timeElapsed), "elapsed", ""))
			}
		}
	}

	ipList = set.NewStringSet()
	for _, ip := range ips {
		if ip.To4() != nil {
			ipList.Add(ip.String())
		}
	}

	return ipList, err
}

// byLastOctetValue implements sort.Interface used in sorting a list
// of ip address by their last octet value in descending order.
type byLastOctetValue []net.IP

func (n byLastOctetValue) Len() int      { return len(n) }
func (n byLastOctetValue) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
func (n byLastOctetValue) Less(i, j int) bool {
	// This case is needed when all ips in the list
	// have same last octets, Following just ensures that
	// 127.0.0.1 is moved to the end of the list.
	if n[i].String() == "127.0.0.1" {
		return false
	}
	if n[j].String() == "127.0.0.1" {
		return true
	}
	return []byte(n[i].To4())[3] > []byte(n[j].To4())[3]
}

// sortIPs - sort ips based on higher octects.
// The logic to sort by last octet is implemented to
// prefer CIDRs with higher octects, this in-turn skips the
// localhost/loopback address to be not preferred as the
// first ip on the list. Subsequently this list helps us print
// a user friendly message with appropriate values.
func sortIPs(ipList []string) []string {
	if len(ipList) == 1 {
		return ipList
	}

	var ipV4s []net.IP
	var nonIPs []string
	for _, ip := range ipList {
		nip := net.ParseIP(ip)
		if nip != nil {
			ipV4s = append(ipV4s, nip)
		} else {
			nonIPs = append(nonIPs, ip)
		}
	}

	sort.Sort(byLastOctetValue(ipV4s))

	var ips []string
	for _, ip := range ipV4s {
		ips = append(ips, ip.String())
	}

	return append(nonIPs, ips...)
}

func getAPIEndpoints(serverAddr string) (apiEndpoints []string) {
	host, port := mustSplitHostPort(serverAddr)

	var ipList []string
	if host == "" {
		ipList = sortIPs(localIP4.ToSlice())
	} else {
		ipList = []string{host}
	}

	for _, ip := range ipList {
		apiEndpoints = append(apiEndpoints, fmt.Sprintf("%s://%s:%s", getURLScheme(globalIsSSL), ip, port))
	}

	return apiEndpoints
}

// checkPortAvailability - check if given port is already in use.
// Note: The check method tries to listen on given port and closes it.
// It is possible to have a disconnected client in this tiny window of time.
func checkPortAvailability(port string) (err error) {
	// Return true if err is "address already in use" error.
	isAddrInUseErr := func(err error) (b bool) {
		if opErr, ok := err.(*net.OpError); ok {
			if sysErr, ok := opErr.Err.(*os.SyscallError); ok {
				if errno, ok := sysErr.Err.(syscall.Errno); ok {
					b = (errno == syscall.EADDRINUSE)
				}
			}
		}

		return b
	}

	network := []string{"tcp", "tcp4", "tcp6"}
	for _, n := range network {
		l, err := net.Listen(n, net.JoinHostPort("", port))
		if err == nil {
			// As we are able to listen on this network, the port is not in use.
			// Close the listener and continue check other networks.
			if err = l.Close(); err != nil {
				return err
			}
		} else if isAddrInUseErr(err) {
			// As we got EADDRINUSE error, the port is in use by other process.
			// Return the error.
			return err
		}
	}

	return nil
}

// isLocalHost - checks if the given parameter
// correspond to one of the local IP of the
// current machine
func isLocalHost(host string) (bool, error) {
	if host == net.IPv4zero.String() {
		return true, nil
	}

	hostIPs, err := getHostIP4(host)
	if err != nil {
		return false, err
	}

	// If intersection of two IP sets is not empty, then the host is local host.
	isLocal := !localIP4.Intersection(hostIPs).IsEmpty()
	return isLocal, nil
}

// parseServerHost - checks if address is valid and local host.
func parseServerHost(address string) (*xnet.Host, error) {
	if strings.HasPrefix(address, ":") {
		address = "0.0.0.0" + address
	}

	host, err := xnet.ParseHost(address)
	if err != nil {
		return nil, err
	}

	if !host.IsPortSet {
		return nil, errors.New("port number missing")
	}

	// No need to check if host is '0.0.0.0'
	if host.Host != net.IPv4zero.String() {
		isLocalHost, err := isLocalHost(host.Host)
		if err != nil {
			return nil, err
		}
		if !isLocalHost {
			return nil, errors.New("host in server address should be this server")
		}
	}

	if host.PortNumber == 0 {
		return nil, errors.New("port number must be non-zero")
	}

	return host, nil
}
