/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"net/url"
	"sort"
	"strings"
	"syscall"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
)

// IPv4 addresses of local host.
var localIP4 = mustGetLocalIP4()

// mustSplitHostPort is a wrapper to net.SplitHostPort() where error is assumed to be a fatal.
func mustSplitHostPort(hostPort string) (host, port string) {
	xh, err := xnet.ParseHost(hostPort)
	if err != nil {
		logger.FatalIf(err, "Unable to split host port %s", hostPort)
	}
	return xh.Name, xh.Port.String()
}

// mustGetLocalIP4 returns IPv4 addresses of localhost.  It panics on error.
func mustGetLocalIP4() (ipList set.StringSet) {
	ipList = set.NewStringSet()
	addrs, err := net.InterfaceAddrs()
	logger.FatalIf(err, "Unable to get IP addresses of this host")

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

// mustGetLocalIP6 returns IPv6 addresses of localhost.  It panics on error.
func mustGetLocalIP6() (ipList set.StringSet) {
	ipList = set.NewStringSet()
	addrs, err := net.InterfaceAddrs()
	logger.FatalIf(err, "Unable to get IP addresses of this host")

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip.To4() == nil {
			ipList.Add(ip.String())
		}
	}

	return ipList
}

// getHostIP returns IP address of given host.
func getHostIP(host string) (ipList set.StringSet, err error) {
	var ips []net.IP

	if ips, err = net.LookupIP(host); err != nil {
		return ipList, err
	}

	ipList = set.NewStringSet()
	for _, ip := range ips {
		ipList.Add(ip.String())
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
	if n[i].IsLoopback() {
		return false
	}
	if n[j].IsLoopback() {
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

func getAPIEndpoints() (apiEndpoints []string) {
	var ipList []string
	if globalMinioHost == "" {
		ipList = sortIPs(mustGetLocalIP4().ToSlice())
		ipList = append(ipList, mustGetLocalIP6().ToSlice()...)
	} else {
		ipList = []string{globalMinioHost}
	}

	for _, ip := range ipList {
		endpoint := fmt.Sprintf("%s://%s", getURLScheme(globalIsSSL), net.JoinHostPort(ip, globalMinioPort))
		apiEndpoints = append(apiEndpoints, endpoint)
	}

	return apiEndpoints
}

// isHostIP - helper for validating if the provided arg is an ip address.
func isHostIP(ipAddress string) bool {
	host, _, err := net.SplitHostPort(ipAddress)
	if err != nil {
		host = ipAddress
	}
	// Strip off IPv6 zone information.
	if i := strings.Index(host, "%"); i > -1 {
		host = host[:i]
	}
	return net.ParseIP(host) != nil
}

// checkPortAvailability - check if given host and port is already in use.
// Note: The check method tries to listen on given port and closes it.
// It is possible to have a disconnected client in this tiny window of time.
func checkPortAvailability(host, port string) (err error) {
	network := []string{"tcp", "tcp4", "tcp6"}
	for _, n := range network {
		l, err := net.Listen(n, net.JoinHostPort(host, port))
		if err == nil {
			// As we are able to listen on this network, the port is not in use.
			// Close the listener and continue check other networks.
			if err = l.Close(); err != nil {
				return err
			}
		} else if errors.Is(err, syscall.EADDRINUSE) {
			// As we got EADDRINUSE error, the port is in use by other process.
			// Return the error.
			return err
		}
	}

	return nil
}

// extractHostPort - extracts host/port from many address formats
// such as, ":9000", "localhost:9000", "http://localhost:9000/"
func extractHostPort(hostAddr string) (string, string, error) {
	var addr, scheme string

	if hostAddr == "" {
		return "", "", errors.New("unable to process empty address")
	}

	// Simplify the work of url.Parse() and always send a url with
	if !strings.HasPrefix(hostAddr, "http://") && !strings.HasPrefix(hostAddr, "https://") {
		hostAddr = "//" + hostAddr
	}

	// Parse address to extract host and scheme field
	u, err := url.Parse(hostAddr)
	if err != nil {
		return "", "", err
	}

	addr = u.Host
	scheme = u.Scheme

	// Use the given parameter again if url.Parse()
	// didn't return any useful result.
	if addr == "" {
		addr = hostAddr
		scheme = "http"
	}

	// At this point, addr can be one of the following form:
	//	":9000"
	//	"localhost:9000"
	//	"localhost" <- in this case, we check for scheme

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		if !strings.Contains(err.Error(), "missing port in address") {
			return "", "", err
		}

		host = addr

		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			return "", "", errors.New("unable to guess port from scheme")
		}
	}

	return host, port, nil
}

// isLocalHost - checks if the given parameter
// correspond to one of the local IP of the
// current machine
func isLocalHost(host string, port string, localPort string) (bool, error) {
	hostIPs, err := getHostIP(host)
	if err != nil {
		return false, err
	}

	nonInterIPV4s := mustGetLocalIP4().Intersection(hostIPs)
	if nonInterIPV4s.IsEmpty() {
		hostIPs = hostIPs.ApplyFunc(func(ip string) string {
			if net.ParseIP(ip).IsLoopback() {
				// Any loopback IP which is not 127.0.0.1
				// convert it to check for intersections.
				return "127.0.0.1"
			}
			return ip
		})
		nonInterIPV4s = mustGetLocalIP4().Intersection(hostIPs)
	}
	nonInterIPV6s := mustGetLocalIP6().Intersection(hostIPs)

	// If intersection of two IP sets is not empty, then the host is localhost.
	isLocalv4 := !nonInterIPV4s.IsEmpty()
	isLocalv6 := !nonInterIPV6s.IsEmpty()
	if port != "" {
		return (isLocalv4 || isLocalv6) && (port == localPort), nil
	}
	return isLocalv4 || isLocalv6, nil
}

// sameLocalAddrs - returns true if two addresses, even with different
// formats, point to the same machine, e.g:
//  ':9000' and 'http://localhost:9000/' will return true
func sameLocalAddrs(addr1, addr2 string) (bool, error) {

	// Extract host & port from given parameters
	host1, port1, err := extractHostPort(addr1)
	if err != nil {
		return false, err
	}
	host2, port2, err := extractHostPort(addr2)
	if err != nil {
		return false, err
	}

	var addr1Local, addr2Local bool

	if host1 == "" {
		// If empty host means it is localhost
		addr1Local = true
	} else {
		// Host not empty, check if it is local
		if addr1Local, err = isLocalHost(host1, port1, port1); err != nil {
			return false, err
		}
	}

	if host2 == "" {
		// If empty host means it is localhost
		addr2Local = true
	} else {
		// Host not empty, check if it is local
		if addr2Local, err = isLocalHost(host2, port2, port2); err != nil {
			return false, err
		}
	}

	// If both of addresses point to the same machine, check if
	// have the same port
	if addr1Local && addr2Local {
		if port1 == port2 {
			return true, nil
		}
	}
	return false, nil
}

// CheckLocalServerAddr - checks if serverAddr is valid and local host.
func CheckLocalServerAddr(serverAddr string) error {
	host, err := xnet.ParseHost(serverAddr)
	if err != nil {
		return config.ErrInvalidAddressFlag(err)
	}

	// 0.0.0.0 is a wildcard address and refers to local network
	// addresses. I.e, 0.0.0.0:9000 like ":9000" refers to port
	// 9000 on localhost.
	if host.Name != "" && host.Name != net.IPv4zero.String() && host.Name != net.IPv6zero.String() {
		localHost, err := isLocalHost(host.Name, host.Port.String(), host.Port.String())
		if err != nil {
			return err
		}
		if !localHost {
			return config.ErrInvalidAddressFlag(nil).Msg("host in server address should be this server")
		}
	}

	return nil
}
