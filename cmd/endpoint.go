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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mountinfo"
)

// EndpointType - enum for endpoint type.
type EndpointType int

const (
	// PathEndpointType - path style endpoint type enum.
	PathEndpointType EndpointType = iota + 1

	// URLEndpointType - URL style endpoint type enum.
	URLEndpointType
)

// Endpoint - any type of endpoint.
type Endpoint struct {
	*url.URL
	IsLocal  bool
	SetIndex int
}

func (endpoint Endpoint) String() string {
	if endpoint.Host == "" {
		return endpoint.Path
	}

	return endpoint.URL.String()
}

// Type - returns type of endpoint.
func (endpoint Endpoint) Type() EndpointType {
	if endpoint.Host == "" {
		return PathEndpointType
	}

	return URLEndpointType
}

// IsHTTPS - returns true if secure for URLEndpointType.
func (endpoint Endpoint) IsHTTPS() bool {
	return endpoint.Scheme == "https"
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (ep Endpoint, e error) {
	// isEmptyPath - check whether given path is not empty.
	isEmptyPath := func(path string) bool {
		return path == "" || path == "/" || path == `\`
	}

	if isEmptyPath(arg) {
		return ep, fmt.Errorf("empty or root endpoint is not supported")
	}

	var isLocal bool
	u, err := url.Parse(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		// Valid URL style endpoint is
		// - Scheme field must contain "http" or "https"
		// - All field should be empty except Host and Path.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.User == nil && u.Opaque == "" && u.ForceQuery == false && u.RawQuery == "" && u.Fragment == "") {
			return ep, fmt.Errorf("invalid URL endpoint format")
		}

		var host, port string
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				return ep, fmt.Errorf("invalid URL endpoint format: %s", err)
			}

			host = u.Host
		} else {
			var p int
			p, err = strconv.Atoi(port)
			if err != nil {
				return ep, fmt.Errorf("invalid URL endpoint format: invalid port number")
			} else if p < 1 || p > 65535 {
				return ep, fmt.Errorf("invalid URL endpoint format: port number must be between 1 to 65535")
			}
		}
		if i := strings.Index(host, "%"); i > -1 {
			host = host[:i]
		}

		if host == "" {
			return ep, fmt.Errorf("invalid URL endpoint format: empty host name")
		}

		// As this is path in the URL, we should use path package, not filepath package.
		// On MS Windows, filepath.Clean() converts into Windows path style ie `/foo` becomes `\foo`
		u.Path = path.Clean(u.Path)
		if isEmptyPath(u.Path) {
			return ep, fmt.Errorf("empty or root path is not supported in URL endpoint")
		}

		// On windows having a preceding "/" will cause problems, if the
		// command line already has C:/<export-folder/ in it. Final resulting
		// path on windows might become C:/C:/ this will cause problems
		// of starting minio server properly in distributed mode on windows.
		// As a special case make sure to trim the separator.

		// NOTE: It is also perfectly fine for windows users to have a path
		// without C:/ since at that point we treat it as relative path
		// and obtain the full filesystem path as well. Providing C:/
		// style is necessary to provide paths other than C:/,
		// such as F:/, D:/ etc.
		//
		// Another additional benefit here is that this style also
		// supports providing \\host\share support as well.
		if runtime.GOOS == globalWindowsOSName {
			if filepath.VolumeName(u.Path[1:]) != "" {
				u.Path = u.Path[1:]
			}
		}

		isLocal, err = isLocalHost(host)
		if err != nil {
			return ep, err
		}
	} else {
		// Only check if the arg is an ip address and ask for scheme since its absent.
		// localhost, example.com, any FQDN cannot be disambiguated from a regular file path such as
		// /mnt/export1. So we go ahead and start the minio server in FS modes in these cases.
		if isHostIP(arg) {
			return ep, fmt.Errorf("invalid URL endpoint format: missing scheme http or https")
		}
		u = &url.URL{Path: path.Clean(arg)}
		isLocal = true
	}

	return Endpoint{
		URL:     u,
		IsLocal: isLocal,
	}, nil
}

// EndpointList - list of same type of endpoint.
type EndpointList []Endpoint

// Nodes - returns number of unique servers.
func (endpoints EndpointList) Nodes() int {
	uniqueNodes := set.NewStringSet()
	for _, endpoint := range endpoints {
		if uniqueNodes.Contains(endpoint.Host) {
			continue
		}
		uniqueNodes.Add(endpoint.Host)
	}
	return len(uniqueNodes)
}

// IsHTTPS - returns true if secure for URLEndpointType.
func (endpoints EndpointList) IsHTTPS() bool {
	return endpoints[0].IsHTTPS()
}

// GetString - returns endpoint string of i-th endpoint (0-based),
// and empty string for invalid indexes.
func (endpoints EndpointList) GetString(i int) string {
	if i < 0 || i >= len(endpoints) {
		return ""
	}
	return endpoints[i].String()
}

// NewEndpointList - returns new endpoint list based on input args.
func NewEndpointList(args ...string) (endpoints EndpointList, err error) {
	var endpointType EndpointType
	var scheme string

	uniqueArgs := set.NewStringSet()
	// Loop through args and adds to endpoint list.
	for i, arg := range args {
		endpoint, err := NewEndpoint(arg)
		if err != nil {
			return nil, fmt.Errorf("'%s': %s", arg, err.Error())
		}

		// All endpoints have to be same type and scheme if applicable.
		if i == 0 {
			endpointType = endpoint.Type()
			scheme = endpoint.Scheme
		} else if endpoint.Type() != endpointType {
			return nil, fmt.Errorf("mixed style endpoints are not supported")
		} else if endpoint.Scheme != scheme {
			return nil, fmt.Errorf("mixed scheme is not supported")
		}

		arg = endpoint.String()
		if uniqueArgs.Contains(arg) {
			return nil, fmt.Errorf("duplicate endpoints found")
		}
		uniqueArgs.Add(arg)
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}

// Checks if there are any cross device mounts.
func checkCrossDeviceMounts(endpoints EndpointList) (err error) {
	var absPaths []string
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			var absPath string
			absPath, err = filepath.Abs(endpoint.Path)
			if err != nil {
				return err
			}
			absPaths = append(absPaths, absPath)
		}
	}
	return mountinfo.CheckCrossDevice(absPaths)
}

// CreateEndpoints - validates and creates new endpoints for given args.
func CreateEndpoints(serverAddr string, args ...[]string) (string, EndpointList, SetupType, error) {
	var endpoints EndpointList
	var setupType SetupType
	var err error

	// Check whether serverAddr is valid for this host.
	if err = CheckLocalServerAddr(serverAddr); err != nil {
		return serverAddr, endpoints, setupType, err
	}

	_, serverAddrPort := mustSplitHostPort(serverAddr)

	// For single arg, return FS setup.
	if len(args) == 1 && len(args[0]) == 1 {
		var endpoint Endpoint
		endpoint, err = NewEndpoint(args[0][0])
		if err != nil {
			return serverAddr, endpoints, setupType, err
		}
		if endpoint.Type() != PathEndpointType {
			return serverAddr, endpoints, setupType, uiErrInvalidFSEndpoint(nil).Msg("use path style endpoint for FS setup")
		}
		endpoints = append(endpoints, endpoint)
		setupType = FSSetupType

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(endpoints); err != nil {
			return serverAddr, endpoints, setupType, uiErrInvalidFSEndpoint(nil).Msg(err.Error())
		}
		return serverAddr, endpoints, setupType, nil
	}

	for i, iargs := range args {
		var newEndpoints EndpointList
		// Convert args to endpoints
		var eps EndpointList
		eps, err = NewEndpointList(iargs...)
		if err != nil {
			return serverAddr, endpoints, setupType, uiErrInvalidErasureEndpoints(nil).Msg(err.Error())
		}

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(eps); err != nil {
			return serverAddr, endpoints, setupType, uiErrInvalidErasureEndpoints(nil).Msg(err.Error())
		}

		for _, ep := range eps {
			ep.SetIndex = i
			newEndpoints = append(newEndpoints, ep)
		}
		endpoints = append(endpoints, newEndpoints...)
	}

	// Return XL setup when all endpoints are path style.
	if endpoints[0].Type() == PathEndpointType {
		setupType = XLSetupType
		return serverAddr, endpoints, setupType, nil
	}

	// Here all endpoints are URL style.
	endpointPathSet := set.NewStringSet()
	localEndpointCount := 0
	localServerAddrSet := set.NewStringSet()
	localPortSet := set.NewStringSet()

	for _, endpoint := range endpoints {
		endpointPathSet.Add(endpoint.Path)
		if endpoint.IsLocal {
			localServerAddrSet.Add(endpoint.Host)

			var port string
			_, port, err = net.SplitHostPort(endpoint.Host)
			if err != nil {
				port = serverAddrPort
			}

			localPortSet.Add(port)

			localEndpointCount++
		}
	}

	// No local endpoint found.
	if localEndpointCount == 0 {
		return serverAddr, endpoints, setupType, uiErrInvalidErasureEndpoints(nil).Msg("no endpoint pointing to the local machine is found")
	}

	// Check whether same path is not used in endpoints of a host on different port.
	{
		pathIPMap := make(map[string]set.StringSet)
		for _, endpoint := range endpoints {
			var host string
			host, _, err = net.SplitHostPort(endpoint.Host)
			if err != nil {
				host = endpoint.Host
			}
			hostIPSet, _ := getHostIP(host)
			if IPSet, ok := pathIPMap[endpoint.Path]; ok {
				if !IPSet.Intersection(hostIPSet).IsEmpty() {
					return serverAddr, endpoints, setupType,
						uiErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("path '%s' can not be served by different port on same address", endpoint.Path))
				}
				pathIPMap[endpoint.Path] = IPSet.Union(hostIPSet)
			} else {
				pathIPMap[endpoint.Path] = hostIPSet
			}
		}
	}

	// Check whether same path is used for more than 1 local endpoints.
	{
		localPathSet := set.CreateStringSet()
		for _, endpoint := range endpoints {
			if !endpoint.IsLocal {
				continue
			}
			if localPathSet.Contains(endpoint.Path) {
				return serverAddr, endpoints, setupType,
					uiErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("path '%s' cannot be served by different address on same server", endpoint.Path))
			}
			localPathSet.Add(endpoint.Path)
		}
	}

	// Check whether serverAddrPort matches at least in one of port used in local endpoints.
	{
		if !localPortSet.Contains(serverAddrPort) {
			if len(localPortSet) > 1 {
				return serverAddr, endpoints, setupType,
					uiErrInvalidErasureEndpoints(nil).Msg("port number in server address must match with one of the port in local endpoints")
			}
			return serverAddr, endpoints, setupType,
				uiErrInvalidErasureEndpoints(nil).Msg("server address and local endpoint have different ports")
		}
	}

	// All endpoints are pointing to local host
	if len(endpoints) == localEndpointCount {
		// If all endpoints have same port number, then this is XL setup using URL style endpoints.
		if len(localPortSet) == 1 {
			if len(localServerAddrSet) > 1 {
				// TODO: Even though all endpoints are local, the local host is referred by different IP/name.
				// eg '172.0.0.1', 'localhost' and 'mylocalhostname' point to same local host.
				//
				// In this case, we bind to 0.0.0.0 ie to all interfaces.
				// The actual way to do is bind to only IPs in uniqueLocalHosts.
				serverAddr = net.JoinHostPort("", serverAddrPort)
			}

			endpointPaths := endpointPathSet.ToSlice()
			endpoints, _ = NewEndpointList(endpointPaths...)
			setupType = XLSetupType
			return serverAddr, endpoints, setupType, nil
		}

		// Even though all endpoints are local, but those endpoints use different ports.
		// This means it is DistXL setup.
	} else {
		// This is DistXL setup.
		// Check whether local server address are not 127.x.x.x
		for _, localServerAddr := range localServerAddrSet.ToSlice() {
			host, _, err := net.SplitHostPort(localServerAddr)
			if err != nil {
				host = localServerAddr
			}

			ipList, err := getHostIP(host)
			logger.FatalIf(err, "unexpected error when resolving host '%s'", host)

			// Filter ipList by IPs those start with '127.' or '::1'
			loopBackIPs := ipList.FuncMatch(func(ip string, matchString string) bool {
				return strings.HasPrefix(ip, "127.") || strings.HasPrefix(ip, "::1")
			}, "")

			// If loop back IP is found and ipList contains only loop back IPs, then error out.
			if len(loopBackIPs) > 0 && len(loopBackIPs) == len(ipList) {
				err = fmt.Errorf("'%s' resolves to loopback address is not allowed for distributed XL", localServerAddr)
				return serverAddr, endpoints, setupType, err
			}
		}
	}

	// Add missing port in all endpoints.
	for i := range endpoints {
		_, port, err := net.SplitHostPort(endpoints[i].Host)
		if err != nil {
			endpoints[i].Host = net.JoinHostPort(endpoints[i].Host, serverAddrPort)
		} else if endpoints[i].IsLocal && serverAddrPort != port {
			// If endpoint is local, but port is different than serverAddrPort, then make it as remote.
			endpoints[i].IsLocal = false
		}
	}

	uniqueArgs := set.NewStringSet()
	for _, endpoint := range endpoints {
		uniqueArgs.Add(endpoint.Host)
	}

	// Error out if we have more than serverCommandLineArgsMax unique servers.
	if len(uniqueArgs.ToSlice()) > serverCommandLineArgsMax {
		err := fmt.Errorf("Unsupported number of endpoints (%s), total number of servers cannot be more than %d", endpoints, serverCommandLineArgsMax)
		return serverAddr, endpoints, setupType, err
	}

	// Error out if we have less than 2 unique servers.
	if len(uniqueArgs.ToSlice()) < 2 && setupType == DistXLSetupType {
		err := fmt.Errorf("Unsupported number of endpoints (%s), minimum number of servers cannot be less than 2 in distributed setup", endpoints)
		return serverAddr, endpoints, setupType, err
	}

	_, dok := os.LookupEnv("MINIO_DOMAIN")
	_, eok := os.LookupEnv("MINIO_ETCD_ENDPOINTS")
	_, iok := os.LookupEnv("MINIO_PUBLIC_IPS")
	if dok && eok && !iok {
		updateDomainIPs(uniqueArgs)
	}

	setupType = DistXLSetupType
	return serverAddr, endpoints, setupType, nil
}

// GetLocalPeer - returns local peer value, returns globalMinioAddr
// for FS and Erasure mode. In case of distributed server return
// the first element from the set of peers which indicate that
// they are local. There is always one entry that is local
// even with repeated server endpoints.
func GetLocalPeer(endpoints EndpointList) (localPeer string) {
	peerSet := set.NewStringSet()
	for _, endpoint := range endpoints {
		if endpoint.Type() != URLEndpointType {
			continue
		}
		if endpoint.IsLocal && endpoint.Host != "" {
			peerSet.Add(endpoint.Host)
		}
	}
	if peerSet.IsEmpty() {
		// Local peer can be empty in FS or Erasure coded mode.
		// If so, return globalMinioHost + globalMinioPort value.
		if globalMinioHost != "" {
			return net.JoinHostPort(globalMinioHost, globalMinioPort)
		}

		return net.JoinHostPort("127.0.0.1", globalMinioPort)
	}
	return peerSet.ToSlice()[0]
}

// GetRemotePeers - get hosts information other than this minio service.
func GetRemotePeers(endpoints EndpointList) []string {
	peerSet := set.NewStringSet()
	for _, endpoint := range endpoints {
		if endpoint.Type() != URLEndpointType {
			continue
		}

		peer := endpoint.Host
		if endpoint.IsLocal {
			if _, port := mustSplitHostPort(peer); port == globalMinioPort {
				continue
			}
		}

		peerSet.Add(peer)
	}

	return peerSet.ToSlice()
}

func updateDomainIPs(endPoints set.StringSet) {
	ipList := set.NewStringSet()
	for e := range endPoints {
		host, _, err := net.SplitHostPort(e)
		if err != nil {
			if strings.Contains(err.Error(), "missing port in address") {
				host = e
			} else {
				continue
			}
		}
		IPs, _ := getHostIP(host)
		ipList = ipList.Union(IPs)
	}
	globalDomainIPs = ipList.FuncMatch(func(ip string, matchString string) bool {
		return !strings.HasPrefix(ip, "127.") || strings.HasPrefix(ip, "::1")
	}, "")
}
