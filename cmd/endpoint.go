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
	"path"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
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
	IsLocal bool
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

// SetHTTPS - sets secure http for URLEndpointType.
func (endpoint Endpoint) SetHTTPS() {
	if endpoint.Host != "" {
		endpoint.Scheme = "https"
	}
}

// SetHTTP - sets insecure http for URLEndpointType.
func (endpoint Endpoint) SetHTTP() {
	if endpoint.Host != "" {
		endpoint.Scheme = "http"
	}
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (Endpoint, error) {
	// isEmptyPath - check whether given path is not empty.
	isEmptyPath := func(path string) bool {
		return path == "" || path == "." || path == "/" || path == `\`
	}

	if isEmptyPath(arg) {
		return Endpoint{}, fmt.Errorf("empty or root endpoint is not supported")
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
			return Endpoint{}, fmt.Errorf("invalid URL endpoint format")
		}

		host, port, err := splitHostPort(u.Host)
		if err != nil {
			return Endpoint{}, fmt.Errorf("invalid URL endpoint format: %s", err)
		}
		if host == "" {
			return Endpoint{}, fmt.Errorf("invalid URL endpoint format: empty host name")
		}

		// As this is path in the URL, we should use path package, not filepath package.
		// On MS Windows, filepath.Clean() converts into Windows path style ie `/foo` becomes `\foo`
		u.Path = path.Clean(u.Path)
		if isEmptyPath(u.Path) {
			return Endpoint{}, fmt.Errorf("empty or root path is not supported in URL endpoint")
		}

		// Get IPv4 address of the host.
		hostIPs, err := getHostIP4(host)
		if err != nil {
			return Endpoint{}, err
		}

		// If intersection of two IP sets is not empty, then the host is local host.
		isLocal = !localIP4.Intersection(hostIPs).IsEmpty()
		u.Host = net.JoinHostPort(host, port)
	} else {
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

// Swap - helper method for sorting.
func (endpoints EndpointList) Swap(i, j int) {
	endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
}

// Len - helper method for sorting.
func (endpoints EndpointList) Len() int {
	return len(endpoints)
}

// Less - helper method for sorting.
func (endpoints EndpointList) Less(i, j int) bool {
	return endpoints[i].String() < endpoints[j].String()
}

// SetHTTPS - sets secure http for URLEndpointType.
func (endpoints EndpointList) SetHTTPS() {
	for i := range endpoints {
		endpoints[i].SetHTTPS()
	}
}

// SetHTTP - sets insecure http for URLEndpointType.
func (endpoints EndpointList) SetHTTP() {
	for i := range endpoints {
		endpoints[i].SetHTTP()
	}
}

// NewEndpointList - returns new endpoint list based on input args.
func NewEndpointList(args ...string) (endpoints EndpointList, err error) {
	// isValidDistribution - checks whether given count is a valid distribution for erasure coding.
	isValidDistribution := func(count int) bool {
		return (count >= 4 && count <= 16 && count%2 == 0)
	}

	// Check whether no. of args are valid for XL distribution.
	if !isValidDistribution(len(args)) {
		return nil, fmt.Errorf("total endpoints %d found. For XL/Distribute, it should be 4, 6, 8, 10, 12, 14 or 16", len(args))
	}

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

	sort.Sort(endpoints)

	return endpoints, nil
}

// CreateEndpoints - validates and creates new endpoints for given args.
func CreateEndpoints(serverAddr string, args ...string) (string, EndpointList, SetupType, error) {
	var endpoints EndpointList
	var setupType SetupType
	var err error

	// Check whether serverAddr is valid for this host.
	isServerAddrEmpty := (serverAddr == "")
	if !isServerAddrEmpty {
		if err = CheckLocalServerAddr(serverAddr); err != nil {
			return serverAddr, endpoints, setupType, err
		}
	}

	// Normalize server address.
	serverAddrHost, serverAddrPort := mustSplitHostPort(serverAddr)
	serverAddr = net.JoinHostPort(serverAddrHost, serverAddrPort)

	// For single arg, return FS setup.
	if len(args) == 1 {
		var endpoint Endpoint
		if endpoint, err = NewEndpoint(args[0]); err == nil {
			if endpoint.Type() == PathEndpointType {
				endpoints = append(endpoints, endpoint)
				setupType = FSSetupType
			} else {
				err = fmt.Errorf("use path style endpoint for FS setup")
			}
		}

		return serverAddr, endpoints, setupType, err
	}

	// Convert args to endpoints
	if endpoints, err = NewEndpointList(args...); err != nil {
		return serverAddr, endpoints, setupType, err
	}

	// Return XL setup when all endpoints are path style.
	if endpoints[0].Type() == PathEndpointType {
		setupType = XLSetupType
		return serverAddr, endpoints, setupType, nil
	}

	// Here all endpoints are URL style.

	// Error out if same path is exported by different ports on same server.
	{
		hostPathMap := make(map[string]set.StringSet)
		for _, endpoint := range endpoints {
			hostPort := endpoint.Host
			path := endpoint.Path
			host, _ := mustSplitHostPort(hostPort)

			pathSet, ok := hostPathMap[host]
			if !ok {
				pathSet = set.NewStringSet()
			}

			if pathSet.Contains(path) {
				return serverAddr, endpoints, setupType, fmt.Errorf("same path can not be served from different port")
			}

			pathSet.Add(path)
			hostPathMap[host] = pathSet
		}
	}

	// Normalized args is used below if URL style endpoint is used for XL.
	normArgs := make([]string, len(args))

	// Get unique hosts.
	var uniqueHosts []string
	{
		sset := set.NewStringSet()
		for i, endpoint := range endpoints {
			sset.Add(endpoint.Host)
			normArgs[i] = endpoint.Path
		}
		uniqueHosts = sset.ToSlice()
	}

	// URL style endpoints are used for XL.
	if len(uniqueHosts) == 1 {
		endpoint := endpoints[0]
		if !endpoint.IsLocal {
			err = fmt.Errorf("no endpoint found for this host")
			return serverAddr, endpoints, setupType, err
		}

		// Normalize endpoint's host ie endpoint's host might not have port, if so use default port.
		endpointHost := uniqueHosts[0]
		portFound := true
		host, port, err := net.SplitHostPort(endpointHost)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				// This should not happen as we already constructed good endpoint.
				fatalIf(err, "unexpected error when splitting endpoint's host port %s", endpointHost)
			}

			portFound = false
			port = defaultPort
			endpointHost = net.JoinHostPort(host, port)
		}

		if isServerAddrEmpty {
			serverAddr = endpointHost
		} else if portFound {
			// As serverAddr is given, serverAddr and endpoint should have same port.
			if serverAddrPort != port {
				return serverAddr, endpoints, setupType, fmt.Errorf("server address and endpoint have different ports")
			}
		}

		endpoints, _ = NewEndpointList(normArgs...)
		setupType = XLSetupType
		return serverAddr, endpoints, setupType, nil
	}

	isXLSetup := true
	localhostPort := ""
	var uniqueLocalHosts []string
	{
		uniqueLocalHostSet := set.NewStringSet()
		for i, endpoint := range endpoints {
			if endpoint.IsLocal {
				uniqueLocalHostSet.Add(endpoint.Host)

				_, port := mustSplitHostPort(endpoint.Host)
				if i == 0 {
					localhostPort = port
				} else if localhostPort != port {
					// Eventhough this endpoint is for local host, but previously
					// assigned port of localhost and this endpoint's port is different.
					// This means that not all endpoints are local ie not a XL setup.
					isXLSetup = false
				}
			} else {
				isXLSetup = false
			}
		}

		// Error out if no endpoint for this server.
		if uniqueLocalHostSet.IsEmpty() {
			return serverAddr, endpoints, setupType, fmt.Errorf("no endpoint found for this host")
		}

		uniqueLocalHosts = uniqueLocalHostSet.ToSlice()
	}

	if isXLSetup {
		// TODO: In this case, we bind to 0.0.0.0 ie to all interfaces.
		// The actual way to do is bind to only IPs in uniqueLocalHosts.
		endpoints, _ = NewEndpointList(normArgs...)
		serverAddr = net.JoinHostPort("", localhostPort)
		setupType = XLSetupType
		return serverAddr, endpoints, setupType, nil
	}

	// This is Distribute setup.
	if len(uniqueLocalHosts) == 1 {
		host, port := mustSplitHostPort(uniqueLocalHosts[0])
		if isServerAddrEmpty {
			serverAddr = net.JoinHostPort(host, port)
		} else {
			// As serverAddr is given, serverAddr and endpoint should have same port.
			if serverAddrPort != port {
				return serverAddr, endpoints, setupType, fmt.Errorf("server address and endpoint have different ports")
			}
		}
	} else {
		// If length of uniqueLocalHosts is more than one,
		// server address should be present with same port with the same or empty hostname.
		if isServerAddrEmpty {
			return serverAddr, endpoints, setupType, fmt.Errorf("for more than one endpoints for local host with different port, server address must be provided")
		}

		// Check if port in server address matches to port in at least one unique local hosts.
		found := false
		for _, host := range uniqueLocalHosts {
			_, port := mustSplitHostPort(host)
			if serverAddrPort == port {
				found = true
				break
			}
		}

		if !found {
			return serverAddr, endpoints, setupType, fmt.Errorf("port in server address does not match with local endpoints")
		}
	}

	setupType = DistXLSetupType
	return serverAddr, endpoints, setupType, nil
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
