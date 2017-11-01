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
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/mountinfo"
	xnet "github.com/minio/minio/pkg/net"
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
	*xnet.URL
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
func NewEndpoint(arg string) (ep Endpoint, e error) {
	// isEmptyPath - check whether given path is not empty.
	isEmptyPath := func(path string) bool {
		return path == "" || path == "/" || path == `\`
	}

	// guessURLStyleError - check whether wrongly formatted URL style value
	guessURLStyleError := func(s string) error {
		if strings.HasPrefix(s, "http:") || strings.HasPrefix(s, "https:") {
			return fmt.Errorf("endpoint URL format must be {http|https}://SERVER[:{1..65535}]/PATH")
		}

		tokens := strings.Split(s, "/")
		if h, err := xnet.ParseHost(tokens[0]); err == nil {
			if h.IsPortSet {
				return fmt.Errorf("invalid URL endpoint format: missing scheme http or https")
			}
		}

		return nil
	}

	if isEmptyPath(arg) {
		return ep, fmt.Errorf("empty or root endpoint is not supported")
	}

	var isLocal bool
	u, err := xnet.ParseURL(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		// Valid URL style endpoint is
		// - Scheme field must contain "http" or "https"
		// - All field should be empty except Host and Path.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.User == nil && u.Opaque == "" && u.ForceQuery == false && u.RawQuery == "" && u.Fragment == "") {
			return ep, fmt.Errorf("invalid URL endpoint format")
		}

		if isEmptyPath(u.Path) {
			return ep, fmt.Errorf("empty or root path is not supported in URL endpoint")
		}

		var h *xnet.Host
		if h, err = xnet.ParseHost(u.Host); err != nil {
			return ep, fmt.Errorf("invalid URL endpoint format: %s", err)
		}

		if isLocal, err = isLocalHost(h.Host); err != nil {
			return ep, err
		}
	} else {
		// path.Clean() is used on purpose because in MS Windows filepath.Clean() converts
		// `/` into `\` ie `/foo` becomes `\foo`
		arg = path.Clean(arg)

		// Check whether wrongly formatted URL style
		if err = guessURLStyleError(arg); err != nil {
			return ep, err
		}

		u = &xnet.URL{Path: arg}
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
		return (count >= minErasureBlocks && count <= maxErasureBlocks && count%2 == 0)
	}

	// Check whether no. of args are valid for XL distribution.
	if !isValidDistribution(len(args)) {
		return nil, fmt.Errorf("A total of %d endpoints were found. For erasure mode it should be an even number between %d and %d", len(args), minErasureBlocks, maxErasureBlocks)
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
func CreateEndpoints(serverHost *xnet.Host, args ...string) (*xnet.Host, EndpointList, SetupType, error) {
	var endpoints EndpointList
	var setupType SetupType
	var err error

	if len(args) == 0 {
		return serverHost, endpoints, setupType, errors.New("empty endpoint arguments")
	}

	// For single arg, return FS setup.
	if len(args) == 1 {
		var endpoint Endpoint
		endpoint, err = NewEndpoint(args[0])
		if err != nil {
			return serverHost, endpoints, setupType, err
		}
		if endpoint.Type() != PathEndpointType {
			return serverHost, endpoints, setupType, fmt.Errorf("use path style endpoint for FS setup")
		}
		endpoints = append(endpoints, endpoint)
		setupType = FSSetupType

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(endpoints); err != nil {
			return serverHost, endpoints, setupType, err
		}
		return serverHost, endpoints, setupType, nil
	}

	// Convert args to endpoints
	if endpoints, err = NewEndpointList(args...); err != nil {
		return serverHost, endpoints, setupType, err
	}

	// Check for cross device mounts if any.
	if err = checkCrossDeviceMounts(endpoints); err != nil {
		return serverHost, endpoints, setupType, err
	}

	// Return XL setup when all endpoints are path style.
	if endpoints[0].Type() == PathEndpointType {
		setupType = XLSetupType
		return serverHost, endpoints, setupType, nil
	}

	// Here all endpoints are URL style.
	endpointPathSet := set.NewStringSet()
	localEndpointCount := 0
	localServerAddrSet := set.NewStringSet()
	localPortSet := make(map[xnet.Port]struct{})

	for _, endpoint := range endpoints {
		endpointPathSet.Add(endpoint.Path)
		if endpoint.IsLocal {
			localServerAddrSet.Add(endpoint.Host)

			host := xnet.MustParseHost(endpoint.Host)
			if !host.IsPortSet {
				host.PortNumber = serverHost.PortNumber
			}
			localPortSet[host.PortNumber] = struct{}{}

			localEndpointCount++
		}
	}

	// No local endpoint found.
	if localEndpointCount == 0 {
		return serverHost, endpoints, setupType, fmt.Errorf("no endpoint found for this host")
	}

	// Check whether same path is not used in endpoints of a host.
	{
		pathIPMap := make(map[string]set.StringSet)
		for _, endpoint := range endpoints {
			host := xnet.MustParseHost(endpoint.Host)
			hostIPSet, _ := getHostIP4(host.Host)
			if IPSet, ok := pathIPMap[endpoint.Path]; ok {
				if !IPSet.Intersection(hostIPSet).IsEmpty() {
					err = fmt.Errorf("path '%s' can not be served by different port on same address", endpoint.Path)
					return serverHost, endpoints, setupType, err
				}

				pathIPMap[endpoint.Path] = IPSet.Union(hostIPSet)
			} else {
				pathIPMap[endpoint.Path] = hostIPSet
			}
		}
	}

	// Check whether serverHost.PortNumber matches at least in one of port used in local endpoints.
	{
		if _, found := localPortSet[serverHost.PortNumber]; !found {
			if len(localPortSet) > 1 {
				err = fmt.Errorf("port number in server address must match with one of the port in local endpoints")
			} else {
				err = fmt.Errorf("server address and local endpoint have different ports")
			}

			return serverHost, endpoints, setupType, err
		}
	}

	// All endpoints are pointing to local host
	if len(endpoints) == localEndpointCount {
		// If all endpoints have same port number, then this is XL setup using URL style endpoints.
		if len(localPortSet) == 1 {
			if len(localServerAddrSet) > 1 {
				// TODO: Eventhough all endpoints are local, the local host is referred by different IP/name.
				// eg '172.0.0.1', 'localhost' and 'mylocalhostname' point to same local host.
				//
				// In this case, we bind to 0.0.0.0 ie to all interfaces.
				// The actual way to do is bind to only IPs in uniqueLocalHosts.
				serverHost.Host = "0.0.0.0"
			}

			endpointPaths := endpointPathSet.ToSlice()
			endpoints, _ = NewEndpointList(endpointPaths...)
			setupType = XLSetupType
			return serverHost, endpoints, setupType, nil
		}

		// Eventhough all endpoints are local, but those endpoints use different ports.
		// This means it is DistXL setup.
	} else {
		// This is DistXL setup.
		// Check whether local server address are not 127.x.x.x
		for _, localServerAddr := range localServerAddrSet.ToSlice() {
			host := xnet.MustParseHost(localServerAddr)
			ipList, err := getHostIP4(host.Host)
			fatalIf(err, "unexpected error when resolving host '%s'", host)

			// Filter ipList by IPs those start with '127.'.
			loopBackIPs := ipList.FuncMatch(func(ip string, matchString string) bool {
				return strings.HasPrefix(ip, "127.")
			}, "")

			// If loop back IP is found and ipList contains only loop back IPs, then error out.
			if len(loopBackIPs) > 0 && len(loopBackIPs) == len(ipList) {
				err = fmt.Errorf("'%s' resolves to loopback address is not allowed for distributed XL", localServerAddr)
				return serverHost, endpoints, setupType, err
			}
		}
	}

	// Add missing port in all endpoints.
	for i := range endpoints {
		host := xnet.MustParseHost(endpoints[i].Host)
		if !host.IsPortSet {
			endpoints[i].Host = endpoints[i].Host + ":" + serverHost.PortNumber.String()
		} else if endpoints[i].IsLocal && serverHost.PortNumber != host.PortNumber {
			// If endpoint is local, but port is different than serverHost.PortNumber, then make it as remote.
			endpoints[i].IsLocal = false
		}
	}

	setupType = DistXLSetupType
	return serverHost, endpoints, setupType, nil
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
		// If local peer is empty can happen in FS or Erasure coded mode.
		// then set the value to globalMinioAddr instead.
		return globalServerHost.String()
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
			host := xnet.MustParseHost(peer)
			if host.PortNumber == globalServerHost.PortNumber {
				continue
			}
		}

		peerSet.Add(peer)
	}

	return peerSet.ToSlice()
}
