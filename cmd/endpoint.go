// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mountinfo"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
)

// EndpointType - enum for endpoint type.
type EndpointType int

const (
	// PathEndpointType - path style endpoint type enum.
	PathEndpointType EndpointType = iota + 1

	// URLEndpointType - URL style endpoint type enum.
	URLEndpointType
)

// ProxyEndpoint - endpoint used for proxy redirects
// See proxyRequest() for details.
type ProxyEndpoint struct {
	Endpoint
	Transport http.RoundTripper
}

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

// HTTPS - returns true if secure for URLEndpointType.
func (endpoint Endpoint) HTTPS() bool {
	return endpoint.Scheme == "https"
}

// UpdateIsLocal - resolves the host and updates if it is local or not.
func (endpoint *Endpoint) UpdateIsLocal() (err error) {
	if !endpoint.IsLocal {
		endpoint.IsLocal, err = isLocalHost(endpoint.Hostname(), endpoint.Port(), globalMinioPort)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewEndpoint - returns new endpoint based on given arguments.
func NewEndpoint(arg string) (ep Endpoint, e error) {
	// isEmptyPath - check whether given path is not empty.
	isEmptyPath := func(path string) bool {
		return path == "" || path == SlashSeparator || path == `\`
	}

	if isEmptyPath(arg) {
		return ep, fmt.Errorf("empty or root endpoint is not supported")
	}

	var isLocal bool
	var host string
	u, err := url.Parse(arg)
	if err == nil && u.Host != "" {
		// URL style of endpoint.
		// Valid URL style endpoint is
		// - Scheme field must contain "http" or "https"
		// - All field should be empty except Host and Path.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			u.User == nil && u.Opaque == "" && !u.ForceQuery && u.RawQuery == "" && u.Fragment == "") {
			return ep, fmt.Errorf("invalid URL endpoint format")
		}

		var port string
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			if !strings.Contains(err.Error(), "missing port in address") {
				return ep, fmt.Errorf("invalid URL endpoint format: %w", err)
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

		// On windows having a preceding SlashSeparator will cause problems, if the
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

	} else {
		// Only check if the arg is an ip address and ask for scheme since its absent.
		// localhost, example.com, any FQDN cannot be disambiguated from a regular file path such as
		// /mnt/export1. So we go ahead and start the minio server in FS modes in these cases.
		if isHostIP(arg) {
			return ep, fmt.Errorf("invalid URL endpoint format: missing scheme http or https")
		}
		absArg, err := filepath.Abs(arg)
		if err != nil {
			return Endpoint{}, fmt.Errorf("absolute path failed %s", err)
		}
		u = &url.URL{Path: path.Clean(absArg)}
		isLocal = true
	}

	return Endpoint{
		URL:     u,
		IsLocal: isLocal,
	}, nil
}

// PoolEndpoints represent endpoints in a given pool
// along with its setCount and setDriveCount.
type PoolEndpoints struct {
	SetCount     int
	DrivesPerSet int
	Endpoints    Endpoints
}

// EndpointServerPools - list of list of endpoints
type EndpointServerPools []PoolEndpoints

// GetLocalPoolIdx returns the pool which endpoint belongs to locally.
// if ep is remote this code will return -1 poolIndex
func (l EndpointServerPools) GetLocalPoolIdx(ep Endpoint) int {
	for i, zep := range l {
		for _, cep := range zep.Endpoints {
			if cep.IsLocal && ep.IsLocal {
				if reflect.DeepEqual(cep, ep) {
					return i
				}
			}
		}
	}
	return -1
}

// Add add pool endpoints
func (l *EndpointServerPools) Add(zeps PoolEndpoints) error {
	existSet := set.NewStringSet()
	for _, zep := range *l {
		for _, ep := range zep.Endpoints {
			existSet.Add(ep.String())
		}
	}
	// Validate if there are duplicate endpoints across serverPools
	for _, ep := range zeps.Endpoints {
		if existSet.Contains(ep.String()) {
			return fmt.Errorf("duplicate endpoints found")
		}
	}
	*l = append(*l, zeps)
	return nil
}

// Localhost - returns the local hostname from list of endpoints
func (l EndpointServerPools) Localhost() string {
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				u := &url.URL{
					Scheme: endpoint.Scheme,
					Host:   endpoint.Host,
				}
				return u.String()
			}
		}
	}
	return ""
}

// LocalDisksPaths returns the disk paths of the local disks
func (l EndpointServerPools) LocalDisksPaths() []string {
	var disks []string
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				disks = append(disks, endpoint.Path)
			}
		}
	}
	return disks
}

// FirstLocal returns true if the first endpoint is local.
func (l EndpointServerPools) FirstLocal() bool {
	return l[0].Endpoints[0].IsLocal
}

// HTTPS - returns true if secure for URLEndpointType.
func (l EndpointServerPools) HTTPS() bool {
	return l[0].Endpoints.HTTPS()
}

// NEndpoints - returns all nodes count
func (l EndpointServerPools) NEndpoints() (count int) {
	for _, ep := range l {
		count += len(ep.Endpoints)
	}
	return count
}

// Hostnames - returns list of unique hostnames
func (l EndpointServerPools) Hostnames() []string {
	foundSet := set.NewStringSet()
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if foundSet.Contains(endpoint.Hostname()) {
				continue
			}
			foundSet.Add(endpoint.Hostname())
		}
	}
	return foundSet.ToSlice()
}

// hostsSorted will return all hosts found.
// The LOCAL host will be nil, but the indexes of all hosts should
// remain consistent across the cluster.
func (l EndpointServerPools) hostsSorted() []*xnet.Host {
	peers, localPeer := l.peers()
	sort.Strings(peers)
	hosts := make([]*xnet.Host, len(peers))
	for i, hostStr := range peers {
		if hostStr == localPeer {
			continue
		}
		host, err := xnet.ParseHost(hostStr)
		if err != nil {
			logger.LogIf(GlobalContext, err)
			continue
		}
		hosts[i] = host
	}

	return hosts
}

// peers will return all peers, including local.
// The local peer is returned as a separate string.
func (l EndpointServerPools) peers() (peers []string, local string) {
	allSet := set.NewStringSet()
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.Type() != URLEndpointType {
				continue
			}

			peer := endpoint.Host
			if endpoint.IsLocal {
				if _, port := mustSplitHostPort(peer); port == globalMinioPort {
					local = peer
				}
			}

			allSet.Add(peer)
		}
	}

	return allSet.ToSlice(), local
}

// Endpoints - list of same type of endpoint.
type Endpoints []Endpoint

// HTTPS - returns true if secure for URLEndpointType.
func (endpoints Endpoints) HTTPS() bool {
	return endpoints[0].HTTPS()
}

// GetString - returns endpoint string of i-th endpoint (0-based),
// and empty string for invalid indexes.
func (endpoints Endpoints) GetString(i int) string {
	if i < 0 || i >= len(endpoints) {
		return ""
	}
	return endpoints[i].String()
}

// GetAllStrings - returns allstring of all endpoints
func (endpoints Endpoints) GetAllStrings() (all []string) {
	for _, e := range endpoints {
		all = append(all, e.String())
	}
	return
}

func hostResolveToLocalhost(endpoint Endpoint) bool {
	hostIPs, err := getHostIP(endpoint.Hostname())
	if err != nil {
		// Log the message to console about the host resolving
		reqInfo := (&logger.ReqInfo{}).AppendTags(
			"host",
			endpoint.Hostname(),
		)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err, logger.Application)
		return false
	}
	var loopback int
	for _, hostIP := range hostIPs.ToSlice() {
		if net.ParseIP(hostIP).IsLoopback() {
			loopback++
		}
	}
	return loopback == len(hostIPs)
}

func (endpoints Endpoints) atleastOneEndpointLocal() bool {
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			return true
		}
	}
	return false
}

// UpdateIsLocal - resolves the host and discovers the local host.
func (endpoints Endpoints) UpdateIsLocal(foundPrevLocal bool) error {
	orchestrated := IsDocker() || IsKubernetes()
	k8sReplicaSet := IsKubernetesReplicaSet()

	var epsResolved int
	var foundLocal bool
	resolvedList := make([]bool, len(endpoints))
	// Mark the starting time
	startTime := time.Now()
	keepAliveTicker := time.NewTicker(10 * time.Millisecond)
	defer keepAliveTicker.Stop()
	for {
		// Break if the local endpoint is found already Or all the endpoints are resolved.
		if foundLocal || (epsResolved == len(endpoints)) {
			break
		}
		// Retry infinitely on Kubernetes and Docker swarm.
		// This is needed as the remote hosts are sometime
		// not available immediately.
		select {
		case <-globalOSSignalCh:
			return fmt.Errorf("The endpoint resolution got interrupted")
		default:
			for i, resolved := range resolvedList {
				if resolved {
					// Continue if host is already resolved.
					continue
				}

				// Log the message to console about the host resolving
				reqInfo := (&logger.ReqInfo{}).AppendTags(
					"host",
					endpoints[i].Hostname(),
				)

				if k8sReplicaSet && hostResolveToLocalhost(endpoints[i]) {
					err := fmt.Errorf("host %s resolves to 127.*, DNS incorrectly configured retrying",
						endpoints[i])
					// time elapsed
					timeElapsed := time.Since(startTime)
					// log error only if more than 1s elapsed
					if timeElapsed > time.Second {
						reqInfo.AppendTags("elapsedTime",
							humanize.RelTime(startTime,
								startTime.Add(timeElapsed),
								"elapsed",
								""))
						ctx := logger.SetReqInfo(GlobalContext, reqInfo)
						logger.LogIf(ctx, err, logger.Application)
					}
					continue
				}

				// return err if not Docker or Kubernetes
				// We use IsDocker() to check for Docker environment
				// We use IsKubernetes() to check for Kubernetes environment
				isLocal, err := isLocalHost(endpoints[i].Hostname(),
					endpoints[i].Port(),
					globalMinioPort,
				)
				if err != nil && !orchestrated {
					return err
				}
				if err != nil {
					// time elapsed
					timeElapsed := time.Since(startTime)
					// log error only if more than 1s elapsed
					if timeElapsed > time.Second {
						reqInfo.AppendTags("elapsedTime",
							humanize.RelTime(startTime,
								startTime.Add(timeElapsed),
								"elapsed",
								"",
							))
						ctx := logger.SetReqInfo(GlobalContext,
							reqInfo)
						logger.LogIf(ctx, err, logger.Application)
					}
				} else {
					resolvedList[i] = true
					endpoints[i].IsLocal = isLocal
					if k8sReplicaSet && !endpoints.atleastOneEndpointLocal() && !foundPrevLocal {
						// In replicated set in k8s deployment, IPs might
						// get resolved for older IPs, add this code
						// to ensure that we wait for this server to
						// participate atleast one disk and be local.
						//
						// In special cases for replica set with expanded
						// pool setups we need to make sure to provide
						// value of foundPrevLocal from pool1 if we already
						// found a local setup. Only if we haven't found
						// previous local we continue to wait to look for
						// atleast one local.
						resolvedList[i] = false
						// time elapsed
						err := fmt.Errorf("no endpoint is local to this host: %s", endpoints[i])
						timeElapsed := time.Since(startTime)
						// log error only if more than 1s elapsed
						if timeElapsed > time.Second {
							reqInfo.AppendTags("elapsedTime",
								humanize.RelTime(startTime,
									startTime.Add(timeElapsed),
									"elapsed",
									"",
								))
							ctx := logger.SetReqInfo(GlobalContext,
								reqInfo)
							logger.LogIf(ctx, err, logger.Application)
						}
						continue
					}
					epsResolved++
					if !foundLocal {
						foundLocal = isLocal
					}
				}
			}

			// Wait for the tick, if the there exist a local endpoint in discovery.
			// Non docker/kubernetes environment we do not need to wait.
			if !foundLocal && orchestrated {
				<-keepAliveTicker.C
			}
		}
	}

	// On Kubernetes/Docker setups DNS resolves inappropriately sometimes
	// where there are situations same endpoints with multiple disks
	// come online indicating either one of them is local and some
	// of them are not local. This situation can never happen and
	// its only a possibility in orchestrated deployments with dynamic
	// DNS. Following code ensures that we treat if one of the endpoint
	// says its local for a given host - it is true for all endpoints
	// for the same host. Following code ensures that this assumption
	// is true and it works in all scenarios and it is safe to assume
	// for a given host.
	endpointLocalMap := make(map[string]bool)
	for _, ep := range endpoints {
		if ep.IsLocal {
			endpointLocalMap[ep.Host] = ep.IsLocal
		}
	}
	for i := range endpoints {
		endpoints[i].IsLocal = endpointLocalMap[endpoints[i].Host]
	}
	return nil
}

// NewEndpoints - returns new endpoint list based on input args.
func NewEndpoints(args ...string) (endpoints Endpoints, err error) {
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
func checkCrossDeviceMounts(endpoints Endpoints) (err error) {
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
func CreateEndpoints(serverAddr string, foundLocal bool, args ...[]string) (Endpoints, SetupType, error) {
	var endpoints Endpoints
	var setupType SetupType
	var err error

	// Check whether serverAddr is valid for this host.
	if err = CheckLocalServerAddr(serverAddr); err != nil {
		return endpoints, setupType, err
	}

	_, serverAddrPort := mustSplitHostPort(serverAddr)

	// For single arg, return FS setup.
	if len(args) == 1 && len(args[0]) == 1 {
		var endpoint Endpoint
		endpoint, err = NewEndpoint(args[0][0])
		if err != nil {
			return endpoints, setupType, err
		}
		if err := endpoint.UpdateIsLocal(); err != nil {
			return endpoints, setupType, err
		}
		if endpoint.Type() != PathEndpointType {
			return endpoints, setupType, config.ErrInvalidFSEndpoint(nil).Msg("use path style endpoint for FS setup")
		}
		endpoints = append(endpoints, endpoint)
		setupType = FSSetupType

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(endpoints); err != nil {
			return endpoints, setupType, config.ErrInvalidFSEndpoint(nil).Msg(err.Error())
		}

		return endpoints, setupType, nil
	}

	for _, iargs := range args {
		// Convert args to endpoints
		eps, err := NewEndpoints(iargs...)
		if err != nil {
			return endpoints, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
		}

		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(eps); err != nil {
			return endpoints, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
		}

		endpoints = append(endpoints, eps...)
	}

	if len(endpoints) == 0 {
		return endpoints, setupType, config.ErrInvalidErasureEndpoints(nil).Msg("invalid number of endpoints")
	}

	// Return Erasure setup when all endpoints are path style.
	if endpoints[0].Type() == PathEndpointType {
		setupType = ErasureSetupType
		return endpoints, setupType, nil
	}

	if err = endpoints.UpdateIsLocal(foundLocal); err != nil {
		return endpoints, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
	}

	// Here all endpoints are URL style.
	endpointPathSet := set.NewStringSet()
	localEndpointCount := 0
	localServerHostSet := set.NewStringSet()
	localPortSet := set.NewStringSet()

	for _, endpoint := range endpoints {
		endpointPathSet.Add(endpoint.Path)
		if endpoint.IsLocal {
			localServerHostSet.Add(endpoint.Hostname())

			var port string
			_, port, err = net.SplitHostPort(endpoint.Host)
			if err != nil {
				port = serverAddrPort
			}
			localPortSet.Add(port)

			localEndpointCount++
		}
	}

	// Check whether same path is not used in endpoints of a host on different port.
	{
		pathIPMap := make(map[string]set.StringSet)
		for _, endpoint := range endpoints {
			host := endpoint.Hostname()
			hostIPSet, _ := getHostIP(host)
			if IPSet, ok := pathIPMap[endpoint.Path]; ok {
				if !IPSet.Intersection(hostIPSet).IsEmpty() {
					return endpoints, setupType,
						config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("path '%s' can not be served by different port on same address", endpoint.Path))
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
				return endpoints, setupType,
					config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("path '%s' cannot be served by different address on same server", endpoint.Path))
			}
			localPathSet.Add(endpoint.Path)
		}
	}

	// All endpoints are pointing to local host
	if len(endpoints) == localEndpointCount {
		// If all endpoints have same port number, Just treat it as local erasure setup
		// using URL style endpoints.
		if len(localPortSet) == 1 {
			if len(localServerHostSet) > 1 {
				return endpoints, setupType,
					config.ErrInvalidErasureEndpoints(nil).Msg("all local endpoints should not have different hostnames/ips")
			}
			return endpoints, ErasureSetupType, nil
		}

		// Even though all endpoints are local, but those endpoints use different ports.
		// This means it is DistErasure setup.
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

	// Error out if we have less than 2 unique servers.
	if len(uniqueArgs.ToSlice()) < 2 && setupType == DistErasureSetupType {
		err := fmt.Errorf("Unsupported number of endpoints (%s), minimum number of servers cannot be less than 2 in distributed setup", endpoints)
		return endpoints, setupType, err
	}

	publicIPs := env.Get(config.EnvPublicIPs, "")
	if len(publicIPs) == 0 {
		updateDomainIPs(uniqueArgs)
	}

	setupType = DistErasureSetupType
	return endpoints, setupType, nil
}

// GetLocalPeer - returns local peer value, returns globalMinioAddr
// for FS and Erasure mode. In case of distributed server return
// the first element from the set of peers which indicate that
// they are local. There is always one entry that is local
// even with repeated server endpoints.
func GetLocalPeer(endpointServerPools EndpointServerPools, host, port string) (localPeer string) {
	peerSet := set.NewStringSet()
	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if endpoint.Type() != URLEndpointType {
				continue
			}
			if endpoint.IsLocal && endpoint.Host != "" {
				peerSet.Add(endpoint.Host)
			}
		}
	}
	if peerSet.IsEmpty() {
		// Local peer can be empty in FS or Erasure coded mode.
		// If so, return globalMinioHost + globalMinioPort value.
		if host != "" {
			return net.JoinHostPort(host, port)
		}

		return net.JoinHostPort("127.0.0.1", port)
	}
	return peerSet.ToSlice()[0]
}

// GetProxyEndpointLocalIndex returns index of the local proxy endpoint
func GetProxyEndpointLocalIndex(proxyEps []ProxyEndpoint) int {
	for i, pep := range proxyEps {
		if pep.IsLocal {
			return i
		}
	}
	return -1
}

func httpDo(clnt *http.Client, req *http.Request, f func(*http.Response, error) error) error {
	ctx, cancel := context.WithTimeout(GlobalContext, 200*time.Millisecond)
	defer cancel()

	// Run the HTTP request in a goroutine and pass the response to f.
	c := make(chan error, 1)
	req = req.WithContext(ctx)
	go func() { c <- f(clnt.Do(req)) }()
	select {
	case <-ctx.Done():
		<-c // Wait for f to return.
		return ctx.Err()
	case err := <-c:
		return err
	}
}

func getOnlineProxyEndpointIdx() int {
	type reqIndex struct {
		Request *http.Request
		Idx     int
	}

	proxyRequests := make(map[*http.Client]reqIndex, len(globalProxyEndpoints))
	for i, proxyEp := range globalProxyEndpoints {
		proxyEp := proxyEp
		serverURL := &url.URL{
			Scheme: proxyEp.Scheme,
			Host:   proxyEp.Host,
			Path:   pathJoin(healthCheckPathPrefix, healthCheckLivenessPath),
		}

		req, err := http.NewRequest(http.MethodGet, serverURL.String(), nil)
		if err != nil {
			continue
		}

		proxyRequests[&http.Client{
			Transport: proxyEp.Transport,
		}] = reqIndex{
			Request: req,
			Idx:     i,
		}
	}

	for c, r := range proxyRequests {
		if err := httpDo(c, r.Request, func(resp *http.Response, err error) error {
			if err != nil {
				return err
			}
			xhttp.DrainBody(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return errors.New(resp.Status)
			}
			if v := resp.Header.Get(xhttp.MinIOServerStatus); v == unavailable {
				return errors.New(v)
			}
			return nil
		}); err != nil {
			continue
		}
		return r.Idx
	}
	return -1
}

// GetProxyEndpoints - get all endpoints that can be used to proxy list request.
func GetProxyEndpoints(endpointServerPools EndpointServerPools) []ProxyEndpoint {
	var proxyEps []ProxyEndpoint

	proxyEpSet := set.NewStringSet()

	for _, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if endpoint.Type() != URLEndpointType {
				continue
			}

			host := endpoint.Host
			if proxyEpSet.Contains(host) {
				continue
			}
			proxyEpSet.Add(host)

			proxyEps = append(proxyEps, ProxyEndpoint{
				Endpoint:  endpoint,
				Transport: globalProxyTransport,
			})
		}
	}
	return proxyEps
}

func updateDomainIPs(endPoints set.StringSet) {
	ipList := set.NewStringSet()
	for e := range endPoints {
		host, port, err := net.SplitHostPort(e)
		if err != nil {
			if strings.Contains(err.Error(), "missing port in address") {
				host = e
				port = globalMinioPort
			} else {
				continue
			}
		}

		if net.ParseIP(host) == nil {
			IPs, err := getHostIP(host)
			if err != nil {
				continue
			}

			IPsWithPort := IPs.ApplyFunc(func(ip string) string {
				return net.JoinHostPort(ip, port)
			})

			ipList = ipList.Union(IPsWithPort)
		}

		ipList.Add(net.JoinHostPort(host, port))
	}

	globalDomainIPs = ipList.FuncMatch(func(ip string, matchString string) bool {
		host, _, err := net.SplitHostPort(ip)
		if err != nil {
			host = ip
		}
		return !net.ParseIP(host).IsLoopback() && host != "localhost"
	}, "")
}
