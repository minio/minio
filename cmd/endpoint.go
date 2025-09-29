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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mountinfo"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
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

// Node holds information about a node in this cluster
type Node struct {
	*url.URL
	Pools    []int
	IsLocal  bool
	GridHost string
}

// Endpoint - any type of endpoint.
type Endpoint struct {
	*url.URL
	IsLocal bool

	PoolIdx, SetIdx, DiskIdx int
}

// Equal returns true if endpoint == ep
func (endpoint Endpoint) Equal(ep Endpoint) bool {
	if endpoint.IsLocal == ep.IsLocal && endpoint.PoolIdx == ep.PoolIdx && endpoint.SetIdx == ep.SetIdx && endpoint.DiskIdx == ep.DiskIdx {
		if endpoint.Path == ep.Path && endpoint.Host == ep.Host {
			return true
		}
	}
	return false
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

// GridHost returns the host to be used for grid connections.
func (endpoint Endpoint) GridHost() string {
	return fmt.Sprintf("%s://%s", endpoint.Scheme, endpoint.Host)
}

// UpdateIsLocal - resolves the host and updates if it is local or not.
func (endpoint *Endpoint) UpdateIsLocal() (err error) {
	if endpoint.Host != "" {
		endpoint.IsLocal, err = isLocalHost(endpoint.Hostname(), endpoint.Port(), globalMinioPort)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetPoolIndex sets a specific pool number to this node
func (endpoint *Endpoint) SetPoolIndex(i int) {
	endpoint.PoolIdx = i
}

// SetSetIndex sets a specific set number to this node
func (endpoint *Endpoint) SetSetIndex(i int) {
	endpoint.SetIdx = i
}

// SetDiskIndex sets a specific disk number to this node
func (endpoint *Endpoint) SetDiskIndex(i int) {
	endpoint.DiskIdx = i
}

func isValidURLEndpoint(u *url.URL) bool {
	// URL style of endpoint.
	// Valid URL style endpoint is
	// - Scheme field must contain "http" or "https"
	// - All field should be empty except Host and Path.
	isURLOk := (u.Scheme == "http" || u.Scheme == "https") &&
		u.User == nil && u.Opaque == "" && !u.ForceQuery &&
		u.RawQuery == "" && u.Fragment == ""
	return isURLOk
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
		if !isValidURLEndpoint(u) {
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
		PoolIdx: -1,
		SetIdx:  -1,
		DiskIdx: -1,
	}, nil
}

// PoolEndpoints represent endpoints in a given pool
// along with its setCount and setDriveCount.
type PoolEndpoints struct {
	// indicates if endpoints are provided in non-ellipses style
	Legacy       bool
	SetCount     int
	DrivesPerSet int
	Endpoints    Endpoints
	CmdLine      string
	Platform     string
}

// EndpointServerPools - list of list of endpoints
type EndpointServerPools []PoolEndpoints

// ESCount returns the total number of erasure sets in this cluster
func (l EndpointServerPools) ESCount() (count int) {
	for _, p := range l {
		count += p.SetCount
	}
	return count
}

// GetNodes returns a sorted list of nodes in this cluster
func (l EndpointServerPools) GetNodes() (nodes []Node) {
	nodesMap := make(map[string]Node)
	for _, pool := range l {
		for _, ep := range pool.Endpoints {
			node, ok := nodesMap[ep.Host]
			if !ok {
				node.IsLocal = ep.IsLocal
				node.URL = &url.URL{
					Scheme: ep.Scheme,
					Host:   ep.Host,
				}
				node.GridHost = ep.GridHost()
			}
			if !slices.Contains(node.Pools, ep.PoolIdx) {
				node.Pools = append(node.Pools, ep.PoolIdx)
			}
			nodesMap[ep.Host] = node
		}
	}
	nodes = make([]Node, 0, len(nodesMap))
	for _, v := range nodesMap {
		nodes = append(nodes, v)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Host < nodes[j].Host
	})
	return nodes
}

// GetPoolIdx return pool index
func (l EndpointServerPools) GetPoolIdx(pool string) int {
	for id, ep := range globalEndpoints {
		if ep.CmdLine != pool {
			continue
		}
		return id
	}
	return -1
}

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

// Legacy returns 'true' if the MinIO server commandline was
// provided with no ellipses pattern, those are considered
// legacy deployments.
func (l EndpointServerPools) Legacy() bool {
	return len(l) == 1 && l[0].Legacy
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
			if endpoint.IsLocal && endpoint.Host != "" {
				u := &url.URL{
					Scheme: endpoint.Scheme,
					Host:   endpoint.Host,
				}
				return u.String()
			}
		}
	}
	host := globalMinioHost
	if host == "" {
		host = sortIPs(localIP4.ToSlice())[0]
	}
	return fmt.Sprintf("%s://%s", getURLScheme(globalIsTLS), net.JoinHostPort(host, globalMinioPort))
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

// NLocalDisksPathsPerPool returns the disk paths of the local disks per pool
func (l EndpointServerPools) NLocalDisksPathsPerPool() []int {
	localDisksCount := make([]int, len(l))
	for i, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				localDisksCount[i]++
			}
		}
	}
	return localDisksCount
}

// FirstLocal returns true if the first endpoint is local.
func (l EndpointServerPools) FirstLocal() bool {
	return l[0].Endpoints[0].IsLocal
}

// HTTPS - returns true if secure for URLEndpointType.
func (l EndpointServerPools) HTTPS() bool {
	return l[0].Endpoints.HTTPS()
}

// NEndpoints - returns number of endpoints
func (l EndpointServerPools) NEndpoints() (count int) {
	for _, ep := range l {
		count += len(ep.Endpoints)
	}
	return count
}

// GridHosts will return all peers, including local.
// in websocket grid compatible format, The local peer
// is returned as a separate string.
func (l EndpointServerPools) GridHosts() (gridHosts []string, gridLocal string) {
	seenHosts := set.NewStringSet()
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			u := endpoint.GridHost()
			if seenHosts.Contains(u) {
				continue
			}
			seenHosts.Add(u)

			// Set local endpoint
			if endpoint.IsLocal {
				gridLocal = u
			}

			gridHosts = append(gridHosts, u)
		}
	}

	return gridHosts, gridLocal
}

// FindGridHostsFromPeerPool will return a matching peerPool from provided peer (as string)
func (l EndpointServerPools) FindGridHostsFromPeerPool(peer string) []int {
	if peer == "" {
		return nil
	}

	var pools []int
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				continue
			}

			if !slices.Contains(pools, endpoint.PoolIdx) {
				pools = append(pools, endpoint.PoolIdx)
			}
		}
	}

	return pools
}

// FindGridHostsFromPeerStr will return a matching peer from provided peer (as string)
func (l EndpointServerPools) FindGridHostsFromPeerStr(peer string) (peerGrid string) {
	if peer == "" {
		return ""
	}
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				continue
			}

			if endpoint.Host == peer {
				return endpoint.GridHost()
			}
		}
	}
	return ""
}

// FindGridHostsFromPeer will return a matching peer from provided peer.
func (l EndpointServerPools) FindGridHostsFromPeer(peer *xnet.Host) (peerGrid string) {
	if peer == nil {
		return ""
	}
	for _, ep := range l {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				continue
			}
			host, err := xnet.ParseHost(endpoint.Host)
			if err != nil {
				continue
			}

			if host.String() == peer.String() {
				return endpoint.GridHost()
			}
		}
	}
	return ""
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
			internalLogIf(GlobalContext, err)
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
					if local == "" {
						local = peer
					}
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
	return all
}

func hostResolveToLocalhost(endpoint Endpoint) bool {
	hostIPs, err := getHostIP(endpoint.Hostname())
	if err != nil {
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

// UpdateIsLocal - resolves the host and discovers the local host.
func (endpoints Endpoints) UpdateIsLocal() error {
	var epsResolved int
	var foundLocal bool
	resolvedList := make([]bool, len(endpoints))
	// Mark the starting time
	startTime := time.Now()
	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()
	for !foundLocal && (epsResolved != len(endpoints)) {
		// Break if the local endpoint is found already Or all the endpoints are resolved.

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

				if endpoints[i].Host == "" {
					resolvedList[i] = true
					endpoints[i].IsLocal = true
					epsResolved++
					if !foundLocal {
						foundLocal = true
					}
					continue
				}

				// Log the message to console about the host resolving
				reqInfo := (&logger.ReqInfo{}).AppendTags(
					"host",
					endpoints[i].Hostname(),
				)

				if orchestrated && hostResolveToLocalhost(endpoints[i]) {
					// time elapsed
					timeElapsed := time.Since(startTime)
					// log error only if more than a second has elapsed
					if timeElapsed > time.Second {
						reqInfo.AppendTags("elapsedTime",
							humanize.RelTime(startTime,
								startTime.Add(timeElapsed),
								"elapsed",
								"",
							))
						ctx := logger.SetReqInfo(GlobalContext,
							reqInfo)
						bootLogOnceIf(ctx, fmt.Errorf("%s resolves to localhost in a containerized deployment, waiting for it to resolve to a valid IP",
							endpoints[i].Hostname()), endpoints[i].Hostname(), logger.ErrorKind)
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
					// log error only if more than a second has elapsed
					if timeElapsed > time.Second {
						reqInfo.AppendTags("elapsedTime",
							humanize.RelTime(startTime,
								startTime.Add(timeElapsed),
								"elapsed",
								"",
							))
						ctx := logger.SetReqInfo(GlobalContext,
							reqInfo)
						bootLogOnceIf(ctx, err, endpoints[i].Hostname(), logger.ErrorKind)
					}
				} else {
					resolvedList[i] = true
					endpoints[i].IsLocal = isLocal
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
		//nolint:gocritic
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

// PoolEndpointList is a temporary type to holds the list of endpoints
type PoolEndpointList []Endpoints

// UpdateIsLocal - resolves all hosts and discovers which are local
func (p PoolEndpointList) UpdateIsLocal() error {
	var epsResolved int
	var epCount int

	for _, endpoints := range p {
		epCount += len(endpoints)
	}

	var foundLocal bool
	resolvedList := make(map[Endpoint]bool)

	// Mark the starting time
	startTime := time.Now()
	keepAliveTicker := time.NewTicker(1 * time.Second)
	defer keepAliveTicker.Stop()
	for !foundLocal && (epsResolved != epCount) {
		// Break if the local endpoint is found already Or all the endpoints are resolved.

		// Retry infinitely on Kubernetes and Docker swarm.
		// This is needed as the remote hosts are sometime
		// not available immediately.
		select {
		case <-globalOSSignalCh:
			return fmt.Errorf("The endpoint resolution got interrupted")
		default:
			for i, endpoints := range p {
				for j, endpoint := range endpoints {
					if resolvedList[endpoint] {
						// Continue if host is already resolved.
						continue
					}

					if endpoint.Host == "" || (orchestrated && env.Get("_MINIO_SERVER_LOCAL", "") == endpoint.Host) {
						if !foundLocal {
							foundLocal = true
						}
						endpoint.IsLocal = true
						endpoints[j] = endpoint
						epsResolved++
						resolvedList[endpoint] = true
						continue
					}

					// Log the message to console about the host resolving
					reqInfo := (&logger.ReqInfo{}).AppendTags(
						"host",
						endpoint.Hostname(),
					)

					if orchestrated && hostResolveToLocalhost(endpoint) {
						// time elapsed
						timeElapsed := time.Since(startTime)
						// log error only if more than a second has elapsed
						if timeElapsed > time.Second {
							reqInfo.AppendTags("elapsedTime",
								humanize.RelTime(startTime,
									startTime.Add(timeElapsed),
									"elapsed",
									"",
								))
							ctx := logger.SetReqInfo(GlobalContext,
								reqInfo)
							bootLogOnceIf(ctx, fmt.Errorf("%s resolves to localhost in a containerized deployment, waiting for it to resolve to a valid IP",
								endpoint.Hostname()), endpoint.Hostname(), logger.ErrorKind)
						}
						continue
					}

					// return err if not Docker or Kubernetes
					// We use IsDocker() to check for Docker environment
					// We use IsKubernetes() to check for Kubernetes environment
					isLocal, err := isLocalHost(endpoint.Hostname(),
						endpoint.Port(),
						globalMinioPort,
					)
					if err != nil && !orchestrated {
						return err
					}
					if err != nil {
						// time elapsed
						timeElapsed := time.Since(startTime)
						// log error only if more than a second has elapsed
						if timeElapsed > time.Second {
							reqInfo.AppendTags("elapsedTime",
								humanize.RelTime(startTime,
									startTime.Add(timeElapsed),
									"elapsed",
									"",
								))
							ctx := logger.SetReqInfo(GlobalContext,
								reqInfo)
							bootLogOnceIf(ctx, fmt.Errorf("Unable to resolve DNS for %s: %w", endpoint, err), endpoint.Hostname(), logger.ErrorKind)
						}
					} else {
						resolvedList[endpoint] = true
						endpoint.IsLocal = isLocal
						epsResolved++
						if !foundLocal {
							foundLocal = isLocal
						}
						endpoints[j] = endpoint
					}
				}

				p[i] = endpoints

				// Wait for the tick, if the there exist a local endpoint in discovery.
				// Non docker/kubernetes environment we do not need to wait.
				if !foundLocal && orchestrated {
					<-keepAliveTicker.C
				}
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
	for i, endpoints := range p {
		endpointLocalMap := make(map[string]bool)
		for _, ep := range endpoints {
			if ep.IsLocal {
				endpointLocalMap[ep.Host] = ep.IsLocal
			}
		}
		for i := range endpoints {
			endpoints[i].IsLocal = endpointLocalMap[endpoints[i].Host]
		}
		p[i] = endpoints
	}

	return nil
}

func isEmptyLayout(poolsLayout ...poolDisksLayout) bool {
	return len(poolsLayout) == 0 || len(poolsLayout[0].layout) == 0 || len(poolsLayout[0].layout[0]) == 0 || len(poolsLayout[0].layout[0][0]) == 0
}

func isSingleDriveLayout(poolsLayout ...poolDisksLayout) bool {
	return len(poolsLayout) == 1 && len(poolsLayout[0].layout) == 1 && len(poolsLayout[0].layout[0]) == 1
}

// CreatePoolEndpoints creates a list of endpoints per pool, resolves their relevant hostnames and
// discovers those are local or remote.
func CreatePoolEndpoints(serverAddr string, poolsLayout ...poolDisksLayout) ([]Endpoints, SetupType, error) {
	var setupType SetupType

	if isEmptyLayout(poolsLayout...) {
		return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg("invalid number of endpoints")
	}

	// Check whether serverAddr is valid for this host.
	if err := CheckLocalServerAddr(serverAddr); err != nil {
		return nil, setupType, err
	}

	_, serverAddrPort := mustSplitHostPort(serverAddr)

	poolEndpoints := make(PoolEndpointList, len(poolsLayout))

	// For single arg, return single drive EC setup.
	if isSingleDriveLayout(poolsLayout...) {
		endpoint, err := NewEndpoint(poolsLayout[0].layout[0][0])
		if err != nil {
			return nil, setupType, err
		}
		if err := endpoint.UpdateIsLocal(); err != nil {
			return nil, setupType, err
		}
		if endpoint.Type() != PathEndpointType {
			return nil, setupType, config.ErrInvalidEndpoint(nil).Msg("use path style endpoint for single node setup")
		}

		endpoint.SetPoolIndex(0)
		endpoint.SetSetIndex(0)
		endpoint.SetDiskIndex(0)

		var endpoints Endpoints
		endpoints = append(endpoints, endpoint)
		setupType = ErasureSDSetupType

		poolEndpoints[0] = endpoints
		// Check for cross device mounts if any.
		if err = checkCrossDeviceMounts(endpoints); err != nil {
			return nil, setupType, config.ErrInvalidEndpoint(nil).Msg(err.Error())
		}

		return poolEndpoints, setupType, nil
	}

	uniqueArgs := set.NewStringSet()

	for poolIdx, pool := range poolsLayout {
		var endpoints Endpoints
		for setIdx, setLayout := range pool.layout {
			// Convert args to endpoints
			eps, err := NewEndpoints(setLayout...)
			if err != nil {
				return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
			}

			// Check for cross device mounts if any.
			if err = checkCrossDeviceMounts(eps); err != nil {
				return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
			}

			for diskIdx := range eps {
				eps[diskIdx].SetPoolIndex(poolIdx)
				eps[diskIdx].SetSetIndex(setIdx)
				eps[diskIdx].SetDiskIndex(diskIdx)
			}

			endpoints = append(endpoints, eps...)
		}

		if len(endpoints) == 0 {
			return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg("invalid number of endpoints")
		}

		poolEndpoints[poolIdx] = endpoints
	}

	if err := poolEndpoints.UpdateIsLocal(); err != nil {
		return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
	}

	for i, endpoints := range poolEndpoints {
		// Here all endpoints are URL style.
		endpointPathSet := set.NewStringSet()
		localEndpointCount := 0
		localServerHostSet := set.NewStringSet()
		localPortSet := set.NewStringSet()

		for _, endpoint := range endpoints {
			endpointPathSet.Add(endpoint.Path)
			if endpoint.IsLocal && endpoint.Host != "" {
				localServerHostSet.Add(endpoint.Hostname())

				_, port, err := net.SplitHostPort(endpoint.Host)
				if err != nil {
					port = serverAddrPort
				}
				localPortSet.Add(port)

				localEndpointCount++
			}
		}

		reverseProxy := (env.Get("_MINIO_REVERSE_PROXY", "") != "") && ((env.Get("MINIO_CI_CD", "") != "") || (env.Get("CI", "") != ""))
		// If not orchestrated
		// and not setup in reverse proxy
		if !orchestrated && !reverseProxy {
			// Check whether same path is not used in endpoints of a host on different port.
			// Only verify this on baremetal setups, DNS is not available in orchestrated
			// environments so we can't do much here.
			pathIPMap := make(map[string]set.StringSet)
			hostIPCache := make(map[string]set.StringSet)
			for _, endpoint := range endpoints {
				host := endpoint.Hostname()
				var hostIPSet set.StringSet
				if host != "" {
					var ok bool
					hostIPSet, ok = hostIPCache[host]
					if !ok {
						var err error
						hostIPSet, err = getHostIP(host)
						if err != nil {
							return nil, setupType, config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("host '%s' cannot resolve: %s", host, err))
						}
						hostIPCache[host] = hostIPSet
					}
				}
				if IPSet, ok := pathIPMap[endpoint.Path]; ok {
					if !IPSet.Intersection(hostIPSet).IsEmpty() {
						return nil, setupType,
							config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("same path '%s' can not be served by different port on same address", endpoint.Path))
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
					return nil, setupType,
						config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("path '%s' cannot be served by different address on same server", endpoint.Path))
				}
				localPathSet.Add(endpoint.Path)
			}
		}

		// Add missing port in all endpoints.
		for i := range endpoints {
			if endpoints[i].Host != "" {
				_, port, err := net.SplitHostPort(endpoints[i].Host)
				if err != nil {
					endpoints[i].Host = net.JoinHostPort(endpoints[i].Host, serverAddrPort)
				} else if endpoints[i].IsLocal && serverAddrPort != port {
					// If endpoint is local, but port is different than serverAddrPort, then make it as remote.
					endpoints[i].IsLocal = false
				}
			}
		}

		// All endpoints are pointing to local host
		if len(endpoints) == localEndpointCount {
			// If all endpoints have same port number, Just treat it as local erasure setup
			// using URL style endpoints.
			if len(localPortSet) == 1 {
				if len(localServerHostSet) > 1 {
					return nil, setupType,
						config.ErrInvalidErasureEndpoints(nil).Msg("all local endpoints should not have different hostnames/ips")
				}
			}

			// Even though all endpoints are local, but those endpoints use different ports.
			// This means it is DistErasure setup.
		}

		for _, endpoint := range endpoints {
			if endpoint.Host != "" {
				uniqueArgs.Add(endpoint.Host)
			} else {
				uniqueArgs.Add(net.JoinHostPort("localhost", serverAddrPort))
			}
		}

		poolEndpoints[i] = endpoints
	}

	publicIPs := env.Get(config.EnvPublicIPs, "")
	if len(publicIPs) == 0 {
		updateDomainIPs(uniqueArgs)
	}

	erasureType := len(uniqueArgs.ToSlice()) == 1

	for _, endpoints := range poolEndpoints {
		// Return Erasure setup when all endpoints are path style.
		if endpoints[0].Type() == PathEndpointType {
			setupType = ErasureSetupType
			break
		}
		if endpoints[0].Type() == URLEndpointType {
			if erasureType {
				setupType = ErasureSetupType
			} else {
				setupType = DistErasureSetupType
			}
			break
		}
	}

	return poolEndpoints, setupType, nil
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

// GetProxyEndpoints - get all endpoints that can be used to proxy list request.
func GetProxyEndpoints(endpointServerPools EndpointServerPools, transport http.RoundTripper) []ProxyEndpoint {
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
				Transport: transport,
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
