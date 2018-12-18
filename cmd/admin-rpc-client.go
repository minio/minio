/*
 * Minio Cloud Storage, (C) 2014, 2015, 2016, 2017, 2018 Minio, Inc.
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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
)

var errUnsupportedSignal = fmt.Errorf("unsupported signal: only restart and stop signals are supported")

// AdminRPCClient - admin RPC client talks to admin RPC server.
type AdminRPCClient struct {
	*RPCClient
}

// SignalService - calls SignalService RPC.
func (rpcClient *AdminRPCClient) SignalService(signal serviceSignal) (err error) {
	args := SignalServiceArgs{Sig: signal}
	reply := VoidReply{}

	return rpcClient.Call(adminServiceName+".SignalService", &args, &reply)
}

// ServerInfo - returns the server info of the server to which the RPC call is made.
func (rpcClient *AdminRPCClient) ServerInfo() (sid ServerInfoData, err error) {
	err = rpcClient.Call(adminServiceName+".ServerInfo", &AuthArgs{}, &sid)
	return sid, err
}

// StartProfiling - starts profiling in the remote server.
func (rpcClient *AdminRPCClient) StartProfiling(profiler string) error {
	args := StartProfilingArgs{Profiler: profiler}
	reply := VoidReply{}
	return rpcClient.Call(adminServiceName+".StartProfiling", &args, &reply)
}

// DownloadProfilingData - returns profiling data of the remote server.
func (rpcClient *AdminRPCClient) DownloadProfilingData() ([]byte, error) {
	args := AuthArgs{}
	var reply []byte

	err := rpcClient.Call(adminServiceName+".DownloadProfilingData", &args, &reply)
	return reply, err
}

// NewAdminRPCClient - returns new admin RPC client.
func NewAdminRPCClient(host *xnet.Host) (*AdminRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   adminServicePath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	rpcClient, err := NewRPCClient(
		RPCClientArgs{
			NewAuthTokenFunc: newAuthToken,
			RPCVersion:       globalRPCAPIVersion,
			ServiceName:      adminServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &AdminRPCClient{rpcClient}, nil
}

// adminCmdRunner - abstracts local and remote execution of admin
// commands like service stop and service restart.
type adminCmdRunner interface {
	SignalService(s serviceSignal) error
	ServerInfo() (ServerInfoData, error)
	StartProfiling(string) error
	DownloadProfilingData() ([]byte, error)
}

// adminPeer - represents an entity that implements admin API RPCs.
type adminPeer struct {
	addr      string
	cmdRunner adminCmdRunner
	isLocal   bool
}

// type alias for a collection of adminPeer.
type adminPeers []adminPeer

// makeAdminPeers - helper function to construct a collection of adminPeer.
func makeAdminPeers(endpoints EndpointList) (adminPeerList adminPeers) {
	localAddr := GetLocalPeer(endpoints)
	if strings.HasPrefix(localAddr, "127.0.0.1:") {
		// Use first IPv4 instead of loopback address.
		localAddr = net.JoinHostPort(sortIPs(localIP4.ToSlice())[0], globalMinioPort)
	}
	if strings.HasPrefix(localAddr, "[::1]:") {
		// Use first IPv4 instead of loopback address.
		localAddr = net.JoinHostPort(localIP6.ToSlice()[0], globalMinioPort)
	}

	adminPeerList = append(adminPeerList, adminPeer{
		addr:      localAddr,
		cmdRunner: localAdminClient{},
		isLocal:   true,
	})

	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.FatalIf(err, "Unable to parse Admin RPC Host")
		rpcClient, err := NewAdminRPCClient(host)
		logger.FatalIf(err, "Unable to initialize Admin RPC Client")
		adminPeerList = append(adminPeerList, adminPeer{
			addr:      hostStr,
			cmdRunner: rpcClient,
		})
	}

	return adminPeerList
}

// Initialize global adminPeer collection.
func initGlobalAdminPeers(endpoints EndpointList) {
	globalAdminPeers = makeAdminPeers(endpoints)
}

// invokeServiceCmd - Invoke Restart/Stop command.
func invokeServiceCmd(cp adminPeer, cmd serviceSignal) (err error) {
	switch cmd {
	case serviceRestart, serviceStop:
		err = cp.cmdRunner.SignalService(cmd)
	}
	return err
}

// sendServiceCmd - Invoke Restart command on remote peers
// adminPeer followed by on the local peer.
func sendServiceCmd(cps adminPeers, cmd serviceSignal) {
	// Send service command like stop or restart to all remote nodes and finally run on local node.
	errs := make([]error, len(cps))
	var wg sync.WaitGroup
	remotePeers := cps[1:]
	for i := range remotePeers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// we use idx+1 because remotePeers slice is 1 position shifted w.r.t cps
			errs[idx+1] = invokeServiceCmd(remotePeers[idx], cmd)
		}(i)
	}
	wg.Wait()
	errs[0] = invokeServiceCmd(cps[0], cmd)
}

// uptimeSlice - used to sort uptimes in chronological order.
type uptimeSlice []struct {
	err    error
	uptime time.Duration
}

func (ts uptimeSlice) Len() int {
	return len(ts)
}

func (ts uptimeSlice) Less(i, j int) bool {
	return ts[i].uptime < ts[j].uptime
}

func (ts uptimeSlice) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

// getPeerUptimes - returns the uptime since the last time read quorum
// was established on success. Otherwise returns errXLReadQuorum.
func getPeerUptimes(peers adminPeers) (time.Duration, error) {
	// In a single node Erasure or FS backend setup the uptime of
	// the setup is the uptime of the single minio server
	// instance.
	if !globalIsDistXL {
		return UTCNow().Sub(globalBootTime), nil
	}

	uptimes := make(uptimeSlice, len(peers))

	// Get up time of all servers.
	wg := sync.WaitGroup{}
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, peer adminPeer) {
			defer wg.Done()
			serverInfoData, rpcErr := peer.cmdRunner.ServerInfo()
			uptimes[idx].uptime, uptimes[idx].err = serverInfoData.Properties.Uptime, rpcErr
		}(i, peer)
	}
	wg.Wait()

	// Sort uptimes in chronological order.
	sort.Sort(uptimes)

	// Pick the readQuorum'th uptime in chronological order. i.e,
	// the time at which read quorum was (re-)established.
	readQuorum := len(uptimes) / 2
	validCount := 0
	latestUptime := time.Duration(0)
	for _, uptime := range uptimes {
		if uptime.err != nil {
			logger.LogIf(context.Background(), uptime.err)
			continue
		}

		validCount++
		if validCount >= readQuorum {
			latestUptime = uptime.uptime
			break
		}
	}

	// Less than readQuorum "Admin.Uptime" RPC call returned
	// successfully, so read-quorum unavailable.
	if validCount < readQuorum {
		return time.Duration(0), InsufficientReadQuorum{}
	}

	return latestUptime, nil
}
