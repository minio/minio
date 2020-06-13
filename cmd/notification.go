/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zip"
	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/sync/errgroup"
	"github.com/willf/bloom"
)

// NotificationSys - notification system.
type NotificationSys struct {
	sync.RWMutex
	targetList                 *event.TargetList
	targetResCh                chan event.TargetIDResult
	bucketRulesMap             map[string]event.RulesMap
	bucketRemoteTargetRulesMap map[string]map[event.TargetID]event.RulesMap
	peerClients                []*peerRESTClient
}

// GetARNList - returns available ARNs.
func (sys *NotificationSys) GetARNList(onlyActive bool) []string {
	arns := []string{}
	if sys == nil {
		return arns
	}
	region := globalServerRegion
	for targetID, target := range sys.targetList.TargetMap() {
		// httpclient target is part of ListenBucketNotification
		// which doesn't need to be listed as part of the ARN list
		// This list is only meant for external targets, filter
		// this out pro-actively.
		if !strings.HasPrefix(targetID.ID, "httpclient+") {
			if onlyActive && !target.HasQueueStore() {
				if _, err := target.IsActive(); err != nil {
					continue
				}
			}
			arns = append(arns, targetID.ToARN(region).String())
		}
	}

	return arns
}

// NotificationPeerErr returns error associated for a remote peer.
type NotificationPeerErr struct {
	Host xnet.Host // Remote host on which the rpc call was initiated
	Err  error     // Error returned by the remote peer for an rpc call
}

// A NotificationGroup is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero NotificationGroup is valid and does not cancel on error.
type NotificationGroup struct {
	wg   sync.WaitGroup
	errs []NotificationPeerErr
}

// WithNPeers returns a new NotificationGroup with length of errs slice upto nerrs,
// upon Wait() errors are returned collected from all tasks.
func WithNPeers(nerrs int) *NotificationGroup {
	return &NotificationGroup{errs: make([]NotificationPeerErr, nerrs)}
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the slice of errors from all function calls.
func (g *NotificationGroup) Wait() []NotificationPeerErr {
	g.wg.Wait()
	return g.errs
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error will be
// collected in errs slice and returned by Wait().
func (g *NotificationGroup) Go(ctx context.Context, f func() error, index int, addr xnet.Host) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()
		g.errs[index] = NotificationPeerErr{
			Host: addr,
		}
		for i := 0; i < 3; i++ {
			if err := f(); err != nil {
				g.errs[index].Err = err
				// Last iteration log the error.
				if i == 2 {
					reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", addr.String())
					ctx := logger.SetReqInfo(ctx, reqInfo)
					logger.LogIf(ctx, err)
				}
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
				continue
			}
			break
		}
	}()
}

// ReloadFormat - calls ReloadFormat REST call on all peers.
func (sys *NotificationSys) ReloadFormat(dryRun bool) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.ReloadFormat(dryRun)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DeletePolicy - deletes policy across all peers.
func (sys *NotificationSys) DeletePolicy(policyName string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.DeletePolicy(policyName)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadPolicy - reloads a specific modified policy across all peers
func (sys *NotificationSys) LoadPolicy(policyName string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.LoadPolicy(policyName)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadPolicyMapping - reloads a policy mapping across all peers
func (sys *NotificationSys) LoadPolicyMapping(userOrGroup string, isGroup bool) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.LoadPolicyMapping(userOrGroup, isGroup)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DeleteUser - deletes a specific user across all peers
func (sys *NotificationSys) DeleteUser(accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.DeleteUser(accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadUser - reloads a specific user across all peers
func (sys *NotificationSys) LoadUser(accessKey string, temp bool) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.LoadUser(accessKey, temp)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadGroup - loads a specific group on all peers.
func (sys *NotificationSys) LoadGroup(group string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error { return client.LoadGroup(group) }, idx, *client.host)
	}
	return ng.Wait()
}

// DeleteServiceAccount - deletes a specific service account across all peers
func (sys *NotificationSys) DeleteServiceAccount(accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.DeleteServiceAccount(accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadServiceAccount - reloads a specific service account across all peers
func (sys *NotificationSys) LoadServiceAccount(accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.LoadServiceAccount(accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// BackgroundHealStatus - returns background heal status of all peers
func (sys *NotificationSys) BackgroundHealStatus() []madmin.BgHealState {
	states := make([]madmin.BgHealState, len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		st, err := client.BackgroundHealStatus()
		if err != nil {
			logger.LogIf(GlobalContext, err)
		} else {
			states[idx] = st
		}
	}

	return states
}

// StartProfiling - start profiling on remote peers, by initiating a remote RPC.
func (sys *NotificationSys) StartProfiling(profiler string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.StartProfiling(profiler)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DownloadProfilingData - download profiling data from all remote peers.
func (sys *NotificationSys) DownloadProfilingData(ctx context.Context, writer io.Writer) bool {
	profilingDataFound := false

	// Initialize a zip writer which will provide a zipped content
	// of profiling data of all nodes
	zipWriter := zip.NewWriter(writer)
	defer zipWriter.Close()

	for _, client := range sys.peerClients {
		if client == nil {
			continue
		}
		data, err := client.DownloadProfileData()
		if err != nil {
			reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
			ctx := logger.SetReqInfo(ctx, reqInfo)
			logger.LogIf(ctx, err)
			continue
		}

		profilingDataFound = true

		for typ, data := range data {
			// Send profiling data to zip as file
			header, zerr := zip.FileInfoHeader(dummyFileInfo{
				name:    fmt.Sprintf("profile-%s-%s", client.host.String(), typ),
				size:    int64(len(data)),
				mode:    0600,
				modTime: UTCNow(),
				isDir:   false,
				sys:     nil,
			})
			if zerr != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				logger.LogIf(ctx, zerr)
				continue
			}
			zwriter, zerr := zipWriter.CreateHeader(header)
			if zerr != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				logger.LogIf(ctx, zerr)
				continue
			}
			if _, err = io.Copy(zwriter, bytes.NewBuffer(data)); err != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				logger.LogIf(ctx, err)
				continue
			}
		}
	}

	// Local host
	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		logger.LogIf(ctx, err)
		return profilingDataFound
	}

	data, err := getProfileData()
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", thisAddr.String())
		ctx := logger.SetReqInfo(ctx, reqInfo)
		logger.LogIf(ctx, err)
		return profilingDataFound
	}

	profilingDataFound = true

	// Send profiling data to zip as file
	for typ, data := range data {
		header, zerr := zip.FileInfoHeader(dummyFileInfo{
			name:    fmt.Sprintf("profile-%s-%s", thisAddr, typ),
			size:    int64(len(data)),
			mode:    0600,
			modTime: UTCNow(),
			isDir:   false,
			sys:     nil,
		})
		if zerr != nil {
			return profilingDataFound
		}

		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			return profilingDataFound
		}

		if _, err = io.Copy(zwriter, bytes.NewBuffer(data)); err != nil {
			return profilingDataFound
		}
	}

	return profilingDataFound
}

// ServerUpdate - updates remote peers.
func (sys *NotificationSys) ServerUpdate(updateURL, sha256Hex string, latestReleaseTime time.Time) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.ServerUpdate(updateURL, sha256Hex, latestReleaseTime)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// SignalService - calls signal service RPC call on all peers.
func (sys *NotificationSys) SignalService(sig serviceSignal) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.SignalService(sig)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// updateBloomFilter will cycle all servers to the current index and
// return a merged bloom filter if a complete one can be retrieved.
func (sys *NotificationSys) updateBloomFilter(ctx context.Context, current uint64) (*bloomFilter, error) {
	var req = bloomFilterRequest{
		Current: current,
		Oldest:  current - dataUsageUpdateDirCycles,
	}
	if current < dataUsageUpdateDirCycles {
		req.Oldest = 0
	}

	// Load initial state from local...
	var bf *bloomFilter
	bfr, err := intDataUpdateTracker.cycleFilter(ctx, req.Oldest, req.Current)
	logger.LogIf(ctx, err)
	if err == nil && bfr.Complete {
		nbf := intDataUpdateTracker.newBloomFilter()
		bf = &nbf
		_, err = bf.ReadFrom(bytes.NewBuffer(bfr.Filter))
		logger.LogIf(ctx, err)
	}

	var mu sync.Mutex
	g := errgroup.WithNErrs(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		g.Go(func() error {
			serverBF, err := client.cycleServerBloomFilter(ctx, req)
			if false && intDataUpdateTracker.debug {
				b, _ := json.MarshalIndent(serverBF, "", "  ")
				logger.Info("Disk %v, Bloom filter: %v", client.host.Name, string(b))
			}
			// Keep lock while checking result.
			mu.Lock()
			defer mu.Unlock()

			if err != nil || !serverBF.Complete || bf == nil {
				logger.LogIf(ctx, err)
				bf = nil
				return nil
			}

			var tmp bloom.BloomFilter
			_, err = tmp.ReadFrom(bytes.NewBuffer(serverBF.Filter))
			if err != nil {
				logger.LogIf(ctx, err)
				bf = nil
				return nil
			}
			if bf.BloomFilter == nil {
				bf.BloomFilter = &tmp
			} else {
				err = bf.Merge(&tmp)
				if err != nil {
					logger.LogIf(ctx, err)
					bf = nil
					return nil
				}
			}
			return nil
		}, idx)
	}
	g.Wait()
	return bf, nil
}

// GetLocks - makes GetLocks RPC call on all peers.
func (sys *NotificationSys) GetLocks(ctx context.Context) []*PeerLocks {
	locksResp := make([]*PeerLocks, len(sys.peerClients))
	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// Try to fetch serverInfo remotely in three attempts.
			for i := 0; i < 3; i++ {
				serverLocksResp, err := sys.peerClients[index].GetLocks()
				if err == nil {
					locksResp[index] = &PeerLocks{
						Addr:  sys.peerClients[index].host.String(),
						Locks: serverLocksResp,
					}
					return nil
				}

				// Last iteration log the error.
				if i == 2 {
					return err
				}
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
			}
			return nil
		}, index)
	}
	for index, err := range g.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress",
			sys.peerClients[index].host.String())
		ctx := logger.SetReqInfo(ctx, reqInfo)
		logger.LogOnceIf(ctx, err, sys.peerClients[index].host.String())
	}
	return locksResp
}

// LoadBucketMetadata - calls LoadBucketMetadata call on all peers
func (sys *NotificationSys) LoadBucketMetadata(ctx context.Context, bucketName string) {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.LoadBucketMetadata(bucketName)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err)
		}
	}
}

// DeleteBucketMetadata - calls DeleteBucketMetadata call on all peers
func (sys *NotificationSys) DeleteBucketMetadata(ctx context.Context, bucketName string) {
	globalBucketMetadataSys.Remove(bucketName)

	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.DeleteBucketMetadata(bucketName)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err)
		}
	}
}

// AddRemoteTarget - adds event rules map, HTTP/PeerRPC client target to bucket name.
func (sys *NotificationSys) AddRemoteTarget(bucketName string, target event.Target, rulesMap event.RulesMap) error {
	if err := sys.targetList.Add(target); err != nil {
		return err
	}

	sys.Lock()
	targetMap := sys.bucketRemoteTargetRulesMap[bucketName]
	if targetMap == nil {
		targetMap = make(map[event.TargetID]event.RulesMap)
	}

	rulesMap = rulesMap.Clone()
	targetMap[target.ID()] = rulesMap
	sys.bucketRemoteTargetRulesMap[bucketName] = targetMap

	rulesMap = rulesMap.Clone()
	rulesMap.Add(sys.bucketRulesMap[bucketName])
	sys.bucketRulesMap[bucketName] = rulesMap

	sys.Unlock()

	return nil
}

// Loads notification policies for all buckets into NotificationSys.
func (sys *NotificationSys) load(buckets []BucketInfo, objAPI ObjectLayer) error {
	for _, bucket := range buckets {
		ctx := logger.SetReqInfo(GlobalContext, &logger.ReqInfo{BucketName: bucket.Name})
		config, err := globalBucketMetadataSys.GetNotificationConfig(bucket.Name)
		if err != nil {
			return err
		}
		config.SetRegion(globalServerRegion)
		if err = config.Validate(globalServerRegion, globalNotificationSys.targetList); err != nil {
			if _, ok := err.(*event.ErrARNNotFound); !ok {
				logger.LogIf(ctx, err)
			}
			continue
		}
		sys.AddRulesMap(bucket.Name, config.ToRulesMap())
	}
	return nil
}

// Init - initializes notification system from notification.xml and listenxl.meta of all buckets.
func (sys *NotificationSys) Init(buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, notifications are not supported.
	if globalIsGateway && !objAPI.IsNotificationSupported() {
		return nil
	}

	if globalConfigTargetList != nil {
		for _, target := range globalConfigTargetList.Targets() {
			if err := sys.targetList.Add(target); err != nil {
				return err
			}
		}
	}

	go func() {
		for res := range sys.targetResCh {
			if res.Err != nil {
				reqInfo := &logger.ReqInfo{}
				reqInfo.AppendTags("targetID", res.ID.Name)
				ctx := logger.SetReqInfo(GlobalContext, reqInfo)
				logger.LogOnceIf(ctx, res.Err, res.ID)
			}
		}
	}()

	return sys.load(buckets, objAPI)
}

// AddRulesMap - adds rules map for bucket name.
func (sys *NotificationSys) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	rulesMap = rulesMap.Clone()

	for _, targetRulesMap := range sys.bucketRemoteTargetRulesMap[bucketName] {
		rulesMap.Add(targetRulesMap)
	}

	// Do not add for an empty rulesMap.
	if len(rulesMap) == 0 {
		delete(sys.bucketRulesMap, bucketName)
	} else {
		sys.bucketRulesMap[bucketName] = rulesMap
	}
}

// RemoveRulesMap - removes rules map for bucket name.
func (sys *NotificationSys) RemoveRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	sys.bucketRulesMap[bucketName].Remove(rulesMap)
	if len(sys.bucketRulesMap[bucketName]) == 0 {
		delete(sys.bucketRulesMap, bucketName)
	}
}

// ConfiguredTargetIDs - returns list of configured target id's
func (sys *NotificationSys) ConfiguredTargetIDs() []event.TargetID {
	if sys == nil {
		return nil
	}

	sys.RLock()
	defer sys.RUnlock()

	var targetIDs []event.TargetID
	for _, rmap := range sys.bucketRulesMap {
		for _, rules := range rmap {
			for _, targetSet := range rules {
				for id := range targetSet {
					targetIDs = append(targetIDs, id)
				}
			}
		}
	}
	// Filter out targets configured via env
	var tIDs []event.TargetID
	for _, targetID := range targetIDs {
		if !globalEnvTargetList.Exists(targetID) {
			tIDs = append(tIDs, targetID)
		}
	}
	return tIDs
}

// RemoveNotification - removes all notification configuration for bucket name.
func (sys *NotificationSys) RemoveNotification(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketRulesMap, bucketName)

	targetIDSet := event.NewTargetIDSet()
	for targetID := range sys.bucketRemoteTargetRulesMap[bucketName] {
		targetIDSet[targetID] = struct{}{}
		delete(sys.bucketRemoteTargetRulesMap[bucketName], targetID)
	}
	sys.targetList.Remove(targetIDSet)

	delete(sys.bucketRemoteTargetRulesMap, bucketName)
}

// RemoveAllRemoteTargets - closes and removes all notification targets.
func (sys *NotificationSys) RemoveAllRemoteTargets() {
	sys.Lock()
	defer sys.Unlock()

	for _, targetMap := range sys.bucketRemoteTargetRulesMap {
		targetIDSet := event.NewTargetIDSet()
		for k := range targetMap {
			targetIDSet[k] = struct{}{}
		}
		sys.targetList.Remove(targetIDSet)
	}
}

// Send - sends event data to all matching targets.
func (sys *NotificationSys) Send(args eventArgs) {
	sys.RLock()
	targetIDSet := sys.bucketRulesMap[args.BucketName].Match(args.EventName, args.Object.Name)
	sys.RUnlock()

	if len(targetIDSet) == 0 {
		return
	}

	sys.targetList.Send(args.ToEvent(true), targetIDSet, sys.targetResCh)
}

// NetOBDInfo - Net OBD information
func (sys *NotificationSys) NetOBDInfo(ctx context.Context) madmin.ServerNetOBDInfo {
	var sortedGlobalEndpoints []string

	/*
			Ensure that only untraversed links are visited by this server
		        i.e. if netOBD tests have been performed between a -> b, then do
			not run it between b -> a

		        The graph of tests looks like this

		            a   b   c   d
		        a | o | x | x | x |
		        b | o | o | x | x |
		        c | o | o | o | x |
		        d | o | o | o | o |

		        'x's should be tested, and 'o's should be skipped
	*/

	hostSet := set.NewStringSet()
	for _, ez := range globalEndpoints {
		for _, e := range ez.Endpoints {
			if !hostSet.Contains(e.Host) {
				sortedGlobalEndpoints = append(sortedGlobalEndpoints, e.Host)
				hostSet.Add(e.Host)
			}
		}
	}

	sort.Strings(sortedGlobalEndpoints)
	var remoteTargets []*peerRESTClient
	search := func(host string) *peerRESTClient {
		for index, client := range sys.peerClients {
			if client == nil {
				continue
			}
			if sys.peerClients[index].host.String() == host {
				return client
			}
		}
		return nil
	}

	for i := 0; i < len(sortedGlobalEndpoints); i++ {
		if sortedGlobalEndpoints[i] != GetLocalPeer(globalEndpoints) {
			continue
		}
		for j := 0; j < len(sortedGlobalEndpoints); j++ {
			if j > i {
				remoteTarget := search(sortedGlobalEndpoints[j])
				if remoteTarget != nil {
					remoteTargets = append(remoteTargets, remoteTarget)
				}
			}
		}
	}

	netOBDs := make([]madmin.NetOBDInfo, len(remoteTargets))

	for index, client := range remoteTargets {
		if client == nil {
			continue
		}
		var err error
		netOBDs[index], err = client.NetOBDInfo(ctx)

		addr := client.host.String()
		reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		netOBDs[index].Addr = addr
		if err != nil {
			netOBDs[index].Error = err.Error()
		}
	}
	return madmin.ServerNetOBDInfo{
		Net:  netOBDs,
		Addr: GetLocalPeer(globalEndpoints),
	}
}

// DispatchNetOBDInfo - Net OBD information from other nodes
func (sys *NotificationSys) DispatchNetOBDInfo(ctx context.Context) []madmin.ServerNetOBDInfo {
	serverNetOBDs := []madmin.ServerNetOBDInfo{}

	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		serverNetOBD, err := sys.peerClients[index].DispatchNetOBDInfo(ctx)
		if err != nil {
			serverNetOBD.Addr = client.host.String()
			serverNetOBD.Error = err.Error()
		}
		serverNetOBDs = append(serverNetOBDs, serverNetOBD)
	}
	return serverNetOBDs
}

// DispatchNetOBDChan - Net OBD information from other nodes
func (sys *NotificationSys) DispatchNetOBDChan(ctx context.Context) chan madmin.ServerNetOBDInfo {
	serverNetOBDs := make(chan madmin.ServerNetOBDInfo)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for _, client := range sys.peerClients {
			if client == nil {
				continue
			}
			serverNetOBD, err := client.DispatchNetOBDInfo(ctx)
			if err != nil {
				serverNetOBD.Addr = client.host.String()
				serverNetOBD.Error = err.Error()
			}
			serverNetOBDs <- serverNetOBD
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(serverNetOBDs)
	}()

	return serverNetOBDs
}

// NetOBDParallelInfo - Performs NetOBD tests
func (sys *NotificationSys) NetOBDParallelInfo(ctx context.Context) madmin.ServerNetOBDInfo {
	netOBDs := []madmin.NetOBDInfo{}
	wg := sync.WaitGroup{}

	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}

		wg.Add(1)
		go func(index int) {
			netOBD, err := sys.peerClients[index].NetOBDInfo(ctx)
			netOBD.Addr = sys.peerClients[index].host.String()
			if err != nil {
				netOBD.Error = err.Error()
			}
			netOBDs = append(netOBDs, netOBD)
			wg.Done()
		}(index)
	}
	wg.Wait()
	return madmin.ServerNetOBDInfo{
		Net:  netOBDs,
		Addr: GetLocalPeer(globalEndpoints),
	}

}

// DriveOBDInfo - Drive OBD information
func (sys *NotificationSys) DriveOBDInfo(ctx context.Context) []madmin.ServerDrivesOBDInfo {
	reply := make([]madmin.ServerDrivesOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].DriveOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// DriveOBDInfoChan - Drive OBD information
func (sys *NotificationSys) DriveOBDInfoChan(ctx context.Context) chan madmin.ServerDrivesOBDInfo {
	updateChan := make(chan madmin.ServerDrivesOBDInfo)
	wg := sync.WaitGroup{}

	for _, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient) {
			reply, err := client.DriveOBDInfo(ctx)

			addr := client.host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)

			reply.Addr = addr
			if err != nil {
				reply.Error = err.Error()
			}

			updateChan <- reply
			wg.Done()
		}(client)
	}

	go func() {
		wg.Wait()
		close(updateChan)
	}()

	return updateChan
}

// CPUOBDInfo - CPU OBD information
func (sys *NotificationSys) CPUOBDInfo(ctx context.Context) []madmin.ServerCPUOBDInfo {
	reply := make([]madmin.ServerCPUOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].CPUOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// DiskHwOBDInfo - Disk HW OBD information
func (sys *NotificationSys) DiskHwOBDInfo(ctx context.Context) []madmin.ServerDiskHwOBDInfo {
	reply := make([]madmin.ServerDiskHwOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].DiskHwOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// OsOBDInfo - Os OBD information
func (sys *NotificationSys) OsOBDInfo(ctx context.Context) []madmin.ServerOsOBDInfo {
	reply := make([]madmin.ServerOsOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].OsOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// MemOBDInfo - Mem OBD information
func (sys *NotificationSys) MemOBDInfo(ctx context.Context) []madmin.ServerMemOBDInfo {
	reply := make([]madmin.ServerMemOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].MemOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// ProcOBDInfo - Process OBD information
func (sys *NotificationSys) ProcOBDInfo(ctx context.Context) []madmin.ServerProcOBDInfo {
	reply := make([]madmin.ServerProcOBDInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].ProcOBDInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			addr := sys.peerClients[index].host.String()
			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
			reply[index].Addr = addr
			reply[index].Error = err.Error()
		}
	}
	return reply
}

// ServerInfo - calls ServerInfo RPC call on all peers.
func (sys *NotificationSys) ServerInfo() []madmin.ServerProperties {
	reply := make([]madmin.ServerProperties, len(sys.peerClients))
	var wg sync.WaitGroup
	for i, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient, idx int) {
			defer wg.Done()
			info, err := client.ServerInfo()
			if err != nil {
				info.Endpoint = client.host.String()
				info.State = "offline"
			} else {
				info.State = "ok"
			}
			reply[idx] = info
		}(client, i)
	}
	wg.Wait()
	return reply
}

// GetLocalDiskIDs - return disk ids of the local disks of the peers.
func (sys *NotificationSys) GetLocalDiskIDs(ctx context.Context) []string {
	var diskIDs []string
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient) {
			defer wg.Done()
			ids := client.GetLocalDiskIDs(ctx)
			mu.Lock()
			diskIDs = append(diskIDs, ids...)
			mu.Unlock()
		}(client)
	}
	wg.Wait()
	return diskIDs
}

// NewNotificationSys - creates new notification system object.
func NewNotificationSys(endpoints EndpointZones) *NotificationSys {
	// bucketRulesMap/bucketRemoteTargetRulesMap are initialized by NotificationSys.Init()
	return &NotificationSys{
		targetList:                 event.NewTargetList(),
		targetResCh:                make(chan event.TargetIDResult),
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
		peerClients:                newPeerRestClients(endpoints),
	}
}

type eventArgs struct {
	EventName    event.Name
	BucketName   string
	Object       ObjectInfo
	ReqParams    map[string]string
	RespElements map[string]string
	Host         string
	UserAgent    string
}

// ToEvent - converts to notification event.
func (args eventArgs) ToEvent(escape bool) event.Event {
	eventTime := UTCNow()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())

	respElements := map[string]string{
		"x-amz-request-id":        args.RespElements["requestId"],
		"x-minio-origin-endpoint": globalMinioEndpoint, // MinIO specific custom elements.
	}
	// Add deployment as part of
	if globalDeploymentID != "" {
		respElements["x-minio-deployment-id"] = globalDeploymentID
	}
	if args.RespElements["content-length"] != "" {
		respElements["content-length"] = args.RespElements["content-length"]
	}
	keyName := args.Object.Name
	if escape {
		keyName = url.QueryEscape(args.Object.Name)
	}
	newEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         args.ReqParams["region"],
		EventTime:         eventTime.Format(event.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      event.Identity{PrincipalID: args.ReqParams["accessKey"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{PrincipalID: args.ReqParams["accessKey"]},
				ARN:           policy.ResourceARNPrefix + args.BucketName,
			},
			Object: event.Object{
				Key:       keyName,
				VersionID: args.Object.VersionID,
				Sequencer: uniqueID,
			},
		},
		Source: event.Source{
			Host:      args.Host,
			UserAgent: args.UserAgent,
		},
	}

	if args.EventName != event.ObjectRemovedDelete {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = args.Object.UserDefined
	}

	return newEvent
}

func sendEvent(args eventArgs) {
	args.Object.Size, _ = args.Object.GetActualSize()

	// remove sensitive encryption entries in metadata.
	crypto.RemoveSensitiveEntries(args.Object.UserDefined)
	crypto.RemoveInternalEntries(args.Object.UserDefined)

	// globalNotificationSys is not initialized in gateway mode.
	if globalNotificationSys == nil {
		return
	}

	if globalHTTPListen.HasSubscribers() {
		globalHTTPListen.Publish(args.ToEvent(false))
	}

	globalNotificationSys.Send(args)
}
