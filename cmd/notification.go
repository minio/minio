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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	bucketBandwidth "github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/bucket/policy"
	xnet "github.com/minio/pkg/net"
)

// NotificationSys - notification system.
type NotificationSys struct {
	sync.RWMutex
	targetList                 *event.TargetList
	targetResCh                chan event.TargetIDResult
	bucketRulesMap             map[string]event.RulesMap
	bucketRemoteTargetRulesMap map[string]map[event.TargetID]event.RulesMap
	peerClients                []*peerRESTClient // Excludes self
	allPeerClients             []*peerRESTClient // Includes nil client for self
}

// GetARNList - returns available ARNs.
func (sys *NotificationSys) GetARNList(onlyActive bool) []string {
	arns := []string{}
	if sys == nil {
		return arns
	}
	region := globalServerRegion
	for targetID, target := range sys.targetList.TargetMap() {
		// httpclient target is part of ListenNotification
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
func (sys *NotificationSys) BackgroundHealStatus() ([]madmin.BgHealState, []NotificationPeerErr) {
	ng := WithNPeers(len(sys.peerClients))
	states := make([]madmin.BgHealState, len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		idx := idx
		client := client
		ng.Go(GlobalContext, func() error {
			st, err := client.BackgroundHealStatus()
			if err != nil {
				return err
			}
			states[idx] = st
			return nil
		}, idx, *client.host)
	}

	return states, ng.Wait()
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
			header.Method = zip.Deflate
			zwriter, zerr := zipWriter.CreateHeader(header)
			if zerr != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				logger.LogIf(ctx, zerr)
				continue
			}
			if _, err = io.Copy(zwriter, bytes.NewReader(data)); err != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				logger.LogIf(ctx, err)
				continue
			}
		}
	}

	// Local host
	thisAddr, err := xnet.ParseHost(globalLocalNodeName)
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
		header.Method = zip.Deflate

		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			return profilingDataFound
		}

		if _, err = io.Copy(zwriter, bytes.NewReader(data)); err != nil {
			return profilingDataFound
		}
	}

	return profilingDataFound
}

// ServerUpdate - updates remote peers.
func (sys *NotificationSys) ServerUpdate(ctx context.Context, u *url.URL, sha256Sum []byte, lrTime time.Time, releaseInfo string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.ServerUpdate(ctx, u, sha256Sum, lrTime, releaseInfo)
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
	bfr, err := intDataUpdateTracker.cycleFilter(ctx, req)
	logger.LogIf(ctx, err)
	if err == nil && bfr.Complete {
		nbf := intDataUpdateTracker.newBloomFilter()
		bf = &nbf
		_, err = bf.ReadFrom(bytes.NewReader(bfr.Filter))
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
				logger.LogOnceIf(ctx, err, fmt.Sprintf("host:%s, cycle:%d", client.host, current), client.cycleServerBloomFilter)
				bf = nil
				return nil
			}

			var tmp bloom.BloomFilter
			_, err = tmp.ReadFrom(bytes.NewReader(serverBF.Filter))
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

// findEarliestCleanBloomFilter will find the earliest bloom filter across the cluster
// where the directory is clean.
// Due to how objects are stored this can include object names.
func (sys *NotificationSys) findEarliestCleanBloomFilter(ctx context.Context, dir string) uint64 {

	// Load initial state from local...
	current := intDataUpdateTracker.current()
	best := intDataUpdateTracker.latestWithDir(dir)
	if best == current {
		// If the current is dirty no need to check others.
		return current
	}

	var req = bloomFilterRequest{
		Current:     0,
		Oldest:      best,
		OldestClean: dir,
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

			// Keep lock while checking result.
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				// Error, don't assume clean.
				best = current
				logger.LogIf(ctx, err)
				return nil
			}
			if serverBF.OldestIdx > best {
				best = serverBF.OldestIdx
			}
			return nil
		}, idx)
	}
	g.Wait()
	return best
}

var errPeerNotReachable = errors.New("peer is not reachable")

// GetLocks - makes GetLocks RPC call on all peers.
func (sys *NotificationSys) GetLocks(ctx context.Context, r *http.Request) []*PeerLocks {
	locksResp := make([]*PeerLocks, len(sys.peerClients))
	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		index := index
		g.Go(func() error {
			if client == nil {
				return errPeerNotReachable
			}
			serverLocksResp, err := sys.peerClients[index].GetLocks()
			if err != nil {
				return err
			}
			locksResp[index] = &PeerLocks{
				Addr:  sys.peerClients[index].host.String(),
				Locks: serverLocksResp,
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
	locksResp = append(locksResp, &PeerLocks{
		Addr:  getHostName(r),
		Locks: globalLockServer.DupLockMap(),
	})
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
	globalReplicationStats.Delete(bucketName)
	globalBucketMetadataSys.Remove(bucketName)
	globalBucketTargetSys.Delete(bucketName)
	if localMetacacheMgr != nil {
		localMetacacheMgr.deleteBucketCache(bucketName)
	}

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

// GetClusterBucketStats - calls GetClusterBucketStats call on all peers for a cluster statistics view.
func (sys *NotificationSys) GetClusterBucketStats(ctx context.Context, bucketName string) []BucketStats {
	ng := WithNPeers(len(sys.peerClients))
	bucketStats := make([]BucketStats, len(sys.peerClients))
	for index, client := range sys.peerClients {
		index := index
		client := client
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			bs, err := client.GetBucketStats(bucketName)
			if err != nil {
				return err
			}
			bucketStats[index] = bs
			return nil
		}, index, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err)
		}
	}
	bucketStats = append(bucketStats, BucketStats{
		ReplicationStats: globalReplicationStats.Get(bucketName),
	})
	return bucketStats
}

// LoadTransitionTierConfig notifies remote peers to load their remote tier
// configs from config store.
func (sys *NotificationSys) LoadTransitionTierConfig(ctx context.Context) {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.LoadTransitionTierConfig(ctx)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err)
		}
	}
}

// Loads notification policies for all buckets into NotificationSys.
func (sys *NotificationSys) load(buckets []BucketInfo) {
	for _, bucket := range buckets {
		ctx := logger.SetReqInfo(GlobalContext, &logger.ReqInfo{BucketName: bucket.Name})
		config, err := globalBucketMetadataSys.GetNotificationConfig(bucket.Name)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
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
}

// Init - initializes notification system from notification.xml and listenxl.meta of all buckets.
func (sys *NotificationSys) Init(ctx context.Context, buckets []BucketInfo, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	// In gateway mode, notifications are not supported - except NAS gateway.
	if globalIsGateway && !objAPI.IsNotificationSupported() {
		return nil
	}

	logger.LogIf(ctx, sys.targetList.Add(globalConfigTargetList.Targets()...))

	go func() {
		for res := range sys.targetResCh {
			if res.Err != nil {
				reqInfo := &logger.ReqInfo{}
				reqInfo.AppendTags("targetID", res.ID.Name)
				logger.LogOnceIf(logger.SetReqInfo(GlobalContext, reqInfo), res.Err, res.ID)
			}
		}
	}()

	go sys.load(buckets)
	return nil
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

// GetNetPerfInfo - Net information
func (sys *NotificationSys) GetNetPerfInfo(ctx context.Context) madmin.NetPerfInfo {
	var sortedGlobalEndpoints []string

	/*
			Ensure that only untraversed links are visited by this server
		        i.e. if net perf tests have been performed between a -> b, then do
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
		if sortedGlobalEndpoints[i] != globalLocalNodeName {
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

	netInfos := make([]madmin.PeerNetPerfInfo, len(remoteTargets))

	for index, client := range remoteTargets {
		if client == nil {
			continue
		}
		var err error
		netInfos[index], err = client.GetNetPerfInfo(ctx)

		addr := client.host.String()
		reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.LogIf(ctx, err)
		netInfos[index].Addr = addr
		if err != nil {
			netInfos[index].Error = err.Error()
		}
	}
	return madmin.NetPerfInfo{
		Addr:        globalLocalNodeName,
		RemotePeers: netInfos,
	}
}

// DispatchNetPerfInfo - Net perf information from other nodes
func (sys *NotificationSys) DispatchNetPerfInfo(ctx context.Context) []madmin.NetPerfInfo {
	serverNetInfos := []madmin.NetPerfInfo{}

	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		serverNetInfo, err := sys.peerClients[index].DispatchNetInfo(ctx)
		if err != nil {
			serverNetInfo.Addr = client.host.String()
			serverNetInfo.Error = err.Error()
		}
		serverNetInfos = append(serverNetInfos, serverNetInfo)
	}
	return serverNetInfos
}

// DispatchNetPerfChan - Net perf information from other nodes
func (sys *NotificationSys) DispatchNetPerfChan(ctx context.Context) chan madmin.NetPerfInfo {
	serverNetInfos := make(chan madmin.NetPerfInfo)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for _, client := range sys.peerClients {
			if client == nil {
				continue
			}
			serverNetInfo, err := client.DispatchNetInfo(ctx)
			if err != nil {
				serverNetInfo.Addr = client.host.String()
				serverNetInfo.Error = err.Error()
			}
			serverNetInfos <- serverNetInfo
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(serverNetInfos)
	}()

	return serverNetInfos
}

// GetParallelNetPerfInfo - Performs Net parallel tests
func (sys *NotificationSys) GetParallelNetPerfInfo(ctx context.Context) madmin.NetPerfInfo {
	netInfos := []madmin.PeerNetPerfInfo{}
	wg := sync.WaitGroup{}

	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}

		wg.Add(1)
		go func(index int) {
			netInfo, err := sys.peerClients[index].GetNetPerfInfo(ctx)
			netInfo.Addr = sys.peerClients[index].host.String()
			if err != nil {
				netInfo.Error = err.Error()
			}
			netInfos = append(netInfos, netInfo)
			wg.Done()
		}(index)
	}
	wg.Wait()
	return madmin.NetPerfInfo{
		Addr:        globalLocalNodeName,
		RemotePeers: netInfos,
	}
}

// GetDrivePerfInfos - Drive performance information
func (sys *NotificationSys) GetDrivePerfInfos(ctx context.Context) chan madmin.DrivePerfInfos {
	updateChan := make(chan madmin.DrivePerfInfos)
	wg := sync.WaitGroup{}

	for _, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient) {
			reply, err := client.GetDrivePerfInfos(ctx)

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

// GetCPUs - Get all CPU information.
func (sys *NotificationSys) GetCPUs(ctx context.Context) []madmin.CPUs {
	reply := make([]madmin.CPUs, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetCPUs(ctx)
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

// GetPartitions - Disk partition information
func (sys *NotificationSys) GetPartitions(ctx context.Context) []madmin.Partitions {
	reply := make([]madmin.Partitions, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetPartitions(ctx)
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

// GetOSInfo - Get operating system's information
func (sys *NotificationSys) GetOSInfo(ctx context.Context) []madmin.OSInfo {
	reply := make([]madmin.OSInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetOSInfo(ctx)
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

// GetMemInfo - Memory information
func (sys *NotificationSys) GetMemInfo(ctx context.Context) []madmin.MemInfo {
	reply := make([]madmin.MemInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetMemInfo(ctx)
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

// GetProcInfo - Process information
func (sys *NotificationSys) GetProcInfo(ctx context.Context) []madmin.ProcInfo {
	reply := make([]madmin.ProcInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetProcInfo(ctx)
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

func getOfflineDisks(offlineHost string, endpoints EndpointServerPools) []madmin.Disk {
	var offlineDisks []madmin.Disk
	for _, pool := range endpoints {
		for _, ep := range pool.Endpoints {
			if offlineHost == ep.Host {
				offlineDisks = append(offlineDisks, madmin.Disk{
					Endpoint: ep.String(),
					State:    string(madmin.ItemOffline),
				})
			}
		}
	}
	return offlineDisks
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
				info.State = string(madmin.ItemOffline)
				info.Disks = getOfflineDisks(info.Endpoint, globalEndpoints)
			} else {
				info.State = string(madmin.ItemOnline)
			}
			reply[idx] = info
		}(client, i)
	}
	wg.Wait()

	return reply
}

// GetLocalDiskIDs - return disk ids of the local disks of the peers.
func (sys *NotificationSys) GetLocalDiskIDs(ctx context.Context) (localDiskIDs [][]string) {
	localDiskIDs = make([][]string, len(sys.peerClients))
	var wg sync.WaitGroup
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, client *peerRESTClient) {
			defer wg.Done()
			localDiskIDs[idx] = client.GetLocalDiskIDs(ctx)
		}(idx, client)
	}
	wg.Wait()
	return localDiskIDs
}

// returns all the peers that are currently online.
func (sys *NotificationSys) getOnlinePeers() []*peerRESTClient {
	var peerClients []*peerRESTClient
	for _, peerClient := range sys.allPeerClients {
		if peerClient != nil && peerClient.IsOnline() {
			peerClients = append(peerClients, peerClient)
		}
	}
	return peerClients
}

// restClientFromHash will return a deterministic peerRESTClient based on s.
// Will return nil if client is local.
func (sys *NotificationSys) restClientFromHash(s string) (client *peerRESTClient) {
	if len(sys.peerClients) == 0 {
		return nil
	}
	peerClients := sys.getOnlinePeers()
	if len(peerClients) == 0 {
		return nil
	}
	idx := xxhash.Sum64String(s) % uint64(len(peerClients))
	return peerClients[idx]
}

// NewNotificationSys - creates new notification system object.
func NewNotificationSys(endpoints EndpointServerPools) *NotificationSys {
	// targetList/bucketRulesMap/bucketRemoteTargetRulesMap are populated by NotificationSys.Init()
	remote, all := newPeerRestClients(endpoints)
	return &NotificationSys{
		targetList:                 event.NewTargetList(),
		targetResCh:                make(chan event.TargetIDResult),
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
		peerClients:                remote,
		allPeerClients:             all,
	}
}

// GetPeerOnlineCount gets the count of online and offline nodes.
func GetPeerOnlineCount() (nodesOnline, nodesOffline int) {
	nodesOnline = 1 // Self is always online.
	nodesOffline = 0
	servers := globalNotificationSys.ServerInfo()
	for _, s := range servers {
		if s.State == string(madmin.ItemOnline) {
			nodesOnline++
			continue
		}
		nodesOffline++
	}
	return
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
		UserIdentity:      event.Identity{PrincipalID: args.ReqParams["principalId"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{PrincipalID: args.ReqParams["principalId"]},
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

	if args.EventName != event.ObjectRemovedDelete && args.EventName != event.ObjectRemovedDeleteMarkerCreated {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = args.Object.UserDefined
	}

	return newEvent
}

func sendEvent(args eventArgs) {
	args.Object.Size, _ = args.Object.GetActualSize()

	// avoid generating a notification for REPLICA creation event.
	if _, ok := args.ReqParams[xhttp.MinIOSourceReplicationRequest]; ok {
		return
	}
	// remove sensitive encryption entries in metadata.
	crypto.RemoveSensitiveEntries(args.Object.UserDefined)
	crypto.RemoveInternalEntries(args.Object.UserDefined)

	// globalNotificationSys is not initialized in gateway mode.
	if globalNotificationSys == nil {
		return
	}

	if globalHTTPListen.NumSubscribers() > 0 {
		globalHTTPListen.Publish(args.ToEvent(false))
	}

	globalNotificationSys.Send(args)
}

// GetBandwidthReports - gets the bandwidth report from all nodes including self.
func (sys *NotificationSys) GetBandwidthReports(ctx context.Context, buckets ...string) madmin.BucketBandwidthReport {
	reports := make([]*madmin.BucketBandwidthReport, len(sys.peerClients))
	g := errgroup.WithNErrs(len(sys.peerClients))
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reports[index], err = sys.peerClients[index].MonitorBandwidth(ctx, buckets)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress",
			sys.peerClients[index].host.String())
		ctx := logger.SetReqInfo(ctx, reqInfo)
		logger.LogOnceIf(ctx, err, sys.peerClients[index].host.String())
	}
	reports = append(reports, globalBucketMonitor.GetReport(bucketBandwidth.SelectBuckets(buckets...)))
	consolidatedReport := madmin.BucketBandwidthReport{
		BucketStats: make(map[string]madmin.BandwidthDetails),
	}
	for _, report := range reports {
		if report == nil || report.BucketStats == nil {
			continue
		}
		for bucket := range report.BucketStats {
			d, ok := consolidatedReport.BucketStats[bucket]
			if !ok {
				consolidatedReport.BucketStats[bucket] = madmin.BandwidthDetails{}
				d = consolidatedReport.BucketStats[bucket]
				d.LimitInBytesPerSecond = report.BucketStats[bucket].LimitInBytesPerSecond
			}
			if d.LimitInBytesPerSecond < report.BucketStats[bucket].LimitInBytesPerSecond {
				d.LimitInBytesPerSecond = report.BucketStats[bucket].LimitInBytesPerSecond
			}
			d.CurrentBandwidthInBytesPerSecond += report.BucketStats[bucket].CurrentBandwidthInBytesPerSecond
			consolidatedReport.BucketStats[bucket] = d
		}
	}
	return consolidatedReport
}

// GetClusterMetrics - gets the cluster metrics from all nodes excluding self.
func (sys *NotificationSys) GetClusterMetrics(ctx context.Context) chan Metric {
	g := errgroup.WithNErrs(len(sys.peerClients))
	peerChannels := make([]<-chan Metric, len(sys.peerClients))
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			peerChannels[index], err = sys.peerClients[index].GetPeerMetrics(ctx)
			return err
		}, index)
	}

	ch := make(chan Metric)
	var wg sync.WaitGroup
	for index, err := range g.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress",
			sys.peerClients[index].host.String())
		ctx := logger.SetReqInfo(ctx, reqInfo)
		if err != nil {
			logger.LogOnceIf(ctx, err, sys.peerClients[index].host.String())
			continue
		}
		wg.Add(1)
		go func(ctx context.Context, peerChannel <-chan Metric, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case m, ok := <-peerChannel:
					if !ok {
						return
					}
					ch <- m
				case <-ctx.Done():
					return
				}
			}
		}(ctx, peerChannels[index], &wg)
	}
	go func(wg *sync.WaitGroup, ch chan Metric) {
		wg.Wait()
		close(ch)
	}(&wg, ch)
	return ch
}
