// Copyright (c) 2015-2023 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go/v3"
	xioutil "github.com/minio/minio/internal/ioutil"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/pkg/v3/workers"

	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/logger"
)

// This file contains peer related notifications. For sending notifications to
// external systems, see event-notification.go

// NotificationSys - notification system.
type NotificationSys struct {
	peerClients    []*peerRESTClient // Excludes self
	allPeerClients []*peerRESTClient // Includes nil client for self
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
	workers    *workers.Workers
	errs       []NotificationPeerErr
	retryCount int
}

// WithNPeers returns a new NotificationGroup with length of errs slice upto nerrs,
// upon Wait() errors are returned collected from all tasks.
func WithNPeers(nerrs int) *NotificationGroup {
	if nerrs <= 0 {
		nerrs = 1
	}
	wk, _ := workers.New(nerrs)
	return &NotificationGroup{errs: make([]NotificationPeerErr, nerrs), workers: wk, retryCount: 3}
}

// WithNPeersThrottled returns a new NotificationGroup with length of errs slice upto nerrs,
// upon Wait() errors are returned collected from all tasks, optionally allows for X workers
// only "per" parallel task.
func WithNPeersThrottled(nerrs, wks int) *NotificationGroup {
	if nerrs <= 0 {
		nerrs = 1
	}
	if wks > nerrs {
		wks = nerrs
	}
	wk, _ := workers.New(wks)
	return &NotificationGroup{errs: make([]NotificationPeerErr, nerrs), workers: wk, retryCount: 3}
}

// WithRetries sets the retry count for all function calls from the Go method.
func (g *NotificationGroup) WithRetries(retryCount int) *NotificationGroup {
	if g != nil {
		g.retryCount = retryCount
	}
	return g
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the slice of errors from all function calls.
func (g *NotificationGroup) Wait() []NotificationPeerErr {
	g.workers.Wait()
	return g.errs
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error will be
// collected in errs slice and returned by Wait().
func (g *NotificationGroup) Go(ctx context.Context, f func() error, index int, addr xnet.Host) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	g.workers.Take()

	go func() {
		defer g.workers.Give()

		g.errs[index] = NotificationPeerErr{
			Host: addr,
		}

		retryCount := g.retryCount
		for i := range retryCount {
			g.errs[index].Err = nil
			if err := f(); err != nil {
				g.errs[index].Err = err

				if contextCanceled(ctx) {
					// context already canceled no retries.
					retryCount = 0
				}

				// Last iteration log the error.
				if i == retryCount-1 {
					reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", addr.String())
					ctx := logger.SetReqInfo(ctx, reqInfo)
					peersLogOnceIf(ctx, err, addr.String())
				}

				// Wait for a minimum of 100ms and dynamically increase this based on number of attempts.
				if i < retryCount-1 {
					time.Sleep(100*time.Millisecond + time.Duration(r.Float64()*float64(time.Second)))
					continue
				}
			}
			break
		}
	}()
}

// DeletePolicy - deletes policy across all peers.
func (sys *NotificationSys) DeletePolicy(ctx context.Context, policyName string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.DeletePolicy(ctx, policyName)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadPolicy - reloads a specific modified policy across all peers
func (sys *NotificationSys) LoadPolicy(ctx context.Context, policyName string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.LoadPolicy(ctx, policyName)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadPolicyMapping - reloads a policy mapping across all peers
func (sys *NotificationSys) LoadPolicyMapping(ctx context.Context, userOrGroup string, userType IAMUserType, isGroup bool) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.LoadPolicyMapping(ctx, userOrGroup, userType, isGroup)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DeleteUser - deletes a specific user across all peers
func (sys *NotificationSys) DeleteUser(ctx context.Context, accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.DeleteUser(ctx, accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadUser - reloads a specific user across all peers
func (sys *NotificationSys) LoadUser(ctx context.Context, accessKey string, temp bool) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.LoadUser(ctx, accessKey, temp)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadGroup - loads a specific group on all peers.
func (sys *NotificationSys) LoadGroup(ctx context.Context, group string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.LoadGroup(ctx, group)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DeleteServiceAccount - deletes a specific service account across all peers
func (sys *NotificationSys) DeleteServiceAccount(ctx context.Context, accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.DeleteServiceAccount(ctx, accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// LoadServiceAccount - reloads a specific service account across all peers
func (sys *NotificationSys) LoadServiceAccount(ctx context.Context, accessKey string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	for idx, client := range sys.peerClients {
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			return client.LoadServiceAccount(ctx, accessKey)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// BackgroundHealStatus - returns background heal status of all peers
func (sys *NotificationSys) BackgroundHealStatus(ctx context.Context) ([]madmin.BgHealState, []NotificationPeerErr) {
	ng := WithNPeers(len(sys.peerClients))
	states := make([]madmin.BgHealState, len(sys.peerClients))
	for idx, client := range sys.peerClients {
		client := client
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			st, err := client.BackgroundHealStatus(ctx)
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
func (sys *NotificationSys) StartProfiling(ctx context.Context, profiler string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.StartProfiling(ctx, profiler)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// DownloadProfilingData - download profiling data from all remote peers.
func (sys *NotificationSys) DownloadProfilingData(ctx context.Context, writer io.Writer) (profilingDataFound bool) {
	// Initialize a zip writer which will provide a zipped content
	// of profiling data of all nodes
	zipWriter := zip.NewWriter(writer)
	defer zipWriter.Close()

	// Start by embedding cluster info.
	if b := getClusterMetaInfo(ctx); len(b) > 0 {
		internalLogIf(ctx, embedFileInZip(zipWriter, "cluster.info", b, 0o600))
	}

	// Profiles can be quite big, so we limit to max 16 concurrent downloads.
	ng := WithNPeersThrottled(len(sys.peerClients), 16)
	var writeMu sync.Mutex
	for i, client := range sys.peerClients {
		if client == nil {
			continue
		}
		ng.Go(ctx, func() error {
			// Give 15 seconds to each remote call.
			// Errors are logged but not returned.
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			data, err := client.DownloadProfileData(ctx)
			if err != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
				ctx := logger.SetReqInfo(ctx, reqInfo)
				peersLogOnceIf(ctx, err, client.host.String())
				return nil
			}

			for typ, data := range data {
				// zip writer only handles one concurrent write
				writeMu.Lock()
				profilingDataFound = true
				err := embedFileInZip(zipWriter, fmt.Sprintf("profile-%s-%s", client.host.String(), typ), data, 0o600)
				writeMu.Unlock()
				if err != nil {
					reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", client.host.String())
					ctx := logger.SetReqInfo(ctx, reqInfo)
					peersLogOnceIf(ctx, err, client.host.String())
				}
			}
			return nil
		}, i, *client.host)
	}
	ng.Wait()
	if ctx.Err() != nil {
		return false
	}

	// Local host
	thisAddr, err := xnet.ParseHost(globalLocalNodeName)
	if err != nil {
		bugLogIf(ctx, err)
		return profilingDataFound
	}

	data, err := getProfileData()
	if err != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", thisAddr.String())
		ctx := logger.SetReqInfo(ctx, reqInfo)
		bugLogIf(ctx, err)
		return profilingDataFound
	}

	profilingDataFound = true

	// Send profiling data to zip as file
	for typ, data := range data {
		err := embedFileInZip(zipWriter, fmt.Sprintf("profile-%s-%s", thisAddr, typ), data, 0o600)
		internalLogIf(ctx, err)
	}

	return profilingDataFound
}

// VerifyBinary - asks remote peers to verify the checksum
func (sys *NotificationSys) VerifyBinary(ctx context.Context, u *url.URL, sha256Sum []byte, releaseInfo string, bin []byte) []NotificationPeerErr {
	// FIXME: network calls made in this manner such as one goroutine per node,
	// can easily eat into the internode bandwidth. This function would be mostly
	// TX saturating, however there are situations where a RX might also saturate.
	// To avoid these problems we must split the work at scale. With 1000 node
	// setup becoming a reality we must try to shard the work properly such as
	// pick 10 nodes that precisely can send those 100 requests the first node
	// in the 10 node shard would coordinate between other 9 shards to get the
	// rest of the `99*9` requests.
	//
	// This essentially splits the workload properly and also allows for network
	// utilization to be optimal, instead of blindly throttling the way we are
	// doing below. However the changes that are needed here are a bit involved,
	// further discussion advised. Remove this comment and remove the worker model
	// for this function in future.
	maxWorkers := runtime.GOMAXPROCS(0) / 2
	ng := WithNPeersThrottled(len(sys.peerClients), maxWorkers)
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.VerifyBinary(ctx, u, sha256Sum, releaseInfo, bytes.NewReader(bin))
		}, idx, *client.host)
	}
	return ng.Wait()
}

// CommitBinary - asks remote peers to overwrite the old binary with the new one
func (sys *NotificationSys) CommitBinary(ctx context.Context) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.CommitBinary(ctx)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// SignalConfigReload reloads requested sub-system on a remote peer dynamically.
func (sys *NotificationSys) SignalConfigReload(subSys string) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.SignalService(serviceReloadDynamic, subSys, false, nil)
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
			// force == true preserves the current behavior
			return client.SignalService(sig, "", false, nil)
		}, idx, *client.host)
	}
	return ng.Wait()
}

// SignalServiceV2 - calls signal service RPC call on all peers with v2 API
func (sys *NotificationSys) SignalServiceV2(sig serviceSignal, dryRun bool, execAt *time.Time) []NotificationPeerErr {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.SignalService(sig, "", dryRun, execAt)
		}, idx, *client.host)
	}
	return ng.Wait()
}

var errPeerNotReachable = errors.New("peer is not reachable")

// GetLocks - makes GetLocks RPC call on all peers.
func (sys *NotificationSys) GetLocks(ctx context.Context, r *http.Request) []*PeerLocks {
	locksResp := make([]*PeerLocks, len(sys.peerClients))
	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		client := client
		g.Go(func() error {
			if client == nil {
				return errPeerNotReachable
			}
			serverLocksResp, err := sys.peerClients[index].GetLocks(ctx)
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
		peersLogOnceIf(ctx, err, sys.peerClients[index].host.String())
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
			return client.LoadBucketMetadata(ctx, bucketName)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
}

// DeleteBucketMetadata - calls DeleteBucketMetadata call on all peers
func (sys *NotificationSys) DeleteBucketMetadata(ctx context.Context, bucketName string) {
	globalReplicationStats.Load().Delete(bucketName)
	globalBucketMetadataSys.Remove(bucketName)
	globalBucketTargetSys.Delete(bucketName)
	globalEventNotifier.RemoveNotification(bucketName)
	globalBucketConnStats.delete(bucketName)
	globalBucketHTTPStats.delete(bucketName)
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
			return client.DeleteBucketMetadata(ctx, bucketName)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
}

// GetClusterAllBucketStats - returns bucket stats for all buckets from all remote peers.
func (sys *NotificationSys) GetClusterAllBucketStats(ctx context.Context) []BucketStatsMap {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	replicationStats := make([]BucketStatsMap, len(sys.peerClients))
	for index, client := range sys.peerClients {
		client := client
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			bsMap, err := client.GetAllBucketStats(ctx)
			if err != nil {
				return err
			}
			replicationStats[index] = bsMap
			return nil
		}, index, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}

	replicationStatsList := globalReplicationStats.Load().GetAll()
	bucketStatsMap := BucketStatsMap{
		Stats:     make(map[string]BucketStats, len(replicationStatsList)),
		Timestamp: UTCNow(),
	}
	for k, replicationStats := range replicationStatsList {
		bucketStatsMap.Stats[k] = BucketStats{
			ReplicationStats: replicationStats,
			ProxyStats:       globalReplicationStats.Load().getProxyStats(k),
		}
	}

	replicationStats = append(replicationStats, bucketStatsMap)
	return replicationStats
}

// GetClusterBucketStats - calls GetClusterBucketStats call on all peers for a cluster statistics view.
func (sys *NotificationSys) GetClusterBucketStats(ctx context.Context, bucketName string) []BucketStats {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	bucketStats := make([]BucketStats, len(sys.peerClients))
	for index, client := range sys.peerClients {
		client := client
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			bs, err := client.GetBucketStats(ctx, bucketName)
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
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
	if st := globalReplicationStats.Load(); st != nil {
		bucketStats = append(bucketStats, BucketStats{
			ReplicationStats: st.Get(bucketName),
			QueueStats:       ReplicationQueueStats{Nodes: []ReplQNodeStats{st.getNodeQueueStats(bucketName)}},
			ProxyStats:       st.getProxyStats(bucketName),
		})
	}
	return bucketStats
}

// GetClusterSiteMetrics - calls GetClusterSiteMetrics call on all peers for a cluster statistics view.
func (sys *NotificationSys) GetClusterSiteMetrics(ctx context.Context) []SRMetricsSummary {
	ng := WithNPeers(len(sys.peerClients)).WithRetries(1)
	siteStats := make([]SRMetricsSummary, len(sys.peerClients))
	for index, client := range sys.peerClients {
		client := client
		ng.Go(ctx, func() error {
			if client == nil {
				return errPeerNotReachable
			}
			sm, err := client.GetSRMetrics(ctx)
			if err != nil {
				return err
			}
			siteStats[index] = sm
			return nil
		}, index, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
	siteStats = append(siteStats, globalReplicationStats.Load().getSRMetricsForNode())
	return siteStats
}

// ReloadPoolMeta reloads on disk updates on pool metadata
func (sys *NotificationSys) ReloadPoolMeta(ctx context.Context) {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.ReloadPoolMeta(ctx)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
}

// DeleteUploadID notifies all the MinIO nodes to remove the
// given uploadID from cache
func (sys *NotificationSys) DeleteUploadID(ctx context.Context, uploadID string) {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.DeleteUploadID(ctx, uploadID)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
}

// StopRebalance notifies all MinIO nodes to signal any ongoing rebalance
// goroutine to stop.
func (sys *NotificationSys) StopRebalance(ctx context.Context) {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		internalLogIf(ctx, errServerNotInitialized)
		return
	}

	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.StopRebalance(ctx)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}

	if pools, ok := objAPI.(*erasureServerPools); ok {
		pools.StopRebalance()
	}
}

// LoadRebalanceMeta notifies all peers to load rebalance.bin from object layer.
// Note: Only peers participating in rebalance operation, namely the first node
// in each pool will load rebalance.bin.
func (sys *NotificationSys) LoadRebalanceMeta(ctx context.Context, startRebalance bool) {
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(ctx, func() error {
			return client.LoadRebalanceMeta(ctx, startRebalance)
		}, idx, *client.host)
	}
	for _, nErr := range ng.Wait() {
		reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress", nErr.Host.String())
		if nErr.Err != nil {
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
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
			peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), nErr.Err, nErr.Host.String())
		}
	}
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
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
		}
	}
	return reply
}

// GetNetInfo - Network information
func (sys *NotificationSys) GetNetInfo(ctx context.Context) []madmin.NetInfo {
	reply := make([]madmin.NetInfo, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetNetInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
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
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
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
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
		}
	}
	return reply
}

// GetMetrics - Get metrics from all peers.
func (sys *NotificationSys) GetMetrics(ctx context.Context, t madmin.MetricType, opts collectMetricsOpts) []madmin.RealtimeMetrics {
	reply := make([]madmin.RealtimeMetrics, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		host := client.host.String()
		if len(opts.hosts) > 0 {
			if _, ok := opts.hosts[host]; !ok {
				continue
			}
		}

		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetMetrics(ctx, t, opts)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			reply[index].Errors = []string{fmt.Sprintf("%s: %s (rpc)", sys.peerClients[index].String(), err.Error())}
		}
	}
	return reply
}

// GetResourceMetrics - gets the resource metrics from all nodes excluding self.
func (sys *NotificationSys) GetResourceMetrics(ctx context.Context) <-chan MetricV2 {
	if sys == nil {
		return nil
	}
	g := errgroup.WithNErrs(len(sys.peerClients))
	peerChannels := make([]<-chan MetricV2, len(sys.peerClients))
	for index := range sys.peerClients {
		g.Go(func() error {
			if sys.peerClients[index] == nil {
				return errPeerNotReachable
			}
			var err error
			peerChannels[index], err = sys.peerClients[index].GetResourceMetrics(ctx)
			return err
		}, index)
	}
	return sys.collectPeerMetrics(ctx, peerChannels, g)
}

// GetSysConfig - Get information about system config
// (only the config that are of concern to minio)
func (sys *NotificationSys) GetSysConfig(ctx context.Context) []madmin.SysConfig {
	reply := make([]madmin.SysConfig, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetSysConfig(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
		}
	}
	return reply
}

// GetSysServices - Get information about system services
// (only the services that are of concern to minio)
func (sys *NotificationSys) GetSysServices(ctx context.Context) []madmin.SysServices {
	reply := make([]madmin.SysServices, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetSELinuxInfo(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
		}
	}
	return reply
}

func (sys *NotificationSys) addNodeErr(nodeInfo madmin.NodeInfo, peerClient *peerRESTClient, err error) {
	addr := peerClient.host.String()
	reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", addr)
	ctx := logger.SetReqInfo(GlobalContext, reqInfo)
	peersLogOnceIf(ctx, err, "add-node-err-"+addr)
	nodeInfo.SetAddr(addr)
	nodeInfo.SetError(err.Error())
}

// GetSysErrors - Memory information
func (sys *NotificationSys) GetSysErrors(ctx context.Context) []madmin.SysErrors {
	reply := make([]madmin.SysErrors, len(sys.peerClients))

	g := errgroup.WithNErrs(len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			reply[index], err = sys.peerClients[index].GetSysErrors(ctx)
			return err
		}, index)
	}

	for index, err := range g.Wait() {
		if err != nil {
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
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
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
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
			sys.addNodeErr(&reply[index], sys.peerClients[index], err)
		}
	}
	return reply
}

// Construct a list of offline disks information for a given node.
// If offlineHost is empty, do it for the local disks.
func getOfflineDisks(offlineHost string, endpoints EndpointServerPools) []madmin.Disk {
	var offlineDisks []madmin.Disk
	for _, pool := range endpoints {
		for _, ep := range pool.Endpoints {
			if offlineHost == "" && ep.IsLocal || offlineHost == ep.Host {
				offlineDisks = append(offlineDisks, madmin.Disk{
					Endpoint:  ep.String(),
					State:     string(madmin.ItemOffline),
					PoolIndex: ep.PoolIdx,
					SetIndex:  ep.SetIdx,
					DiskIndex: ep.DiskIdx,
				})
			}
		}
	}
	return offlineDisks
}

// StorageInfo returns disk information across all peers
func (sys *NotificationSys) StorageInfo(ctx context.Context, objLayer ObjectLayer, metrics bool) StorageInfo {
	var storageInfo StorageInfo
	replies := make([]StorageInfo, len(sys.peerClients))

	var wg sync.WaitGroup
	for i, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient, idx int) {
			defer wg.Done()
			info, err := client.LocalStorageInfo(ctx, metrics)
			if err != nil {
				info.Disks = getOfflineDisks(client.host.String(), globalEndpoints)
			}
			replies[idx] = info
		}(client, i)
	}
	wg.Wait()

	// Add local to this server.
	replies = append(replies, objLayer.LocalStorageInfo(ctx, metrics))

	storageInfo.Backend = objLayer.BackendInfo()
	for _, sinfo := range replies {
		storageInfo.Disks = append(storageInfo.Disks, sinfo.Disks...)
	}

	return storageInfo
}

// ServerInfo - calls ServerInfo RPC call on all peers.
func (sys *NotificationSys) ServerInfo(ctx context.Context, metrics bool) []madmin.ServerProperties {
	reply := make([]madmin.ServerProperties, len(sys.peerClients))
	var wg sync.WaitGroup
	for i, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient, idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			info, err := client.ServerInfo(ctx, metrics)
			if err != nil {
				info.Endpoint = client.host.String()
				info.State = string(madmin.ItemOffline)
				info.Disks = getOfflineDisks(info.Endpoint, globalEndpoints)
			}
			reply[idx] = info
		}(client, i)
	}
	wg.Wait()

	return reply
}

// restClientFromHash will return a deterministic peerRESTClient based on s.
// Will return nil if client is local.
func (sys *NotificationSys) restClientFromHash(s string) (client *peerRESTClient) {
	if len(sys.peerClients) == 0 {
		return nil
	}
	peerClients := sys.allPeerClients
	if len(peerClients) == 0 {
		return nil
	}
	idx := xxhash.Sum64String(s) % uint64(len(peerClients))
	return peerClients[idx]
}

// GetPeerOnlineCount gets the count of online and offline nodes.
func (sys *NotificationSys) GetPeerOnlineCount() (nodesOnline, nodesOffline int) {
	nodesOnline = 1 // Self is always online.
	nodesOffline = 0
	nodesOnlineIndex := make([]bool, len(sys.peerClients))
	var wg sync.WaitGroup
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, client *peerRESTClient) {
			defer wg.Done()
			nodesOnlineIndex[idx] = client.restClient.HealthCheckFn()
		}(idx, client)
	}
	wg.Wait()

	for _, online := range nodesOnlineIndex {
		if online {
			nodesOnline++
		} else {
			nodesOffline++
		}
	}
	return nodesOnline, nodesOffline
}

// NewNotificationSys - creates new notification system object.
func NewNotificationSys(endpoints EndpointServerPools) *NotificationSys {
	remote, all := newPeerRestClients(endpoints)
	return &NotificationSys{
		peerClients:    remote,
		allPeerClients: all,
	}
}

// GetBandwidthReports - gets the bandwidth report from all nodes including self.
func (sys *NotificationSys) GetBandwidthReports(ctx context.Context, buckets ...string) bandwidth.BucketBandwidthReport {
	reports := make([]*bandwidth.BucketBandwidthReport, len(sys.peerClients))
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
		peersLogOnceIf(ctx, err, sys.peerClients[index].host.String())
	}
	reports = append(reports, globalBucketMonitor.GetReport(bandwidth.SelectBuckets(buckets...)))
	consolidatedReport := bandwidth.BucketBandwidthReport{
		BucketStats: make(map[bandwidth.BucketOptions]bandwidth.Details),
	}
	for _, report := range reports {
		if report == nil || report.BucketStats == nil {
			continue
		}
		for opts := range report.BucketStats {
			d, ok := consolidatedReport.BucketStats[opts]
			if !ok {
				d = bandwidth.Details{
					LimitInBytesPerSecond: report.BucketStats[opts].LimitInBytesPerSecond,
				}
			}
			dt, ok := report.BucketStats[opts]
			if ok {
				d.CurrentBandwidthInBytesPerSecond += dt.CurrentBandwidthInBytesPerSecond
			}
			consolidatedReport.BucketStats[opts] = d
		}
	}
	return consolidatedReport
}

func (sys *NotificationSys) collectPeerMetrics(ctx context.Context, peerChannels []<-chan MetricV2, g *errgroup.Group) <-chan MetricV2 {
	ch := make(chan MetricV2)
	var wg sync.WaitGroup
	for index, err := range g.Wait() {
		if err != nil {
			if sys.peerClients[index] != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress",
					sys.peerClients[index].host.String())
				peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), err, sys.peerClients[index].host.String())
			} else {
				peersLogOnceIf(ctx, err, "peer-offline")
			}
			continue
		}
		wg.Add(1)
		go func(ctx context.Context, peerChannel <-chan MetricV2, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case m, ok := <-peerChannel:
					if !ok {
						return
					}
					select {
					case ch <- m:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(ctx, peerChannels[index], &wg)
	}
	go func(wg *sync.WaitGroup, ch chan MetricV2) {
		wg.Wait()
		xioutil.SafeClose(ch)
	}(&wg, ch)
	return ch
}

// GetBucketMetrics - gets the cluster level bucket metrics from all nodes excluding self.
func (sys *NotificationSys) GetBucketMetrics(ctx context.Context) <-chan MetricV2 {
	if sys == nil {
		return nil
	}
	g := errgroup.WithNErrs(len(sys.peerClients))
	peerChannels := make([]<-chan MetricV2, len(sys.peerClients))
	for index := range sys.peerClients {
		g.Go(func() error {
			if sys.peerClients[index] == nil {
				return errPeerNotReachable
			}
			var err error
			peerChannels[index], err = sys.peerClients[index].GetPeerBucketMetrics(ctx)
			return err
		}, index)
	}
	return sys.collectPeerMetrics(ctx, peerChannels, g)
}

// GetClusterMetrics - gets the cluster metrics from all nodes excluding self.
func (sys *NotificationSys) GetClusterMetrics(ctx context.Context) <-chan MetricV2 {
	if sys == nil {
		return nil
	}
	g := errgroup.WithNErrs(len(sys.peerClients))
	peerChannels := make([]<-chan MetricV2, len(sys.peerClients))
	for index := range sys.peerClients {
		g.Go(func() error {
			if sys.peerClients[index] == nil {
				return errPeerNotReachable
			}
			var err error
			peerChannels[index], err = sys.peerClients[index].GetPeerMetrics(ctx)
			return err
		}, index)
	}
	return sys.collectPeerMetrics(ctx, peerChannels, g)
}

// ServiceFreeze freezes all S3 API calls when 'freeze' is true,
// 'freeze' is 'false' would resume all S3 API calls again.
// NOTE: once a tenant is frozen either two things needs to
// happen before resuming normal operations.
//   - Server needs to be restarted 'mc admin service restart'
//   - 'freeze' should be set to 'false' for this call
//     to resume normal operations.
func (sys *NotificationSys) ServiceFreeze(ctx context.Context, freeze bool) []NotificationPeerErr {
	serviceSig := serviceUnFreeze
	if freeze {
		serviceSig = serviceFreeze
	}
	ng := WithNPeers(len(sys.peerClients))
	for idx, client := range sys.peerClients {
		if client == nil {
			continue
		}
		client := client
		ng.Go(GlobalContext, func() error {
			return client.SignalService(serviceSig, "", false, nil)
		}, idx, *client.host)
	}
	nerrs := ng.Wait()
	if freeze {
		freezeServices()
	} else {
		unfreezeServices()
	}
	return nerrs
}

// Netperf - perform mesh style network throughput test
func (sys *NotificationSys) Netperf(ctx context.Context, duration time.Duration) []madmin.NetperfNodeResult {
	length := len(sys.allPeerClients)
	if length == 0 {
		// For single node erasure setup.
		return nil
	}
	results := make([]madmin.NetperfNodeResult, length)

	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	var wg sync.WaitGroup
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			r, err := sys.peerClients[index].Netperf(ctx, duration)
			u := &url.URL{
				Scheme: scheme,
				Host:   sys.peerClients[index].host.String(),
			}
			if err != nil {
				results[index].Error = err.Error()
			} else {
				results[index] = r
			}
			results[index].Endpoint = u.String()
		}(index)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		r := netperf(ctx, duration)
		u := &url.URL{
			Scheme: scheme,
			Host:   globalLocalNodeName,
		}
		results[len(results)-1] = r
		results[len(results)-1].Endpoint = u.String()
	}()
	wg.Wait()

	return results
}

// SpeedTest run GET/PUT tests at input concurrency for requested object size,
// optionally you can extend the tests longer with time.Duration.
func (sys *NotificationSys) SpeedTest(ctx context.Context, sopts speedTestOpts) []SpeedTestResult {
	length := len(sys.allPeerClients)
	if length == 0 {
		// For single node erasure setup.
		length = 1
	}
	results := make([]SpeedTestResult, length)

	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	var wg sync.WaitGroup
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			r, err := sys.peerClients[index].SpeedTest(ctx, sopts)
			u := &url.URL{
				Scheme: scheme,
				Host:   sys.peerClients[index].host.String(),
			}
			if err != nil {
				results[index].Error = err.Error()
			} else {
				results[index] = r
			}
			results[index].Endpoint = u.String()
		}(index)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := selfSpeedTest(ctx, sopts)
		u := &url.URL{
			Scheme: scheme,
			Host:   globalLocalNodeName,
		}
		if err != nil {
			results[len(results)-1].Error = err.Error()
		} else {
			results[len(results)-1] = r
		}
		results[len(results)-1].Endpoint = u.String()
	}()
	wg.Wait()

	return results
}

// DriveSpeedTest - Drive performance information
func (sys *NotificationSys) DriveSpeedTest(ctx context.Context, opts madmin.DriveSpeedTestOpts) chan madmin.DriveSpeedTestResult {
	ch := make(chan madmin.DriveSpeedTestResult)
	var wg sync.WaitGroup
	for _, client := range sys.peerClients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(client *peerRESTClient) {
			defer wg.Done()
			resp, err := client.DriveSpeedTest(ctx, opts)
			if err != nil {
				resp.Error = err.Error()
			}

			select {
			case <-ctx.Done():
			case ch <- resp:
			}

			reqInfo := (&logger.ReqInfo{}).AppendTags("remotePeer", client.host.String())
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			peersLogOnceIf(ctx, err, client.host.String())
		}(client)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case ch <- driveSpeedTest(ctx, opts):
		}
	}()

	go func(wg *sync.WaitGroup, ch chan madmin.DriveSpeedTestResult) {
		wg.Wait()
		xioutil.SafeClose(ch)
	}(&wg, ch)

	return ch
}

// ReloadSiteReplicationConfig - tells all peer minio nodes to reload the
// site-replication configuration.
func (sys *NotificationSys) ReloadSiteReplicationConfig(ctx context.Context) []error {
	errs := make([]error, len(sys.allPeerClients))
	var wg sync.WaitGroup
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errs[index] = sys.peerClients[index].ReloadSiteReplicationConfig(ctx)
		}(index)
	}

	wg.Wait()
	return errs
}

// GetLastDayTierStats fetches per-tier stats of the last 24hrs from all peers
func (sys *NotificationSys) GetLastDayTierStats(ctx context.Context) DailyAllTierStats {
	errs := make([]error, len(sys.allPeerClients))
	lastDayStats := make([]DailyAllTierStats, len(sys.allPeerClients))
	var wg sync.WaitGroup
	for index := range sys.peerClients {
		if sys.peerClients[index] == nil {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			lastDayStats[index], errs[index] = sys.peerClients[index].GetLastDayTierStats(ctx)
		}(index)
	}

	wg.Wait()
	merged := globalTransitionState.getDailyAllTierStats()
	for i, stat := range lastDayStats {
		if errs[i] != nil {
			peersLogOnceIf(ctx, fmt.Errorf("failed to fetch last day tier stats: %w", errs[i]), sys.peerClients[i].host.String())
			continue
		}
		merged.merge(stat)
	}
	return merged
}

// GetReplicationMRF - Get replication MRF from all peers.
func (sys *NotificationSys) GetReplicationMRF(ctx context.Context, bucket, node string) (mrfCh chan madmin.ReplicationMRF, err error) {
	g := errgroup.WithNErrs(len(sys.peerClients))
	peerChannels := make([]<-chan madmin.ReplicationMRF, len(sys.peerClients))
	for index, client := range sys.peerClients {
		if client == nil {
			continue
		}
		host := client.host.String()
		if host != node && node != "all" {
			continue
		}
		index := index
		g.Go(func() error {
			var err error
			peerChannels[index], err = sys.peerClients[index].GetReplicationMRF(ctx, bucket)
			return err
		}, index)
	}
	mrfCh = make(chan madmin.ReplicationMRF, 4000)
	var wg sync.WaitGroup

	for index, err := range g.Wait() {
		if err != nil {
			if sys.peerClients[index] != nil {
				reqInfo := (&logger.ReqInfo{}).AppendTags("peerAddress",
					sys.peerClients[index].host.String())
				peersLogOnceIf(logger.SetReqInfo(ctx, reqInfo), err, sys.peerClients[index].host.String())
			} else {
				peersLogOnceIf(ctx, err, "peer-offline")
			}
			continue
		}
		wg.Add(1)
		go func(ctx context.Context, peerChannel <-chan madmin.ReplicationMRF, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case m, ok := <-peerChannel:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case mrfCh <- m:
					}
				case <-ctx.Done():
					return
				}
			}
		}(ctx, peerChannels[index], &wg)
	}
	wg.Add(1)
	go func(ch chan madmin.ReplicationMRF) error {
		defer wg.Done()
		if node != "all" && node != globalLocalNodeName {
			return nil
		}
		mCh, err := globalReplicationPool.Get().getMRF(ctx, bucket)
		if err != nil {
			return err
		}
		for e := range mCh {
			select {
			case <-ctx.Done():
				return err
			case mrfCh <- e:
			}
		}
		return nil
	}(mrfCh)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		xioutil.SafeClose(mrfCh)
	}(&wg)
	return mrfCh, nil
}
