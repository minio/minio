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
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/grid"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/rest"
	xnet "github.com/minio/pkg/v3/net"
)

// client to talk to peer Nodes.
type peerRESTClient struct {
	host       *xnet.Host
	restClient *rest.Client
	gridHost   string
	// Function that returns the grid connection for this peer when initialized.
	// Will return nil if the grid connection is not initialized yet.
	gridConn func() *grid.Connection
}

// Returns a peer rest client.
func newPeerRESTClient(peer *xnet.Host, gridHost string) *peerRESTClient {
	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   peerRESTPath,
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	// Use a separate client to avoid recursive calls.
	healthClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	healthClient.NoMetrics = true

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, peerRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}
	var gridConn atomic.Pointer[grid.Connection]

	return &peerRESTClient{
		host: peer, restClient: restClient, gridHost: gridHost,
		gridConn: func() *grid.Connection {
			// Lazy initialization of grid connection.
			// When we create this peer client, the grid connection is likely not yet initialized.
			if gridHost == "" {
				bugLogIf(context.Background(), fmt.Errorf("gridHost is empty for peer %s", peer.String()), peer.String()+":gridHost")
				return nil
			}
			gc := gridConn.Load()
			if gc != nil {
				return gc
			}
			gm := globalGrid.Load()
			if gm == nil {
				return nil
			}
			gc = gm.Connection(gridHost)
			if gc == nil {
				bugLogIf(context.Background(), fmt.Errorf("gridHost %q not found for peer %s", gridHost, peer.String()), peer.String()+":gridHost")
				return nil
			}
			gridConn.Store(gc)
			return gc
		},
	}
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if client == nil {
		return nil, errPeerNotReachable
	}

	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	if xnet.IsNetworkOrHostDown(err, true) {
		return nil, errPeerNotReachable
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// IsOnline returns true if the peer client is online.
func (client *peerRESTClient) IsOnline() bool {
	conn := client.gridConn()
	if conn == nil {
		return false
	}
	return client.restClient.IsOnline() || conn.State() == grid.StateConnected
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// GetLocks - fetch older locks for a remote node.
func (client *peerRESTClient) GetLocks(ctx context.Context) (lockMap map[string][]lockRequesterInfo, err error) {
	resp, err := getLocksRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil || resp == nil {
		return nil, err
	}
	return *resp, nil
}

// LocalStorageInfo - fetch server information for a remote node.
func (client *peerRESTClient) LocalStorageInfo(ctx context.Context, metrics bool) (info StorageInfo, err error) {
	resp, err := localStorageInfoRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTMetrics: strconv.FormatBool(metrics),
	}))
	return resp.ValueOrZero(), err
}

// ServerInfo - fetch server information for a remote node.
func (client *peerRESTClient) ServerInfo(ctx context.Context, metrics bool) (info madmin.ServerProperties, err error) {
	resp, err := serverInfoRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{peerRESTMetrics: strconv.FormatBool(metrics)}))
	return resp.ValueOrZero(), err
}

// GetCPUs - fetch CPU information for a remote node.
func (client *peerRESTClient) GetCPUs(ctx context.Context) (info madmin.CPUs, err error) {
	resp, err := getCPUsHandler.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetNetInfo - fetch network information for a remote node.
func (client *peerRESTClient) GetNetInfo(ctx context.Context) (info madmin.NetInfo, err error) {
	resp, err := getNetInfoRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetPartitions - fetch disk partition information for a remote node.
func (client *peerRESTClient) GetPartitions(ctx context.Context) (info madmin.Partitions, err error) {
	resp, err := getPartitionsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetOSInfo - fetch OS information for a remote node.
func (client *peerRESTClient) GetOSInfo(ctx context.Context) (info madmin.OSInfo, err error) {
	resp, err := getOSInfoRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetSELinuxInfo - fetch SELinux information for a remote node.
func (client *peerRESTClient) GetSELinuxInfo(ctx context.Context) (info madmin.SysServices, err error) {
	resp, err := getSysServicesRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetSysConfig - fetch sys config for a remote node.
func (client *peerRESTClient) GetSysConfig(ctx context.Context) (info madmin.SysConfig, err error) {
	sent := time.Now()
	resp, err := getSysConfigRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	info = resp.ValueOrZero()
	if ti, ok := info.Config["time-info"].(madmin.TimeInfo); ok {
		rt := int32(time.Since(sent).Milliseconds())
		ti.RoundtripDuration = rt
		info.Config["time-info"] = ti
	}
	return info, err
}

// GetSysErrors - fetch sys errors for a remote node.
func (client *peerRESTClient) GetSysErrors(ctx context.Context) (info madmin.SysErrors, err error) {
	resp, err := getSysErrorsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetMemInfo - fetch memory information for a remote node.
func (client *peerRESTClient) GetMemInfo(ctx context.Context) (info madmin.MemInfo, err error) {
	resp, err := getMemInfoRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetMetrics - fetch metrics from a remote node.
func (client *peerRESTClient) GetMetrics(ctx context.Context, t madmin.MetricType, opts collectMetricsOpts) (info madmin.RealtimeMetrics, err error) {
	values := make(url.Values)
	values.Set(peerRESTMetricsTypes, strconv.FormatUint(uint64(t), 10))
	for disk := range opts.disks {
		values.Add(peerRESTDisk, disk)
	}
	for host := range opts.hosts {
		values.Add(peerRESTHost, host)
	}
	values.Set(peerRESTJobID, opts.jobID)
	values.Set(peerRESTDepID, opts.depID)
	v, err := getMetricsRPC.Call(ctx, client.gridConn(), grid.NewURLValuesWith(values))
	return v.ValueOrZero(), err
}

// GetProcInfo - fetch MinIO process information for a remote node.
func (client *peerRESTClient) GetProcInfo(ctx context.Context) (info madmin.ProcInfo, err error) {
	resp, err := getProcInfoRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// StartProfiling - Issues profiling command on the peer node.
func (client *peerRESTClient) StartProfiling(ctx context.Context, profiler string) error {
	values := make(url.Values)
	values.Set(peerRESTProfiler, profiler)
	respBody, err := client.callWithContext(ctx, peerRESTMethodStartProfiling, values, nil, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)
	return nil
}

// DownloadProfileData - download profiled data from a remote node.
func (client *peerRESTClient) DownloadProfileData(ctx context.Context) (data map[string][]byte, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDownloadProfilingData, nil, nil, -1)
	if err != nil {
		return data, err
	}
	defer xhttp.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&data)
	return data, err
}

// GetBucketStats - load bucket statistics
func (client *peerRESTClient) GetBucketStats(ctx context.Context, bucket string) (BucketStats, error) {
	resp, err := getBucketStatsRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTBucket: bucket,
	}))
	if err != nil || resp == nil {
		return BucketStats{}, err
	}
	return *resp, nil
}

// GetSRMetrics loads site replication metrics, optionally for a specific bucket
func (client *peerRESTClient) GetSRMetrics(ctx context.Context) (SRMetricsSummary, error) {
	resp, err := getSRMetricsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil || resp == nil {
		return SRMetricsSummary{}, err
	}
	return *resp, nil
}

// GetAllBucketStats - load replication stats for all buckets
func (client *peerRESTClient) GetAllBucketStats(ctx context.Context) (BucketStatsMap, error) {
	resp, err := getAllBucketStatsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil || resp == nil {
		return BucketStatsMap{}, err
	}
	return *resp, nil
}

// LoadBucketMetadata - load bucket metadata
func (client *peerRESTClient) LoadBucketMetadata(ctx context.Context, bucket string) error {
	_, err := loadBucketMetadataRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTBucket: bucket,
	}))
	return err
}

// DeleteBucketMetadata - Delete bucket metadata
func (client *peerRESTClient) DeleteBucketMetadata(ctx context.Context, bucket string) error {
	_, err := deleteBucketMetadataRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTBucket: bucket,
	}))
	return err
}

// DeletePolicy - delete a specific canned policy.
func (client *peerRESTClient) DeletePolicy(ctx context.Context, policyName string) (err error) {
	_, err = deletePolicyRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTPolicy: policyName,
	}))
	return err
}

// LoadPolicy - reload a specific canned policy.
func (client *peerRESTClient) LoadPolicy(ctx context.Context, policyName string) (err error) {
	_, err = loadPolicyRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTPolicy: policyName,
	}))
	return err
}

// LoadPolicyMapping - reload a specific policy mapping
func (client *peerRESTClient) LoadPolicyMapping(ctx context.Context, userOrGroup string, userType IAMUserType, isGroup bool) error {
	_, err := loadPolicyMappingRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTUserOrGroup: userOrGroup,
		peerRESTUserType:    strconv.Itoa(int(userType)),
		peerRESTIsGroup:     strconv.FormatBool(isGroup),
	}))
	return err
}

// DeleteUser - delete a specific user.
func (client *peerRESTClient) DeleteUser(ctx context.Context, accessKey string) (err error) {
	_, err = deleteUserRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTUser: accessKey,
	}))
	return err
}

// DeleteServiceAccount - delete a specific service account.
func (client *peerRESTClient) DeleteServiceAccount(ctx context.Context, accessKey string) (err error) {
	_, err = deleteSvcActRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTUser: accessKey,
	}))
	return err
}

// LoadUser - reload a specific user.
func (client *peerRESTClient) LoadUser(ctx context.Context, accessKey string, temp bool) (err error) {
	_, err = loadUserRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTUser:     accessKey,
		peerRESTUserTemp: strconv.FormatBool(temp),
	}))
	return err
}

// LoadServiceAccount - reload a specific service account.
func (client *peerRESTClient) LoadServiceAccount(ctx context.Context, accessKey string) (err error) {
	_, err = loadSvcActRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTUser: accessKey,
	}))
	return err
}

// LoadGroup - send load group command to peers.
func (client *peerRESTClient) LoadGroup(ctx context.Context, group string) error {
	_, err := loadGroupRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTGroup: group,
	}))
	return err
}

func (client *peerRESTClient) ReloadSiteReplicationConfig(ctx context.Context) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}

	_, err := reloadSiteReplicationConfigRPC.Call(ctx, conn, grid.NewMSS())
	return err
}

// VerifyBinary - sends verify binary message to remote peers.
func (client *peerRESTClient) VerifyBinary(ctx context.Context, u *url.URL, sha256Sum []byte, releaseInfo string, reader io.Reader) error {
	values := make(url.Values)
	values.Set(peerRESTURL, u.String())
	values.Set(peerRESTSha256Sum, hex.EncodeToString(sha256Sum))
	values.Set(peerRESTReleaseInfo, releaseInfo)

	respBody, err := client.callWithContext(ctx, peerRESTMethodVerifyBinary, values, reader, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)
	return nil
}

// CommitBinary - sends commit binary message to remote peers.
func (client *peerRESTClient) CommitBinary(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodCommitBinary, nil, nil, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)
	return nil
}

// SignalService - sends signal to peer nodes.
func (client *peerRESTClient) SignalService(sig serviceSignal, subSys string, dryRun bool, execAt *time.Time) error {
	values := grid.NewMSS()
	values.Set(peerRESTSignal, strconv.Itoa(int(sig)))
	values.Set(peerRESTDryRun, strconv.FormatBool(dryRun))
	values.Set(peerRESTSubSys, subSys)
	if execAt != nil {
		values.Set(peerRESTExecAt, execAt.Format(time.RFC3339Nano))
	}
	_, err := signalServiceRPC.Call(context.Background(), client.gridConn(), values)
	return err
}

func (client *peerRESTClient) BackgroundHealStatus(ctx context.Context) (madmin.BgHealState, error) {
	resp, err := getBackgroundHealStatusRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	return resp.ValueOrZero(), err
}

// GetMetacacheListing - get a new or existing metacache.
func (client *peerRESTClient) GetMetacacheListing(ctx context.Context, o listPathOptions) (*metacache, error) {
	if client == nil {
		resp := localMetacacheMgr.getBucket(ctx, o.Bucket).findCache(o)
		return &resp, nil
	}
	return getMetacacheListingRPC.Call(ctx, client.gridConn(), &o)
}

// UpdateMetacacheListing - update an existing metacache it will unconditionally be updated to the new state.
func (client *peerRESTClient) UpdateMetacacheListing(ctx context.Context, m metacache) (metacache, error) {
	if client == nil {
		return localMetacacheMgr.updateCacheEntry(m)
	}
	resp, err := updateMetacacheListingRPC.Call(ctx, client.gridConn(), &m)
	if err != nil || resp == nil {
		return metacache{}, err
	}
	return *resp, nil
}

func (client *peerRESTClient) ReloadPoolMeta(ctx context.Context) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}
	_, err := reloadPoolMetaRPC.Call(ctx, conn, grid.NewMSSWith(map[string]string{}))
	return err
}

func (client *peerRESTClient) DeleteUploadID(ctx context.Context, uploadID string) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}
	_, err := cleanupUploadIDCacheMetaRPC.Call(ctx, conn, grid.NewMSSWith(map[string]string{
		peerRESTUploadID: uploadID,
	}))
	return err
}

func (client *peerRESTClient) StopRebalance(ctx context.Context) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}
	_, err := stopRebalanceRPC.Call(ctx, conn, grid.NewMSSWith(map[string]string{}))
	return err
}

func (client *peerRESTClient) LoadRebalanceMeta(ctx context.Context, startRebalance bool) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}
	_, err := loadRebalanceMetaRPC.Call(ctx, conn, grid.NewMSSWith(map[string]string{
		peerRESTStartRebalance: strconv.FormatBool(startRebalance),
	}))
	return err
}

func (client *peerRESTClient) LoadTransitionTierConfig(ctx context.Context) error {
	conn := client.gridConn()
	if conn == nil {
		return nil
	}
	_, err := loadTransitionTierConfigRPC.Call(ctx, conn, grid.NewMSSWith(map[string]string{}))
	return err
}

func (client *peerRESTClient) doTrace(ctx context.Context, traceCh chan<- []byte, traceOpts madmin.ServiceTraceOpts) {
	gridConn := client.gridConn()
	if gridConn == nil {
		return
	}

	payload, err := json.Marshal(traceOpts)
	if err != nil {
		bugLogIf(ctx, err)
		return
	}

	st, err := gridConn.NewStream(ctx, grid.HandlerTrace, payload)
	if err != nil {
		return
	}
	st.Results(func(b []byte) error {
		select {
		case traceCh <- b:
		default:
			// Do not block on slow receivers.
			// Just recycle the buffer.
			grid.PutByteBuffer(b)
		}
		return nil
	})
}

func (client *peerRESTClient) doListen(ctx context.Context, listenCh chan<- []byte, v url.Values) {
	conn := client.gridConn()
	if conn == nil {
		return
	}
	st, err := listenRPC.Call(ctx, conn, grid.NewURLValuesWith(v))
	if err != nil {
		return
	}
	st.Results(func(b *grid.Bytes) error {
		select {
		case listenCh <- *b:
		default:
			// Do not block on slow receivers.
			b.Recycle()
		}
		return nil
	})
}

// Listen - listen on peers.
func (client *peerRESTClient) Listen(ctx context.Context, listenCh chan<- []byte, v url.Values) {
	go func() {
		for {
			client.doListen(ctx, listenCh, v)
			select {
			case <-ctx.Done():
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

// Trace - send http trace request to peer nodes
func (client *peerRESTClient) Trace(ctx context.Context, traceCh chan<- []byte, traceOpts madmin.ServiceTraceOpts) {
	go func() {
		for {
			// Blocks until context is canceled or an error occurs.
			client.doTrace(ctx, traceCh, traceOpts)
			select {
			case <-ctx.Done():
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func (client *peerRESTClient) doConsoleLog(ctx context.Context, kind madmin.LogMask, logCh chan<- []byte) {
	st, err := consoleLogRPC.Call(ctx, client.gridConn(), grid.NewMSSWith(map[string]string{
		peerRESTLogMask: strconv.Itoa(int(kind)),
	}))
	if err != nil {
		return
	}
	st.Results(func(b *grid.Bytes) error {
		select {
		case logCh <- *b:
		default:
			consoleLogRPC.PutResponse(b)
			// Do not block on slow receivers.
		}
		return nil
	})
}

// ConsoleLog - sends request to peer nodes to get console logs
func (client *peerRESTClient) ConsoleLog(ctx context.Context, kind madmin.LogMask, logCh chan<- []byte) {
	go func() {
		for {
			client.doConsoleLog(ctx, kind, logCh)
			select {
			case <-ctx.Done():
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

// newPeerRestClients creates new peer clients.
// The two slices will point to the same clients,
// but 'all' will contain nil entry for local client.
// The 'all' slice will be in the same order across the cluster.
func newPeerRestClients(endpoints EndpointServerPools) (remote, all []*peerRESTClient) {
	if !globalIsDistErasure {
		// Only useful in distributed setups
		return nil, nil
	}

	hosts := endpoints.hostsSorted()
	remote = make([]*peerRESTClient, 0, len(hosts))
	all = make([]*peerRESTClient, len(hosts))
	for i, host := range hosts {
		if host == nil {
			continue
		}
		all[i] = newPeerRESTClient(host, endpoints.FindGridHostsFromPeer(host))
		remote = append(remote, all[i])
	}
	if len(all) != len(remote)+1 {
		peersLogIf(context.Background(), fmt.Errorf("Expected number of all hosts (%v) to be remote +1 (%v)", len(all), len(remote)), logger.WarningKind)
	}
	return remote, all
}

// MonitorBandwidth - send http trace request to peer nodes
func (client *peerRESTClient) MonitorBandwidth(ctx context.Context, buckets []string) (*bandwidth.BucketBandwidthReport, error) {
	values := grid.NewURLValuesWith(map[string][]string{
		peerRESTBuckets: buckets,
	})
	return getBandwidthRPC.Call(ctx, client.gridConn(), values)
}

func (client *peerRESTClient) GetResourceMetrics(ctx context.Context) (<-chan MetricV2, error) {
	resp, err := getResourceMetricsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil {
		return nil, err
	}
	ch := make(chan MetricV2)
	go func(ch chan<- MetricV2) {
		defer close(ch)
		for _, m := range resp.Value() {
			if m == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- *m:
			}
		}
	}(ch)
	return ch, nil
}

func (client *peerRESTClient) GetPeerMetrics(ctx context.Context) (<-chan MetricV2, error) {
	resp, err := getPeerMetricsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil {
		return nil, err
	}
	ch := make(chan MetricV2)
	go func() {
		defer close(ch)
		for _, m := range resp.Value() {
			if m == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- *m:
			}
		}
	}()
	return ch, nil
}

func (client *peerRESTClient) GetPeerBucketMetrics(ctx context.Context) (<-chan MetricV2, error) {
	resp, err := getPeerBucketMetricsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil {
		return nil, err
	}
	ch := make(chan MetricV2)
	go func() {
		defer close(ch)
		for _, m := range resp.Value() {
			if m == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case ch <- *m:
			}
		}
	}()
	return ch, nil
}

func (client *peerRESTClient) SpeedTest(ctx context.Context, opts speedTestOpts) (SpeedTestResult, error) {
	values := make(url.Values)
	values.Set(peerRESTSize, strconv.Itoa(opts.objectSize))
	values.Set(peerRESTConcurrent, strconv.Itoa(opts.concurrency))
	values.Set(peerRESTDuration, opts.duration.String())
	values.Set(peerRESTStorageClass, opts.storageClass)
	values.Set(peerRESTBucket, opts.bucketName)
	values.Set(peerRESTEnableSha256, strconv.FormatBool(opts.enableSha256))
	values.Set(peerRESTEnableMultipart, strconv.FormatBool(opts.enableMultipart))
	values.Set(peerRESTAccessKey, opts.creds.AccessKey)

	respBody, err := client.callWithContext(context.Background(), peerRESTMethodSpeedTest, values, nil, -1)
	if err != nil {
		return SpeedTestResult{}, err
	}
	defer xhttp.DrainBody(respBody)
	waitReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return SpeedTestResult{}, err
	}

	var result SpeedTestResult
	err = gob.NewDecoder(waitReader).Decode(&result)
	if err != nil {
		return result, err
	}
	if result.Error != "" {
		return result, errors.New(result.Error)
	}
	return result, nil
}

func (client *peerRESTClient) DriveSpeedTest(ctx context.Context, opts madmin.DriveSpeedTestOpts) (madmin.DriveSpeedTestResult, error) {
	queryVals := make(url.Values)
	if opts.Serial {
		queryVals.Set("serial", "true")
	}
	queryVals.Set("blocksize", strconv.FormatUint(opts.BlockSize, 10))
	queryVals.Set("filesize", strconv.FormatUint(opts.FileSize, 10))

	respBody, err := client.callWithContext(ctx, peerRESTMethodDriveSpeedTest, queryVals, nil, -1)
	if err != nil {
		return madmin.DriveSpeedTestResult{}, err
	}
	defer xhttp.DrainBody(respBody)
	waitReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return madmin.DriveSpeedTestResult{}, err
	}

	var result madmin.DriveSpeedTestResult
	err = gob.NewDecoder(waitReader).Decode(&result)
	if err != nil {
		return result, err
	}
	if result.Error != "" {
		return result, errors.New(result.Error)
	}
	return result, nil
}

func (client *peerRESTClient) GetLastDayTierStats(ctx context.Context) (DailyAllTierStats, error) {
	resp, err := getLastDayTierStatsRPC.Call(ctx, client.gridConn(), grid.NewMSS())
	if err != nil || resp == nil {
		return DailyAllTierStats{}, err
	}
	return *resp, nil
}

// DevNull - Used by netperf to pump data to peer
func (client *peerRESTClient) DevNull(ctx context.Context, r io.Reader) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDevNull, nil, r, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)
	return err
}

// Netperf - To initiate netperf on peer
func (client *peerRESTClient) Netperf(ctx context.Context, duration time.Duration) (madmin.NetperfNodeResult, error) {
	var result madmin.NetperfNodeResult
	values := make(url.Values)
	values.Set(peerRESTDuration, duration.String())
	respBody, err := client.callWithContext(context.Background(), peerRESTMethodNetperf, values, nil, -1)
	if err != nil {
		return result, err
	}
	defer xhttp.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&result)
	return result, err
}

// GetReplicationMRF - get replication MRF for bucket
func (client *peerRESTClient) GetReplicationMRF(ctx context.Context, bucket string) (chan madmin.ReplicationMRF, error) {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	respBody, err := client.callWithContext(ctx, peerRESTMethodGetReplicationMRF, values, nil, -1)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(respBody)
	ch := make(chan madmin.ReplicationMRF)
	go func(ch chan madmin.ReplicationMRF) {
		defer func() {
			xhttp.DrainBody(respBody)
			close(ch)
		}()
		for {
			var entry madmin.ReplicationMRF
			if err := dec.Decode(&entry); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case ch <- entry:
			}
		}
	}(ch)
	return ch, nil
}
