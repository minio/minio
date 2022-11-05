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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/http"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/message/log"
	"github.com/minio/minio/internal/rest"
	xnet "github.com/minio/pkg/net"
	"github.com/tinylib/msgp/msgp"
)

// client to talk to peer Nodes.
type peerRESTClient struct {
	host       *xnet.Host
	restClient *rest.Client
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(GlobalContext, method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// IsOnline returns true if the peer client is online.
func (client *peerRESTClient) IsOnline() bool {
	return client.restClient.IsOnline()
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// GetLocks - fetch older locks for a remote node.
func (client *peerRESTClient) GetLocks() (lockMap map[string][]lockRequesterInfo, err error) {
	respBody, err := client.call(peerRESTMethodGetLocks, nil, nil, -1)
	if err != nil {
		return
	}
	lockMap = map[string][]lockRequesterInfo{}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&lockMap)
	return lockMap, err
}

// ServerInfo - fetch server information for a remote node.
func (client *peerRESTClient) ServerInfo() (info madmin.ServerProperties, err error) {
	respBody, err := client.call(peerRESTMethodServerInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetCPUs - fetch CPU information for a remote node.
func (client *peerRESTClient) GetCPUs(ctx context.Context) (info madmin.CPUs, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodCPUInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetPartitions - fetch disk partition information for a remote node.
func (client *peerRESTClient) GetPartitions(ctx context.Context) (info madmin.Partitions, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDiskHwInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetOSInfo - fetch OS information for a remote node.
func (client *peerRESTClient) GetOSInfo(ctx context.Context) (info madmin.OSInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodOsInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetSELinuxInfo - fetch SELinux information for a remote node.
func (client *peerRESTClient) GetSELinuxInfo(ctx context.Context) (info madmin.SysServices, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodSysServices, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetSysConfig - fetch sys config for a remote node.
func (client *peerRESTClient) GetSysConfig(ctx context.Context) (info madmin.SysConfig, err error) {
	sent := time.Now()
	respBody, err := client.callWithContext(ctx, peerRESTMethodSysConfig, nil, nil, -1)
	if err != nil {
		return
	}
	roundtrip := int32(time.Since(sent).Milliseconds())
	defer http.DrainBody(respBody)

	err = gob.NewDecoder(respBody).Decode(&info)
	cfg := info.Config["time-info"]
	if cfg != nil {
		ti := cfg.(madmin.TimeInfo)
		ti.RoundtripDuration = roundtrip
		info.Config["time-info"] = ti
	}
	return info, err
}

// GetSysErrors - fetch sys errors for a remote node.
func (client *peerRESTClient) GetSysErrors(ctx context.Context) (info madmin.SysErrors, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodSysErrors, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetMemInfo - fetch memory information for a remote node.
func (client *peerRESTClient) GetMemInfo(ctx context.Context) (info madmin.MemInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodMemInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetMetrics - fetch metrics from a remote node.
func (client *peerRESTClient) GetMetrics(ctx context.Context, t madmin.MetricType, opts collectMetricsOpts) (info madmin.RealtimeMetrics, err error) {
	values := make(url.Values)
	values.Set(peerRESTMetricsTypes, strconv.FormatUint(uint64(t), 10))
	for disk := range opts.disks {
		values.Set(peerRESTDisk, disk)
	}
	values.Set(peerRESTJobID, opts.jobID)
	respBody, err := client.callWithContext(ctx, peerRESTMethodMetrics, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetProcInfo - fetch MinIO process information for a remote node.
func (client *peerRESTClient) GetProcInfo(ctx context.Context) (info madmin.ProcInfo, err error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodProcInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// StartProfiling - Issues profiling command on the peer node.
func (client *peerRESTClient) StartProfiling(profiler string) error {
	values := make(url.Values)
	values.Set(peerRESTProfiler, profiler)
	respBody, err := client.call(peerRESTMethodStartProfiling, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DownloadProfileData - download profiled data from a remote node.
func (client *peerRESTClient) DownloadProfileData() (data map[string][]byte, err error) {
	respBody, err := client.call(peerRESTMethodDownloadProfilingData, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&data)
	return data, err
}

// GetBucketStats - load bucket statistics
func (client *peerRESTClient) GetBucketStats(bucket string) (BucketStats, error) {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodGetBucketStats, values, nil, -1)
	if err != nil {
		return BucketStats{}, err
	}

	var bs BucketStats
	defer http.DrainBody(respBody)
	return bs, msgp.Decode(respBody, &bs)
}

// GetAllBucketStats - load replication stats for all buckets
func (client *peerRESTClient) GetAllBucketStats() (BucketStatsMap, error) {
	values := make(url.Values)
	respBody, err := client.call(peerRESTMethodGetAllBucketStats, values, nil, -1)
	if err != nil {
		return BucketStatsMap{}, err
	}

	bsMap := BucketStatsMap{}
	defer http.DrainBody(respBody)
	return bsMap, msgp.Decode(respBody, &bsMap)
}

// LoadBucketMetadata - load bucket metadata
func (client *peerRESTClient) LoadBucketMetadata(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodLoadBucketMetadata, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteBucketMetadata - Delete bucket metadata
func (client *peerRESTClient) DeleteBucketMetadata(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodDeleteBucketMetadata, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// cycleServerBloomFilter will cycle the bloom filter to start recording to index y if not already.
// The response will contain a bloom filter starting at index x up to, but not including index y.
// If y is 0, the response will not update y, but return the currently recorded information
// from the current x to y-1.
func (client *peerRESTClient) cycleServerBloomFilter(ctx context.Context, req bloomFilterRequest) (*bloomFilterResponse, error) {
	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(req)
	if err != nil {
		return nil, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodCycleBloom, nil, &reader, -1)
	if err != nil {
		return nil, err
	}
	var resp bloomFilterResponse
	defer http.DrainBody(respBody)
	return &resp, gob.NewDecoder(respBody).Decode(&resp)
}

// DeletePolicy - delete a specific canned policy.
func (client *peerRESTClient) DeletePolicy(policyName string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTPolicy, policyName)

	respBody, err := client.call(peerRESTMethodDeletePolicy, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadPolicy - reload a specific canned policy.
func (client *peerRESTClient) LoadPolicy(policyName string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTPolicy, policyName)

	respBody, err := client.call(peerRESTMethodLoadPolicy, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadPolicyMapping - reload a specific policy mapping
func (client *peerRESTClient) LoadPolicyMapping(userOrGroup string, userType IAMUserType, isGroup bool) error {
	values := make(url.Values)
	values.Set(peerRESTUserOrGroup, userOrGroup)
	values.Set(peerRESTUserType, strconv.Itoa(int(userType)))
	if isGroup {
		values.Set(peerRESTIsGroup, "")
	}

	respBody, err := client.call(peerRESTMethodLoadPolicyMapping, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteUser - delete a specific user.
func (client *peerRESTClient) DeleteUser(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodDeleteUser, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// DeleteServiceAccount - delete a specific service account.
func (client *peerRESTClient) DeleteServiceAccount(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodDeleteServiceAccount, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadUser - reload a specific user.
func (client *peerRESTClient) LoadUser(accessKey string, temp bool) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)
	values.Set(peerRESTUserTemp, strconv.FormatBool(temp))

	respBody, err := client.call(peerRESTMethodLoadUser, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadServiceAccount - reload a specific service account.
func (client *peerRESTClient) LoadServiceAccount(accessKey string) (err error) {
	values := make(url.Values)
	values.Set(peerRESTUser, accessKey)

	respBody, err := client.call(peerRESTMethodLoadServiceAccount, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	return nil
}

// LoadGroup - send load group command to peers.
func (client *peerRESTClient) LoadGroup(group string) error {
	values := make(url.Values)
	values.Set(peerRESTGroup, group)
	respBody, err := client.call(peerRESTMethodLoadGroup, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

type binaryInfo struct {
	URL         *url.URL
	Sha256Sum   []byte
	ReleaseInfo string
	BinaryFile  []byte
}

// VerifyBinary - sends verify binary message to remote peers.
func (client *peerRESTClient) VerifyBinary(ctx context.Context, u *url.URL, sha256Sum []byte, releaseInfo string, readerInput []byte) error {
	values := make(url.Values)
	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(binaryInfo{
		URL:         u,
		Sha256Sum:   sha256Sum,
		ReleaseInfo: releaseInfo,
		BinaryFile:  readerInput,
	}); err != nil {
		return err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodDownloadBinary, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// CommitBinary - sends commit binary message to remote peers.
func (client *peerRESTClient) CommitBinary(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodCommitBinary, nil, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// SignalService - sends signal to peer nodes.
func (client *peerRESTClient) SignalService(sig serviceSignal, subSys string) error {
	values := make(url.Values)
	values.Set(peerRESTSignal, strconv.Itoa(int(sig)))
	values.Set(peerRESTSubSys, subSys)
	respBody, err := client.call(peerRESTMethodSignalService, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) BackgroundHealStatus() (madmin.BgHealState, error) {
	respBody, err := client.call(peerRESTMethodBackgroundHealStatus, nil, nil, -1)
	if err != nil {
		return madmin.BgHealState{}, err
	}
	defer http.DrainBody(respBody)

	state := madmin.BgHealState{}
	err = gob.NewDecoder(respBody).Decode(&state)
	return state, err
}

// GetLocalDiskIDs - get a peer's local disks' IDs.
func (client *peerRESTClient) GetLocalDiskIDs(ctx context.Context) (diskIDs []string) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetLocalDiskIDs, nil, nil, -1)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil
	}
	defer http.DrainBody(respBody)
	if err = gob.NewDecoder(respBody).Decode(&diskIDs); err != nil {
		logger.LogIf(ctx, err)
		return nil
	}
	return diskIDs
}

// GetMetacacheListing - get a new or existing metacache.
func (client *peerRESTClient) GetMetacacheListing(ctx context.Context, o listPathOptions) (*metacache, error) {
	if client == nil {
		resp := localMetacacheMgr.getBucket(ctx, o.Bucket).findCache(o)
		return &resp, nil
	}

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(o)
	if err != nil {
		return nil, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetMetacacheListing, nil, &reader, int64(reader.Len()))
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	var resp metacache
	defer http.DrainBody(respBody)
	return &resp, msgp.Decode(respBody, &resp)
}

// UpdateMetacacheListing - update an existing metacache it will unconditionally be updated to the new state.
func (client *peerRESTClient) UpdateMetacacheListing(ctx context.Context, m metacache) (metacache, error) {
	if client == nil {
		return localMetacacheMgr.updateCacheEntry(m)
	}
	b, err := m.MarshalMsg(nil)
	if err != nil {
		return m, err
	}
	respBody, err := client.callWithContext(ctx, peerRESTMethodUpdateMetacacheListing, nil, bytes.NewBuffer(b), int64(len(b)))
	if err != nil {
		logger.LogIf(ctx, err)
		return m, err
	}
	defer http.DrainBody(respBody)
	var resp metacache
	return resp, msgp.Decode(respBody, &resp)
}

func (client *peerRESTClient) ReloadPoolMeta(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodReloadPoolMeta, nil, nil, 0)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) StopRebalance(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodStopRebalance, nil, nil, 0)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) LoadRebalanceMeta(ctx context.Context, startRebalance bool) error {
	values := url.Values{}
	values.Set(peerRESTStartRebalance, strconv.FormatBool(startRebalance))
	respBody, err := client.callWithContext(ctx, peerRESTMethodLoadRebalanceMeta, values, nil, 0)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) LoadTransitionTierConfig(ctx context.Context) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodLoadTransitionTierConfig, nil, nil, 0)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) doTrace(traceCh chan<- madmin.TraceInfo, doneCh <-chan struct{}, traceOpts madmin.ServiceTraceOpts) {
	values := make(url.Values)
	traceOpts.AddParams(values)

	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(GlobalContext)

	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		select {
		case <-doneCh:
		case <-cancelCh:
			// There was an error in the REST request.
		}
		cancel()
	}()

	respBody, err := client.callWithContext(ctx, peerRESTMethodTrace, values, nil, -1)
	defer http.DrainBody(respBody)

	if err != nil {
		return
	}

	dec := gob.NewDecoder(respBody)
	for {
		var info madmin.TraceInfo
		if err = dec.Decode(&info); err != nil {
			return
		}
		if len(info.NodeName) > 0 {
			select {
			case traceCh <- info:
			default:
				// Do not block on slow receivers.
			}
		}
	}
}

func (client *peerRESTClient) doListen(listenCh chan<- event.Event, doneCh <-chan struct{}, v url.Values) {
	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(GlobalContext)

	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		select {
		case <-doneCh:
		case <-cancelCh:
			// There was an error in the REST request.
		}
		cancel()
	}()

	respBody, err := client.callWithContext(ctx, peerRESTMethodListen, v, nil, -1)
	defer http.DrainBody(respBody)

	if err != nil {
		return
	}

	dec := gob.NewDecoder(respBody)
	for {
		var ev event.Event
		if err := dec.Decode(&ev); err != nil {
			return
		}
		if len(ev.EventVersion) > 0 {
			select {
			case listenCh <- ev:
			default:
				// Do not block on slow receivers.
			}
		}
	}
}

// Listen - listen on peers.
func (client *peerRESTClient) Listen(listenCh chan<- event.Event, doneCh <-chan struct{}, v url.Values) {
	go func() {
		for {
			client.doListen(listenCh, doneCh, v)
			select {
			case <-doneCh:
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

// Trace - send http trace request to peer nodes
func (client *peerRESTClient) Trace(traceCh chan<- madmin.TraceInfo, doneCh <-chan struct{}, traceOpts madmin.ServiceTraceOpts) {
	go func() {
		for {
			client.doTrace(traceCh, doneCh, traceOpts)
			select {
			case <-doneCh:
				return
			default:
				// There was error in the REST request, retry after sometime as probably the peer is down.
				time.Sleep(5 * time.Second)
			}
		}
	}()
}

func (client *peerRESTClient) doConsoleLog(logCh chan log.Info, doneCh <-chan struct{}) {
	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(GlobalContext)

	cancelCh := make(chan struct{})
	defer close(cancelCh)
	go func() {
		select {
		case <-doneCh:
		case <-cancelCh:
			// There was an error in the REST request.
		}
		cancel()
	}()

	respBody, err := client.callWithContext(ctx, peerRESTMethodLog, nil, nil, -1)
	defer http.DrainBody(respBody)
	if err != nil {
		return
	}

	dec := gob.NewDecoder(respBody)
	for {
		var lg log.Info
		if err = dec.Decode(&lg); err != nil {
			break
		}
		select {
		case logCh <- lg:
		default:
			// Do not block on slow receivers.
		}
	}
}

// ConsoleLog - sends request to peer nodes to get console logs
func (client *peerRESTClient) ConsoleLog(logCh chan log.Info, doneCh <-chan struct{}) {
	go func() {
		for {
			client.doConsoleLog(logCh, doneCh)
			select {
			case <-doneCh:
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
		all[i] = newPeerRESTClient(host)
		remote = append(remote, all[i])
	}
	if len(all) != len(remote)+1 {
		logger.LogIf(context.Background(), fmt.Errorf("WARNING: Expected number of all hosts (%v) to be remote +1 (%v)", len(all), len(remote)))
	}
	return remote, all
}

// Returns a peer rest client.
func newPeerRESTClient(peer *xnet.Host) *peerRESTClient {
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
	healthClient.ExpectTimeouts = true
	healthClient.NoMetrics = true

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, peerRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}

	return &peerRESTClient{host: peer, restClient: restClient}
}

// MonitorBandwidth - send http trace request to peer nodes
func (client *peerRESTClient) MonitorBandwidth(ctx context.Context, buckets []string) (*madmin.BucketBandwidthReport, error) {
	values := make(url.Values)
	values.Set(peerRESTBuckets, strings.Join(buckets, ","))
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetBandwidth, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)

	dec := gob.NewDecoder(respBody)
	var bandwidthReport madmin.BucketBandwidthReport
	err = dec.Decode(&bandwidthReport)
	return &bandwidthReport, err
}

func (client *peerRESTClient) GetPeerMetrics(ctx context.Context) (<-chan Metric, error) {
	respBody, err := client.callWithContext(ctx, peerRESTMethodGetPeerMetrics, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(respBody)
	ch := make(chan Metric)
	go func(ch chan<- Metric) {
		for {
			var metric Metric
			if err := dec.Decode(&metric); err != nil {
				http.DrainBody(respBody)
				close(ch)
				return
			}
			ch <- metric
		}
	}(ch)
	return ch, nil
}

func (client *peerRESTClient) SpeedTest(ctx context.Context, opts speedTestOpts) (SpeedTestResult, error) {
	values := make(url.Values)
	values.Set(peerRESTSize, strconv.Itoa(opts.objectSize))
	values.Set(peerRESTConcurrent, strconv.Itoa(opts.concurrency))
	values.Set(peerRESTDuration, opts.duration.String())
	values.Set(peerRESTStorageClass, opts.storageClass)
	values.Set(peerRESTBucket, opts.bucketName)

	respBody, err := client.callWithContext(context.Background(), peerRESTMethodSpeedTest, values, nil, -1)
	if err != nil {
		return SpeedTestResult{}, err
	}
	defer http.DrainBody(respBody)
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
	defer http.DrainBody(respBody)
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

func (client *peerRESTClient) ReloadSiteReplicationConfig(ctx context.Context) error {
	respBody, err := client.callWithContext(context.Background(), peerRESTMethodReloadSiteReplicationConfig, nil, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

func (client *peerRESTClient) GetLastDayTierStats(ctx context.Context) (DailyAllTierStats, error) {
	var result map[string]lastDayTierStats
	respBody, err := client.callWithContext(context.Background(), peerRESTMethodGetLastDayTierStats, nil, nil, -1)
	if err != nil {
		return result, err
	}
	defer http.DrainBody(respBody)

	err = gob.NewDecoder(respBody).Decode(&result)
	if err != nil {
		return DailyAllTierStats{}, err
	}
	return DailyAllTierStats(result), nil
}

// DevNull - Used by netperf to pump data to peer
func (client *peerRESTClient) DevNull(ctx context.Context, r io.Reader) error {
	respBody, err := client.callWithContext(ctx, peerRESTMethodDevNull, nil, r, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
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
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&result)
	return result, err
}
