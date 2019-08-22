/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"crypto/tls"
	"encoding/gob"
	"io"
	"math/rand"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/lifecycle"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
	trace "github.com/minio/minio/pkg/trace"
)

// client to talk to peer Nodes.
type peerRESTClient struct {
	host       *xnet.Host
	restClient *rest.Client
	connected  int32
}

// Reconnect to a peer rest server.
func (client *peerRESTClient) reConnect() {
	atomic.StoreInt32(&client.connected, 1)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(context.Background(), method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerRESTClient) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if !client.IsOnline() {
		client.reConnect()
	}

	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.CallWithContext(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	if isNetworkError(err) {
		atomic.StoreInt32(&client.connected, 0)
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *peerRESTClient) IsOnline() bool {
	return atomic.LoadInt32(&client.connected) == 1
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	atomic.StoreInt32(&client.connected, 0)
	client.restClient.Close()
	return nil
}

// GetLocksResp stores various info from the client for each lock that is requested.
type GetLocksResp map[string][]lockRequesterInfo

// NetReadPerfInfo - fetch network read performance information for a remote node.
func (client *peerRESTClient) NetReadPerfInfo(size int64) (info ServerNetReadPerfInfo, err error) {
	params := make(url.Values)
	params.Set(peerRESTNetPerfSize, strconv.FormatInt(size, 10))
	respBody, err := client.call(
		peerRESTMethodNetReadPerfInfo,
		params,
		rand.New(rand.NewSource(time.Now().UnixNano())),
		size,
	)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// CollectNetPerfInfo - collect network performance information of other peers.
func (client *peerRESTClient) CollectNetPerfInfo(size int64) (info []ServerNetReadPerfInfo, err error) {
	params := make(url.Values)
	params.Set(peerRESTNetPerfSize, strconv.FormatInt(size, 10))
	respBody, err := client.call(peerRESTMethodCollectNetPerfInfo, params, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// GetLocks - fetch older locks for a remote node.
func (client *peerRESTClient) GetLocks() (locks GetLocksResp, err error) {
	respBody, err := client.call(peerRESTMethodGetLocks, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&locks)
	return locks, err
}

// ServerInfo - fetch server information for a remote node.
func (client *peerRESTClient) ServerInfo() (info ServerInfoData, err error) {
	respBody, err := client.call(peerRESTMethodServerInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// CPULoadInfo - fetch CPU information for a remote node.
func (client *peerRESTClient) CPULoadInfo() (info ServerCPULoadInfo, err error) {
	respBody, err := client.call(peerRESTMethodCPULoadInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// CPUInfo - fetch CPU hardware information for a remote node.
func (client *peerRESTClient) CPUInfo() (info madmin.ServerCPUHardwareInfo, err error) {
	respBody, err := client.call(peerRESTMethodHardwareCPUInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// NetworkInfo - fetch network hardware information for a remote node.
func (client *peerRESTClient) NetworkInfo() (info madmin.ServerNetworkHardwareInfo, err error) {
	respBody, err := client.call(peerRESTMethodHardwareNetworkInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// DrivePerfInfo - fetch Drive performance information for a remote node.
func (client *peerRESTClient) DrivePerfInfo(size int64) (info madmin.ServerDrivesPerfInfo, err error) {
	params := make(url.Values)
	params.Set(peerRESTDrivePerfSize, strconv.FormatInt(size, 10))
	respBody, err := client.call(peerRESTMethodDrivePerfInfo, params, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// MemUsageInfo - fetch memory usage information for a remote node.
func (client *peerRESTClient) MemUsageInfo() (info ServerMemUsageInfo, err error) {
	respBody, err := client.call(peerRESTMethodMemUsageInfo, nil, nil, -1)
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
func (client *peerRESTClient) DownloadProfileData() (data []byte, err error) {
	respBody, err := client.call(peerRESTMethodDownloadProfilingData, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&data)
	return data, err
}

// DeleteBucket - Delete notification and policies related to the bucket.
func (client *peerRESTClient) DeleteBucket(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodDeleteBucket, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// ReloadFormat - reload format on the peer node.
func (client *peerRESTClient) ReloadFormat(dryRun bool) error {
	values := make(url.Values)
	if dryRun {
		values.Set(peerRESTDryRun, "true")
	} else {
		values.Set(peerRESTDryRun, "false")
	}

	respBody, err := client.call(peerRESTMethodReloadFormat, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// ListenBucketNotification - send listen bucket notification to peer nodes.
func (client *peerRESTClient) ListenBucketNotification(bucket string, eventNames []event.Name,
	pattern string, targetID event.TargetID, addr xnet.Host) error {
	args := listenBucketNotificationReq{
		EventNames: eventNames,
		Pattern:    pattern,
		TargetID:   targetID,
		Addr:       addr,
	}

	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(args)
	if err != nil {
		return err
	}

	respBody, err := client.call(peerRESTMethodBucketNotificationListen, values, &reader, -1)
	if err != nil {
		return err
	}

	defer http.DrainBody(respBody)
	return nil
}

// SendEvent - calls send event RPC.
func (client *peerRESTClient) SendEvent(bucket string, targetID, remoteTargetID event.TargetID, eventData event.Event) error {
	args := sendEventRequest{
		TargetID: remoteTargetID,
		Event:    eventData,
	}

	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(args)
	if err != nil {
		return err
	}
	respBody, err := client.call(peerRESTMethodSendEvent, values, &reader, -1)
	if err != nil {
		return err
	}

	var eventResp sendEventResp
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&eventResp)

	if err != nil || !eventResp.Success {
		reqInfo := &logger.ReqInfo{BucketName: bucket}
		reqInfo.AppendTags("targetID", targetID.Name)
		reqInfo.AppendTags("event", eventData.EventName.String())
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		globalNotificationSys.RemoveRemoteTarget(bucket, targetID)
	}

	return err
}

// RemoteTargetExist - calls remote target ID exist REST API.
func (client *peerRESTClient) RemoteTargetExist(bucket string, targetID event.TargetID) (bool, error) {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(targetID)
	if err != nil {
		return false, err
	}

	respBody, err := client.call(peerRESTMethodTargetExists, values, &reader, -1)
	if err != nil {
		return false, err
	}
	defer http.DrainBody(respBody)
	var targetExists remoteTargetExistsResp
	err = gob.NewDecoder(respBody).Decode(&targetExists)
	return targetExists.Exists, err
}

// RemoveBucketPolicy - Remove bucket policy on the peer node.
func (client *peerRESTClient) RemoveBucketPolicy(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodBucketPolicyRemove, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// SetBucketPolicy - Set bucket policy on the peer node.
func (client *peerRESTClient) SetBucketPolicy(bucket string, bucketPolicy *policy.Policy) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(bucketPolicy)
	if err != nil {
		return err
	}

	respBody, err := client.call(peerRESTMethodBucketPolicySet, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// RemoveBucketLifecycle - Remove bucket lifecycle configuration on the peer node
func (client *peerRESTClient) RemoveBucketLifecycle(bucket string) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)
	respBody, err := client.call(peerRESTMethodBucketLifecycleRemove, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// SetBucketLifecycle - Set bucket lifecycle configuration on the peer node
func (client *peerRESTClient) SetBucketLifecycle(bucket string, bucketLifecycle *lifecycle.Lifecycle) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(bucketLifecycle)
	if err != nil {
		return err
	}

	respBody, err := client.call(peerRESTMethodBucketLifecycleSet, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// PutBucketNotification - Put bucket notification on the peer node.
func (client *peerRESTClient) PutBucketNotification(bucket string, rulesMap event.RulesMap) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(&rulesMap)
	if err != nil {
		return err
	}

	respBody, err := client.call(peerRESTMethodBucketNotificationPut, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// PutBucketObjectLockConfig - PUT bucket object lock configuration.
func (client *peerRESTClient) PutBucketObjectLockConfig(bucket string, retention Retention) error {
	values := make(url.Values)
	values.Set(peerRESTBucket, bucket)

	var reader bytes.Buffer
	err := gob.NewEncoder(&reader).Encode(&retention)
	if err != nil {
		return err
	}

	respBody, err := client.call(peerRESTMethodPutBucketObjectLockConfig, values, &reader, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
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
func (client *peerRESTClient) LoadPolicyMapping(userOrGroup string, isGroup bool) error {
	values := make(url.Values)
	values.Set(peerRESTUserOrGroup, userOrGroup)
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

// LoadUsers - send load users command to peer nodes.
func (client *peerRESTClient) LoadUsers() (err error) {
	respBody, err := client.call(peerRESTMethodLoadUsers, nil, nil, -1)
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

// ServerUpdate - sends server update message to remote peers.
func (client *peerRESTClient) ServerUpdate(updateURL, sha256Hex string, latestReleaseTime time.Time) error {
	values := make(url.Values)
	values.Set(peerRESTUpdateURL, updateURL)
	values.Set(peerRESTSha256Hex, sha256Hex)
	if !latestReleaseTime.IsZero() {
		values.Set(peerRESTLatestRelease, latestReleaseTime.Format(time.RFC3339))
	} else {
		values.Set(peerRESTLatestRelease, "")
	}
	respBody, err := client.call(peerRESTMethodServerUpdate, values, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	return nil
}

// SignalService - sends signal to peer nodes.
func (client *peerRESTClient) SignalService(sig serviceSignal) error {
	values := make(url.Values)
	values.Set(peerRESTSignal, strconv.Itoa(int(sig)))
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

// BgLifecycleOpsStatus describes the status
// of the background lifecycle operations
type BgLifecycleOpsStatus struct {
	LastActivity time.Time
}

// BgOpsStatus describes the status of all operations performed
// in background such as auto-healing and lifecycle.
// Notice: We need to increase peer REST API version when adding
// new fields to this struct.
type BgOpsStatus struct {
	LifecycleOps BgLifecycleOpsStatus
}

func (client *peerRESTClient) BackgroundOpsStatus() (BgOpsStatus, error) {
	respBody, err := client.call(peerRESTMethodBackgroundOpsStatus, nil, nil, -1)
	if err != nil {
		return BgOpsStatus{}, err
	}
	defer http.DrainBody(respBody)

	state := BgOpsStatus{}
	err = gob.NewDecoder(respBody).Decode(&state)
	return state, err
}

func (client *peerRESTClient) doTrace(traceCh chan interface{}, doneCh chan struct{}, trcAll, trcErr bool) {
	values := make(url.Values)
	values.Set(peerRESTTraceAll, strconv.FormatBool(trcAll))
	values.Set(peerRESTTraceErr, strconv.FormatBool(trcErr))

	// To cancel the REST request in case doneCh gets closed.
	ctx, cancel := context.WithCancel(context.Background())

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
		var info trace.Info
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

// Trace - send http trace request to peer nodes
func (client *peerRESTClient) Trace(traceCh chan interface{}, doneCh chan struct{}, trcAll, trcErr bool) {
	go func() {
		for {
			client.doTrace(traceCh, doneCh, trcAll, trcErr)
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

// ConsoleLog - sends request to peer nodes to get console logs
func (client *peerRESTClient) ConsoleLog(logCh chan interface{}, doneCh chan struct{}) {
	go func() {
		for {
			// get cancellation context to properly unsubscribe peers
			ctx, cancel := context.WithCancel(context.Background())
			respBody, err := client.callWithContext(ctx, peerRESTMethodLog, nil, nil, -1)
			if err != nil {
				// Retry the failed request.
				time.Sleep(5 * time.Second)
			} else {
				dec := gob.NewDecoder(respBody)

				go func() {
					<-doneCh
					cancel()
				}()

				for {
					var log madmin.LogInfo
					if err = dec.Decode(&log); err != nil {
						break
					}
					select {
					case logCh <- log:
					default:
					}
				}
			}

			select {
			case <-doneCh:
				cancel()
				http.DrainBody(respBody)
				return
			default:
				// There was error in the REST request, retry.
			}
		}
	}()
}

func getRemoteHosts(endpoints EndpointList) []*xnet.Host {
	var remoteHosts []*xnet.Host
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		remoteHosts = append(remoteHosts, host)
	}

	return remoteHosts
}

func getRestClients(endpoints EndpointList) []*peerRESTClient {
	peerHosts := getRemoteHosts(endpoints)
	restClients := make([]*peerRESTClient, len(peerHosts))
	for i, host := range peerHosts {
		client, err := newPeerRESTClient(host)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		restClients[i] = client
	}

	return restClients
}

// Returns a peer rest client.
func newPeerRESTClient(peer *xnet.Host) (*peerRESTClient, error) {

	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   peerRESTPath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: peer.Name,
			RootCAs:    globalRootCAs,
		}
	}

	restClient, err := rest.NewClient(serverURL, tlsConfig, rest.DefaultRESTTimeout, newAuthToken)

	if err != nil {
		return &peerRESTClient{host: peer, restClient: restClient, connected: 0}, err
	}

	return &peerRESTClient{host: peer, restClient: restClient, connected: 1}, nil
}
