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
	"net/url"
	"strconv"
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
	connected  bool
}

// Reconnect to a peer rest server.
func (client *peerRESTClient) reConnect() error {
	// correct (intelligent) retry logic will be
	// implemented in subsequent PRs.
	client.connected = true
	return nil
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
	if !client.connected {
		err := client.reConnect()
		logger.LogIf(ctx, err)
		if err != nil {
			return nil, err
		}
	}

	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.CallWithContext(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	if isNetworkError(err) {
		client.connected = false
	}

	return nil, err
}

// Stringer provides a canonicalized representation of node.
func (client *peerRESTClient) String() string {
	return client.host.String()
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *peerRESTClient) IsOnline() bool {
	return client.connected
}

// Close - marks the client as closed.
func (client *peerRESTClient) Close() error {
	client.connected = false
	client.restClient.Close()
	return nil
}

// GetLocksResp stores various info from the client for each lock that is requested.
type GetLocksResp map[string][]lockRequesterInfo

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

// DrivePerfInfo - fetch Drive performance information for a remote node.
func (client *peerRESTClient) DrivePerfInfo() (info ServerDrivesPerfInfo, err error) {
	respBody, err := client.call(peerRESTMethodDrivePerfInfo, nil, nil, -1)
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

// SignalService - sends signal to peer nodes.
func (client *peerRESTClient) SignalService(sig serviceSignal) error {
	values := make(url.Values)
	values.Set(peerRESTSignal, string(sig))
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
		select {
		case traceCh <- info:
		default:
			// Do not block on slow receivers.
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

func getRemoteHosts(endpoints EndpointList) []*xnet.Host {
	var remoteHosts []*xnet.Host
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.FatalIf(err, "Unable to parse peer Host")
		remoteHosts = append(remoteHosts, host)
	}

	return remoteHosts
}

func getRestClients(peerHosts []*xnet.Host) ([]*peerRESTClient, error) {
	restClients := make([]*peerRESTClient, len(peerHosts))
	for i, host := range peerHosts {
		client, err := newPeerRESTClient(host)
		if err != nil {
			logger.LogIf(context.Background(), err)
		}
		restClients[i] = client
	}

	return restClients, nil
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
		return &peerRESTClient{host: peer, restClient: restClient, connected: false}, err
	}

	return &peerRESTClient{host: peer, restClient: restClient, connected: true}, nil
}
