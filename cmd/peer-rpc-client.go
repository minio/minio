/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
)

// PeerRPCClient - peer RPC client talks to peer RPC server.
type PeerRPCClient struct {
	*RPCClient
}

// DeleteBucket - calls delete bucket RPC.
func (rpcClient *PeerRPCClient) DeleteBucket(bucketName string) error {
	args := DeleteBucketArgs{BucketName: bucketName}
	reply := VoidReply{}
	return rpcClient.Call(peerServiceName+".DeleteBucket", &args, &reply)
}

// SetBucketPolicy - calls set bucket policy RPC.
func (rpcClient *PeerRPCClient) SetBucketPolicy(bucketName string, bucketPolicy *policy.Policy) error {
	args := SetBucketPolicyArgs{
		BucketName: bucketName,
		Policy:     *bucketPolicy,
	}
	reply := VoidReply{}
	return rpcClient.Call(peerServiceName+".SetBucketPolicy", &args, &reply)
}

// RemoveBucketPolicy - calls remove bucket policy RPC.
func (rpcClient *PeerRPCClient) RemoveBucketPolicy(bucketName string) error {
	args := RemoveBucketPolicyArgs{
		BucketName: bucketName,
	}
	reply := VoidReply{}
	return rpcClient.Call(peerServiceName+".RemoveBucketPolicy", &args, &reply)
}

// PutBucketNotification - calls put bukcet notification RPC.
func (rpcClient *PeerRPCClient) PutBucketNotification(bucketName string, rulesMap event.RulesMap) error {
	args := PutBucketNotificationArgs{
		BucketName: bucketName,
		RulesMap:   rulesMap,
	}
	reply := VoidReply{}
	return rpcClient.Call(peerServiceName+".PutBucketNotification", &args, &reply)
}

// ListenBucketNotification - calls listen bucket notification RPC.
func (rpcClient *PeerRPCClient) ListenBucketNotification(bucketName string, eventNames []event.Name,
	pattern string, targetID event.TargetID, addr xnet.Host) error {
	args := ListenBucketNotificationArgs{
		BucketName: bucketName,
		EventNames: eventNames,
		Pattern:    pattern,
		TargetID:   targetID,
		Addr:       addr,
	}
	reply := VoidReply{}
	return rpcClient.Call(peerServiceName+".ListenBucketNotification", &args, &reply)
}

// RemoteTargetExist - calls remote target ID exist RPC.
func (rpcClient *PeerRPCClient) RemoteTargetExist(bucketName string, targetID event.TargetID) (bool, error) {
	args := RemoteTargetExistArgs{
		BucketName: bucketName,
		TargetID:   targetID,
	}
	var reply bool

	err := rpcClient.Call(peerServiceName+".RemoteTargetExist", &args, &reply)
	return reply, err
}

// SendEvent - calls send event RPC.
func (rpcClient *PeerRPCClient) SendEvent(bucketName string, targetID, remoteTargetID event.TargetID, eventData event.Event) error {
	args := SendEventArgs{
		BucketName: bucketName,
		TargetID:   remoteTargetID,
		Event:      eventData,
	}
	var reply bool

	err := rpcClient.Call(peerServiceName+".SendEvent", &args, &reply)
	if err != nil && !reply {
		reqInfo := &logger.ReqInfo{BucketName: bucketName}
		reqInfo.AppendTags("targetID", targetID.Name)
		reqInfo.AppendTags("event", eventData.EventName.String())
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		globalNotificationSys.RemoveRemoteTarget(bucketName, targetID)
	}

	return err
}

// ReloadFormat - calls reload format RPC.
func (rpcClient *PeerRPCClient) ReloadFormat(dryRun bool) error {
	args := ReloadFormatArgs{
		DryRun: dryRun,
	}
	reply := VoidReply{}

	return rpcClient.Call(peerServiceName+".ReloadFormat", &args, &reply)
}

// LoadUsers - calls load users RPC.
func (rpcClient *PeerRPCClient) LoadUsers() error {
	args := AuthArgs{}
	reply := VoidReply{}

	return rpcClient.Call(peerServiceName+".LoadUsers", &args, &reply)
}

// LoadCredentials - calls load credentials RPC.
func (rpcClient *PeerRPCClient) LoadCredentials() error {
	args := AuthArgs{}
	reply := VoidReply{}

	return rpcClient.Call(peerServiceName+".LoadCredentials", &args, &reply)
}

// NewPeerRPCClient - returns new peer RPC client.
func NewPeerRPCClient(host *xnet.Host) (*PeerRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   peerServicePath,
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
			ServiceName:      peerServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &PeerRPCClient{rpcClient}, nil
}

// makeRemoteRPCClients - creates Peer RPCClients for given endpoint list.
func makeRemoteRPCClients(endpoints EndpointList) map[xnet.Host]*PeerRPCClient {
	peerRPCClientMap := make(map[xnet.Host]*PeerRPCClient)
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.FatalIf(err, "Unable to parse peer RPC Host")
		rpcClient, err := NewPeerRPCClient(host)
		logger.FatalIf(err, "Unable to parse peer RPC Client")
		peerRPCClientMap[*host] = rpcClient
	}

	return peerRPCClientMap
}
