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
	"fmt"
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
)

const s3Path = "/s3/remote"

// PeerRPCReceiver - Peer RPC receiver for peer RPC server.
type PeerRPCReceiver struct {
	AuthRPCServer
}

// DeleteBucketArgs - delete bucket RPC arguments.
type DeleteBucketArgs struct {
	AuthRPCArgs
	BucketName string
}

// DeleteBucket - handles delete bucket RPC call which removes all values of given bucket in global NotificationSys object.
func (receiver *PeerRPCReceiver) DeleteBucket(args *DeleteBucketArgs, reply *AuthRPCArgs) error {
	globalNotificationSys.RemoveNotification(args.BucketName)
	globalPolicySys.Remove(args.BucketName)
	return nil
}

// SetBucketPolicyArgs - set bucket policy RPC arguments.
type SetBucketPolicyArgs struct {
	AuthRPCArgs
	BucketName string
	Policy     policy.Policy
}

// SetBucketPolicy - handles set bucket policy RPC call which adds bucket policy to globalPolicySys.
func (receiver *PeerRPCReceiver) SetBucketPolicy(args *SetBucketPolicyArgs, reply *AuthRPCArgs) error {
	globalPolicySys.Set(args.BucketName, args.Policy)
	return nil
}

// RemoveBucketPolicyArgs - delete bucket policy RPC arguments.
type RemoveBucketPolicyArgs struct {
	AuthRPCArgs
	BucketName string
}

// RemoveBucketPolicy - handles delete bucket policy RPC call which removes bucket policy to globalPolicySys.
func (receiver *PeerRPCReceiver) RemoveBucketPolicy(args *RemoveBucketPolicyArgs, reply *AuthRPCArgs) error {
	globalPolicySys.Remove(args.BucketName)
	return nil
}

// PutBucketNotificationArgs - put bucket notification RPC arguments.
type PutBucketNotificationArgs struct {
	AuthRPCArgs
	BucketName string
	RulesMap   event.RulesMap
}

// PutBucketNotification - handles put bucket notification RPC call which adds rules to given bucket to global NotificationSys object.
func (receiver *PeerRPCReceiver) PutBucketNotification(args *PutBucketNotificationArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	globalNotificationSys.AddRulesMap(args.BucketName, args.RulesMap)
	return nil
}

// ListenBucketNotificationArgs - listen bucket notification RPC arguments.
type ListenBucketNotificationArgs struct {
	AuthRPCArgs `json:"-"`
	BucketName  string         `json:"-"`
	EventNames  []event.Name   `json:"eventNames"`
	Pattern     string         `json:"pattern"`
	TargetID    event.TargetID `json:"targetId"`
	Addr        xnet.Host      `json:"addr"`
}

// ListenBucketNotification - handles listen bucket notification RPC call. It creates PeerRPCClient target which pushes requested events to target in remote peer.
func (receiver *PeerRPCReceiver) ListenBucketNotification(args *ListenBucketNotificationArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	rpcClient := globalNotificationSys.GetPeerRPCClient(args.Addr)
	if rpcClient == nil {
		return fmt.Errorf("unable to find PeerRPCClient for provided address %v. This happens only if remote and this minio run with different set of endpoints", args.Addr)
	}

	target := NewPeerRPCClientTarget(args.BucketName, args.TargetID, rpcClient)
	rulesMap := event.NewRulesMap(args.EventNames, args.Pattern, target.ID())
	if err := globalNotificationSys.AddRemoteTarget(args.BucketName, target, rulesMap); err != nil {
		reqInfo := &logger.ReqInfo{BucketName: target.bucketName}
		reqInfo.AppendTags("targetName", target.id.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// RemoteTargetExistArgs - remote target ID exist RPC arguments.
type RemoteTargetExistArgs struct {
	AuthRPCArgs
	BucketName string
	TargetID   event.TargetID
}

// RemoteTargetExistReply - remote target ID exist RPC reply.
type RemoteTargetExistReply struct {
	AuthRPCReply
	Exist bool
}

// RemoteTargetExist - handles target ID exist RPC call which checks whether given target ID is a HTTP client target or not.
func (receiver *PeerRPCReceiver) RemoteTargetExist(args *RemoteTargetExistArgs, reply *RemoteTargetExistReply) error {
	reply.Exist = globalNotificationSys.RemoteTargetExist(args.BucketName, args.TargetID)
	return nil
}

// SendEventArgs - send event RPC arguments.
type SendEventArgs struct {
	AuthRPCArgs
	Event      event.Event
	TargetID   event.TargetID
	BucketName string
}

// SendEventReply - send event RPC reply.
type SendEventReply struct {
	AuthRPCReply
	Error error
}

// SendEvent - handles send event RPC call which sends given event to target by given target ID.
func (receiver *PeerRPCReceiver) SendEvent(args *SendEventArgs, reply *SendEventReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	var err error
	for _, terr := range globalNotificationSys.send(args.BucketName, args.Event, args.TargetID) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("Event", args.Event.EventName.String())
		reqInfo.AppendTags("targetName", args.TargetID.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, terr.Err)
		err = terr.Err
	}

	reply.Error = err
	return nil
}

// registerS3PeerRPCRouter - creates and registers Peer RPC server and its router.
func registerS3PeerRPCRouter(router *mux.Router) error {
	peerRPCServer := newRPCServer()
	if err := peerRPCServer.RegisterName("Peer", &PeerRPCReceiver{}); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}

	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(s3Path).Handler(peerRPCServer)
	return nil
}

// PeerRPCClient - peer RPC client talks to peer RPC server.
type PeerRPCClient struct {
	*AuthRPCClient
}

// DeleteBucket - calls delete bucket RPC.
func (rpcClient *PeerRPCClient) DeleteBucket(bucketName string) error {
	args := DeleteBucketArgs{BucketName: bucketName}
	reply := AuthRPCReply{}
	return rpcClient.Call("Peer.DeleteBucket", &args, &reply)
}

// SetBucketPolicy - calls set bucket policy RPC.
func (rpcClient *PeerRPCClient) SetBucketPolicy(bucketName string, bucketPolicy *policy.Policy) error {
	args := SetBucketPolicyArgs{
		BucketName: bucketName,
		Policy:     *bucketPolicy,
	}
	reply := AuthRPCReply{}
	return rpcClient.Call("Peer.SetBucketPolicy", &args, &reply)
}

// RemoveBucketPolicy - calls remove bucket policy RPC.
func (rpcClient *PeerRPCClient) RemoveBucketPolicy(bucketName string) error {
	args := RemoveBucketPolicyArgs{
		BucketName: bucketName,
	}
	reply := AuthRPCReply{}
	return rpcClient.Call("Peer.RemoveBucketPolicy", &args, &reply)
}

// PutBucketNotification - calls put bukcet notification RPC.
func (rpcClient *PeerRPCClient) PutBucketNotification(bucketName string, rulesMap event.RulesMap) error {
	args := PutBucketNotificationArgs{
		BucketName: bucketName,
		RulesMap:   rulesMap,
	}
	reply := AuthRPCReply{}
	return rpcClient.Call("Peer.PutBucketNotification", &args, &reply)
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
	reply := AuthRPCReply{}
	return rpcClient.Call("Peer.ListenBucketNotification", &args, &reply)
}

// RemoteTargetExist - calls remote target ID exist RPC.
func (rpcClient *PeerRPCClient) RemoteTargetExist(bucketName string, targetID event.TargetID) (bool, error) {
	args := RemoteTargetExistArgs{
		BucketName: bucketName,
		TargetID:   targetID,
	}

	reply := RemoteTargetExistReply{}
	if err := rpcClient.Call("Peer.RemoteTargetExist", &args, &reply); err != nil {
		return false, err
	}

	return reply.Exist, nil
}

// SendEvent - calls send event RPC.
func (rpcClient *PeerRPCClient) SendEvent(bucketName string, targetID, remoteTargetID event.TargetID, eventData event.Event) error {
	args := SendEventArgs{
		BucketName: bucketName,
		TargetID:   remoteTargetID,
		Event:      eventData,
	}
	reply := SendEventReply{}
	if err := rpcClient.Call("Peer.SendEvent", &args, &reply); err != nil {
		return err
	}

	if reply.Error != nil {
		reqInfo := &logger.ReqInfo{BucketName: bucketName}
		reqInfo.AppendTags("targetID", targetID.Name)
		reqInfo.AppendTags("event", eventData.EventName.String())
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, reply.Error)
		globalNotificationSys.RemoveRemoteTarget(bucketName, targetID)
	}

	return reply.Error
}

// makeRemoteRPCClients - creates Peer RPCClients for given endpoint list.
func makeRemoteRPCClients(endpoints EndpointList) map[xnet.Host]*PeerRPCClient {
	peerRPCClientMap := make(map[xnet.Host]*PeerRPCClient)

	cred := globalServerConfig.GetCredential()
	serviceEndpoint := path.Join(minioReservedBucketPath, s3Path)
	for _, hostStr := range GetRemotePeers(endpoints) {
		host, err := xnet.ParseHost(hostStr)
		logger.CriticalIf(context.Background(), err)
		peerRPCClientMap[*host] = &PeerRPCClient{newAuthRPCClient(authConfig{
			accessKey:       cred.AccessKey,
			secretKey:       cred.SecretKey,
			serverAddr:      hostStr,
			serviceEndpoint: serviceEndpoint,
			secureConn:      globalIsSSL,
			serviceName:     "Peer",
		})}
	}

	return peerRPCClientMap
}

// PeerRPCClientTarget - RPCClient is an event.Target which sends event to target of remote peer.
type PeerRPCClientTarget struct {
	id             event.TargetID
	remoteTargetID event.TargetID
	rpcClient      *PeerRPCClient
	bucketName     string
}

// ID - returns target ID.
func (target *PeerRPCClientTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to remote peer by making RPC call.
func (target *PeerRPCClientTarget) Send(eventData event.Event) error {
	return target.rpcClient.SendEvent(target.bucketName, target.id, target.remoteTargetID, eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *PeerRPCClientTarget) Close() error {
	return nil
}

// NewPeerRPCClientTarget - creates RPCClient target with given target ID available in remote peer.
func NewPeerRPCClientTarget(bucketName string, targetID event.TargetID, rpcClient *PeerRPCClient) *PeerRPCClientTarget {
	return &PeerRPCClientTarget{
		id:             event.TargetID{targetID.ID, targetID.Name + "+" + mustGetUUID()},
		remoteTargetID: targetID,
		bucketName:     bucketName,
		rpcClient:      rpcClient,
	}
}
