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
	xrpc "github.com/minio/minio/cmd/rpc"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
)

const peerServiceName = "Peer"
const peerServiceSubPath = "/s3/remote"

var peerServicePath = path.Join(minioReservedBucketPath, peerServiceSubPath)

// peerRPCReceiver - Peer RPC receiver for peer RPC server.
type peerRPCReceiver struct{}

// DeleteBucketArgs - delete bucket RPC arguments.
type DeleteBucketArgs struct {
	AuthArgs
	BucketName string
}

// DeleteBucket - handles delete bucket RPC call which removes all values of given bucket in global NotificationSys object.
func (receiver *peerRPCReceiver) DeleteBucket(args *DeleteBucketArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalNotificationSys.RemoveNotification(args.BucketName)
	globalPolicySys.Remove(args.BucketName)
	return nil
}

// SetBucketPolicyArgs - set bucket policy RPC arguments.
type SetBucketPolicyArgs struct {
	AuthArgs
	BucketName string
	Policy     policy.Policy
}

// SetBucketPolicy - handles set bucket policy RPC call which adds bucket policy to globalPolicySys.
func (receiver *peerRPCReceiver) SetBucketPolicy(args *SetBucketPolicyArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalPolicySys.Set(args.BucketName, args.Policy)
	return nil
}

// RemoveBucketPolicyArgs - delete bucket policy RPC arguments.
type RemoveBucketPolicyArgs struct {
	AuthArgs
	BucketName string
}

// RemoveBucketPolicy - handles delete bucket policy RPC call which removes bucket policy to globalPolicySys.
func (receiver *peerRPCReceiver) RemoveBucketPolicy(args *RemoveBucketPolicyArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalPolicySys.Remove(args.BucketName)
	return nil
}

// PutBucketNotificationArgs - put bucket notification RPC arguments.
type PutBucketNotificationArgs struct {
	AuthArgs
	BucketName string
	RulesMap   event.RulesMap
}

// PutBucketNotification - handles put bucket notification RPC call which adds rules to given bucket to global NotificationSys object.
func (receiver *peerRPCReceiver) PutBucketNotification(args *PutBucketNotificationArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	globalNotificationSys.AddRulesMap(args.BucketName, args.RulesMap)
	return nil
}

// ListenBucketNotificationArgs - listen bucket notification RPC arguments.
type ListenBucketNotificationArgs struct {
	AuthArgs   `json:"-"`
	BucketName string         `json:"-"`
	EventNames []event.Name   `json:"eventNames"`
	Pattern    string         `json:"pattern"`
	TargetID   event.TargetID `json:"targetId"`
	Addr       xnet.Host      `json:"addr"`
}

// ListenBucketNotification - handles listen bucket notification RPC call. It creates PeerRPCClient target which pushes requested events to target in remote peer.
func (receiver *peerRPCReceiver) ListenBucketNotification(args *ListenBucketNotificationArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	rpcClient := globalNotificationSys.GetPeerRPCClient(args.Addr)
	if rpcClient == nil {
		return fmt.Errorf("unable to find PeerRPCClient for provided address %v. This happens only if remote and this minio run with different set of endpoints", args.Addr)
	}

	target := NewPeerRPCClientTarget(args.BucketName, args.TargetID, rpcClient)
	rulesMap := event.NewRulesMap(args.EventNames, args.Pattern, target.ID())
	if err := globalNotificationSys.AddRemoteTarget(args.BucketName, target, rulesMap); err != nil {
		reqInfo := &logger.ReqInfo{BucketName: target.bucketName}
		reqInfo.AppendTags("target", target.id.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

// RemoteTargetExistArgs - remote target ID exist RPC arguments.
type RemoteTargetExistArgs struct {
	AuthArgs
	BucketName string
	TargetID   event.TargetID
}

// RemoteTargetExist - handles target ID exist RPC call which checks whether given target ID is a HTTP client target or not.
func (receiver *peerRPCReceiver) RemoteTargetExist(args *RemoteTargetExistArgs, reply *bool) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	*reply = globalNotificationSys.RemoteTargetExist(args.BucketName, args.TargetID)
	return nil
}

// SendEventArgs - send event RPC arguments.
type SendEventArgs struct {
	AuthArgs
	Event      event.Event
	TargetID   event.TargetID
	BucketName string
}

// SendEvent - handles send event RPC call which sends given event to target by given target ID.
func (receiver *peerRPCReceiver) SendEvent(args *SendEventArgs, reply *bool) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Set default to true to keep the target.
	*reply = true
	errs := globalNotificationSys.send(args.BucketName, args.Event, args.TargetID)

	for i := range errs {
		reqInfo := (&logger.ReqInfo{}).AppendTags("Event", args.Event.EventName.String())
		reqInfo.AppendTags("targetName", args.TargetID.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, errs[i].Err)

		*reply = false // send failed i.e. do not keep the target.
		return errs[i].Err
	}

	return nil
}

// ReloadFormatArgs - send event RPC arguments.
type ReloadFormatArgs struct {
	AuthArgs
	DryRun bool
}

// ReloadFormat - handles reload format RPC call, reloads latest `format.json`
func (receiver *peerRPCReceiver) ReloadFormat(args *ReloadFormatArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	return objAPI.ReloadFormat(context.Background(), args.DryRun)
}

// LoadUsers - handles load users RPC call.
func (receiver *peerRPCReceiver) LoadUsers(args *AuthArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}
	return globalIAMSys.Load(objAPI)
}

// LoadCredentials - handles load credentials RPC call.
func (receiver *peerRPCReceiver) LoadCredentials(args *AuthArgs, reply *VoidReply) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Construct path to config.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, minioConfigFile)
	transactionConfigFile := configFile + ".transaction"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid race.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetRLock(globalOperationTimeout); err != nil {
		return err
	}
	objLock.RUnlock()

	return globalConfigSys.Load(newObjectLayerFn())
}

// NewPeerRPCServer - returns new peer RPC server.
func NewPeerRPCServer() (*xrpc.Server, error) {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(peerServiceName, &peerRPCReceiver{}); err != nil {
		return nil, err
	}
	return rpcServer, nil
}

// registerPeerRPCRouter - creates and registers Peer RPC server and its router.
func registerPeerRPCRouter(router *mux.Router) {
	rpcServer, err := NewPeerRPCServer()
	logger.FatalIf(err, "Unable to initialize peer RPC Server")
	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(peerServiceSubPath).HandlerFunc(httpTraceHdrs(rpcServer.ServeHTTP))
}
