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

import "github.com/minio/minio/pkg/event"

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
		id:             event.TargetID{ID: targetID.ID, Name: targetID.Name + "+" + mustGetUUID()},
		remoteTargetID: targetID,
		bucketName:     bucketName,
		rpcClient:      rpcClient,
	}
}
