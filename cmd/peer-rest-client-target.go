/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

// PeerRESTClientTarget - RPCClient is an event.Target which sends event to target of remote peer.
type PeerRESTClientTarget struct {
	id             event.TargetID
	remoteTargetID event.TargetID
	restClient     *peerRESTClient
	bucketName     string
}

// ID - returns target ID.
func (target *PeerRESTClientTarget) ID() event.TargetID {
	return target.id
}

// Save - Sends event directly without persisting.
func (target *PeerRESTClientTarget) Save(eventData event.Event) error {
	return target.send(eventData)
}

// Send - interface compatible method does no-op.
func (target *PeerRESTClientTarget) Send(eventKey string) error {
	return nil
}

// sends event to remote peer by making RPC call.
func (target *PeerRESTClientTarget) send(eventData event.Event) error {
	return target.restClient.SendEvent(target.bucketName, target.id, target.remoteTargetID, eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *PeerRESTClientTarget) Close() error {
	return nil
}

// NewPeerRESTClientTarget - creates RPCClient target with given target ID available in remote peer.
func NewPeerRESTClientTarget(bucketName string, targetID event.TargetID, restClient *peerRESTClient) *PeerRESTClientTarget {
	return &PeerRESTClientTarget{
		id:             event.TargetID{ID: targetID.ID, Name: targetID.Name + "+" + mustGetUUID()},
		remoteTargetID: targetID,
		bucketName:     bucketName,
		restClient:     restClient,
	}
}
