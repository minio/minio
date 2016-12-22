/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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
	"encoding/json"
	"fmt"
	"sync"
)

// Bucket Meta State interface to be registered in S3 Peers API
type s3PeerBucketMetaStateAPI struct{}

// Bucket Meta State API name
func (c s3PeerBucketMetaStateAPI) APIName() string {
	return "bms"
}

// Return Bucket Meta State API for local use
func (c s3PeerBucketMetaStateAPI) NewLocalClient() interface{} {
	return &localBucketMetaState{ObjectAPI: newObjectLayerFn}
}

// Return Bucket Meta State API for remote nodes
func (c s3PeerBucketMetaStateAPI) NewRemoteClient(authClient *AuthRPCClient) interface{} {
	return &remoteBucketMetaState{authClient}
}

// Fetch the bucket meta state client by peer address
func getPeerBMSClient(peerAddr string) BucketMetaState {
	client := globalS3Peers.GetPeerClientByAPI(peerAddr, "bms")
	if client == nil {
		return nil
	}
	return client.(BucketMetaState)
}

// S3PeersSendBMSUpdate sends bucket metadata updates to all given peer
// indices. The update calls are sent in parallel, and errors are
// returned per peer in an array. The returned error arrayslice is
// always as long as s3p.peers.addr.
//
// The input peerIndex slice can be nil if the update is to be sent to
// all peers. This is the common case.
//
// The updates are sent via a type implementing the BucketMetaState
// interface. This makes sure that the local node is directly updated,
// and remote nodes are updated via RPC calls.
func S3PeersSendBMSUpdate(peerIndex []int, args BucketUpdater) []error {
	// peer error array
	errs := make([]error, len(globalS3Peers))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup

	// Function that sends update to peer at `index`
	sendUpdateToPeer := func(index int) {
		defer wg.Done()
		errs[index] = args.BucketUpdate(getPeerBMSClient(globalS3Peers[index].addr))
	}

	// Special (but common) case of peerIndex == nil, implies send
	// update to all peers.
	if peerIndex == nil {
		for idx := 0; idx < len(globalS3Peers); idx++ {
			wg.Add(1)
			go sendUpdateToPeer(idx)
		}
	} else {
		// Send update only to given peer indices.
		for _, idx := range peerIndex {
			// check idx is in array bounds.
			if !(idx >= 0 && idx < len(globalS3Peers)) {
				errorIf(
					fmt.Errorf("Bad peer index %d input to SendUpdate()", idx),
					"peerIndex out of bounds",
				)
				continue
			}
			wg.Add(1)
			go sendUpdateToPeer(idx)
		}
	}

	// Wait for requests to complete and return
	wg.Wait()
	return errs
}

// S3PeersUpdateBucketNotification - Sends Update Bucket notification
// request to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketNotification(bucket string, ncfg *notificationConfig) {
	setBNPArgs := &SetBucketNotificationPeerArgs{Bucket: bucket, NCfg: ncfg}
	errs := S3PeersSendBMSUpdate(nil, setBNPArgs)
	for idx, err := range errs {
		errorIf(
			err,
			"Error sending update bucket notification to %s - %v",
			globalS3Peers[idx].addr, err,
		)
	}
}

// S3PeersUpdateBucketListener - Sends Update Bucket listeners request
// to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketListener(bucket string, lcfg []listenerConfig) {
	setBLPArgs := &SetBucketListenerPeerArgs{Bucket: bucket, LCfg: lcfg}
	errs := S3PeersSendBMSUpdate(nil, setBLPArgs)
	for idx, err := range errs {
		errorIf(
			err,
			"Error sending update bucket listener to %s - %v",
			globalS3Peers[idx].addr, err,
		)
	}
}

// S3PeersUpdateBucketPolicy - Sends update bucket policy request to
// all peers. Currently we log an error and continue.
func S3PeersUpdateBucketPolicy(bucket string, pCh policyChange) {
	byts, err := json.Marshal(pCh)
	if err != nil {
		errorIf(err, "Failed to marshal policyChange - this is a BUG!")
		return
	}
	setBPPArgs := &SetBucketPolicyPeerArgs{Bucket: bucket, PChBytes: byts}
	errs := S3PeersSendBMSUpdate(nil, setBPPArgs)
	for idx, err := range errs {
		errorIf(
			err,
			"Error sending update bucket policy to %s - %v",
			globalS3Peers[idx].addr, err,
		)
	}
}
