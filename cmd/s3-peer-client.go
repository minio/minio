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
	"path"
	"sync"

	"github.com/minio/minio-go/pkg/set"
)

// s3Peer structs contains the address of a peer in the cluster, and
// its BucketMetaState interface objects.
type s3Peer struct {
	// address in `host:port` format
	addr string
	// BucketMetaState client interface
	bmsClient BucketMetaState
}

// type representing all peers in the cluster
type s3Peers []s3Peer

// makeS3Peers makes an s3Peers struct value from the given urls
// slice. The urls slice is assumed to be non-empty and free of nil
// values.
func makeS3Peers(endpoints EndpointList) (s3PeerList s3Peers) {
	localAddr := GetLocalPeer(endpoints)
	s3PeerList = append(s3PeerList, s3Peer{
		localAddr,
		&localBucketMetaState{ObjectAPI: newObjectLayerFn},
	})

	hostSet := set.CreateStringSet(localAddr)
	cred := globalServerConfig.GetCredential()
	serviceEndpoint := path.Join(minioReservedBucketPath, s3Path)
	for _, host := range GetRemotePeers(endpoints) {
		if hostSet.Contains(host) {
			continue
		}
		hostSet.Add(host)
		s3PeerList = append(s3PeerList, s3Peer{
			addr: host,
			bmsClient: &remoteBucketMetaState{
				newAuthRPCClient(authConfig{
					accessKey:       cred.AccessKey,
					secretKey:       cred.SecretKey,
					serverAddr:      host,
					serviceEndpoint: serviceEndpoint,
					secureConn:      globalIsSSL,
					serviceName:     "S3",
				})},
		})
	}
	return s3PeerList
}

// initGlobalS3Peers - initialize globalS3Peers by passing in
// endpoints - intended to be called early in program start-up.
func initGlobalS3Peers(endpoints EndpointList) {
	globalS3Peers = makeS3Peers(endpoints)
}

// GetPeerClient - fetch BucketMetaState interface by peer address
func (s3p s3Peers) GetPeerClient(peer string) BucketMetaState {
	for _, p := range s3p {
		if p.addr == peer {
			return p.bmsClient
		}
	}
	return nil
}

// SendUpdate sends bucket metadata updates to all given peer
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
func (s3p s3Peers) SendUpdate(peerIndex []int, args BucketUpdater) []error {

	// peer error array
	errs := make([]error, len(s3p))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup

	// Function that sends update to peer at `index`
	sendUpdateToPeer := func(index int) {
		defer wg.Done()
		errs[index] = args.BucketUpdate(s3p[index].bmsClient)
	}

	// Special (but common) case of peerIndex == nil, implies send
	// update to all peers.
	if peerIndex == nil {
		for idx := 0; idx < len(s3p); idx++ {
			wg.Add(1)
			go sendUpdateToPeer(idx)
		}
	} else {
		// Send update only to given peer indices.
		for _, idx := range peerIndex {
			// check idx is in array bounds.
			if !(idx >= 0 && idx < len(s3p)) {
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
	errs := globalS3Peers.SendUpdate(nil, setBNPArgs)
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
	errs := globalS3Peers.SendUpdate(nil, setBLPArgs)
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
	errs := globalS3Peers.SendUpdate(nil, setBPPArgs)
	for idx, err := range errs {
		errorIf(
			err,
			"Error sending update bucket policy to %s - %v",
			globalS3Peers[idx].addr, err,
		)
	}
}
