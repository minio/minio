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
	"net/url"
	"path"
	"sync"
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
func makeS3Peers(eps []*url.URL) s3Peers {
	var ret []s3Peer

	// map to store peers that are already added to ret
	seenAddr := make(map[string]bool)

	// add local (self) as peer in the array
	ret = append(ret, s3Peer{
		globalMinioAddr,
		&localBucketMetaState{ObjectAPI: newObjectLayerFn},
	})
	seenAddr[globalMinioAddr] = true

	// iterate over endpoints to find new remote peers and add
	// them to ret.
	for _, ep := range eps {
		if ep.Host == "" {
			continue
		}

		// Check if the remote host has been added already
		if !seenAddr[ep.Host] {
			cfg := authConfig{
				accessKey:   serverConfig.GetCredential().AccessKeyID,
				secretKey:   serverConfig.GetCredential().SecretAccessKey,
				address:     ep.Host,
				secureConn:  isSSL(),
				path:        path.Join(reservedBucket, s3Path),
				loginMethod: "S3.LoginHandler",
			}

			ret = append(ret, s3Peer{
				addr:      ep.Host,
				bmsClient: &remoteBucketMetaState{newAuthClient(&cfg)},
			})
			seenAddr[ep.Host] = true
		}
	}

	return ret
}

// initGlobalS3Peers - initialize globalS3Peers by passing in
// endpoints - intended to be called early in program start-up.
func initGlobalS3Peers(eps []*url.URL) {
	globalS3Peers = makeS3Peers(eps)
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

// UpdateBucketNotification sends SetBucketNotificationPeerArgs to all S3 Peers.
// The request is sent through BucketMetaState interface. This makes sure that
// local node is directly updated, and remote nodes are updated via RPC calls.
func (s3p s3Peers) UpdateBucketNotification(args SetBucketNotificationPeerArgs) []error {
	// peer error array
	errs := make([]error, len(s3p))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup

	for i, s3peer := range s3p {
		wg.Add(1)
		go func(bms BucketMetaState, i int) {
			defer wg.Done()
			errs[i] = bms.UpdateBucketNotification(&args)
		}(s3peer.bmsClient, i)
	}

	// Wait for requests to complete and return
	wg.Wait()

	return errs
}

// UpdateBucketListener sends SetBucketListenerPeerArgs to all S3 Peers.
// The request is sent through BucketMetaState interface. This makes sure that
// local node is directly updated, and remote nodes are updated via RPC calls.
func (s3p s3Peers) UpdateBucketListener(args SetBucketListenerPeerArgs) []error {
	// peer error array
	errs := make([]error, len(s3p))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup

	for i, s3peer := range s3p {
		wg.Add(1)
		go func(bms BucketMetaState, i int) {
			defer wg.Done()
			errs[i] = bms.UpdateBucketListener(&args)
		}(s3peer.bmsClient, i)
	}

	// Wait for requests to complete and return
	wg.Wait()

	return errs
}

// UpdateBucketPolicy sends SetBucketPolicyPeerArgs to all S3 Peers.
// The request is sent through BucketMetaState interface. This makes sure that
// local node is directly updated, and remote nodes are updated via RPC calls.
func (s3p s3Peers) UpdateBucketPolicy(args SetBucketPolicyPeerArgs) []error {
	// peer error array
	errs := make([]error, len(s3p))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup

	for i, s3peer := range s3p {
		wg.Add(1)
		go func(bms BucketMetaState, i int) {
			defer wg.Done()
			errs[i] = bms.UpdateBucketPolicy(&args)
		}(s3peer.bmsClient, i)
	}

	// Wait for requests to complete and return
	wg.Wait()

	return errs
}

// S3PeersUpdateBucketNotification - Sends Update Bucket notification
// request to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketNotification(bucket string, ncfg *notificationConfig) {
	args := SetBucketNotificationPeerArgs{Bucket: bucket, NCfg: ncfg}
	errs := globalS3Peers.UpdateBucketNotification(args)
	for idx, err := range errs {
		errorIf(err,
			"Error sending update bucket notification to %s - %v",
			globalS3Peers[idx].addr, err)
	}
}

// S3PeersUpdateBucketListener - Sends Update Bucket listeners request
// to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketListener(bucket string, lcfg []listenerConfig) {
	args := SetBucketListenerPeerArgs{Bucket: bucket, LCfg: lcfg}
	errs := globalS3Peers.UpdateBucketListener(args)
	for idx, err := range errs {
		errorIf(err,
			"Error sending update bucket listener to %s - %v",
			globalS3Peers[idx].addr, err)
	}
}

// S3PeersUpdateBucketPolicy - Sends update bucket policy request to
// all peers. Currently we log an error and continue.
func S3PeersUpdateBucketPolicy(bucket string, policy policyChange) {
	byts, err := json.Marshal(policy)
	if err != nil {
		errorIf(err, "Failed to marshal policyChange - this is a BUG!")
		return
	}
	args := SetBucketPolicyPeerArgs{Bucket: bucket, PChBytes: byts}
	errs := globalS3Peers.UpdateBucketPolicy(args)
	for idx, err := range errs {
		errorIf(err,
			"Error sending update bucket policy to %s - %v",
			globalS3Peers[idx].addr, err)
	}
}
