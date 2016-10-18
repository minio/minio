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
	"net/rpc"
	"path"
	"sync"
	"time"
)

type s3Peers struct {
	// A map of peer server address (in `host:port` format) to RPC
	// client connections.
	rpcClients map[string]*AuthRPCClient

	mutex *sync.RWMutex

	// Slice of all peer addresses (in `host:port` format).
	peers []string
}

func initGlobalS3Peers(disks []storageEndPoint) {
	// Get list of de-duplicated peers.
	peers := getAllPeers(disks)

	// Initialize global state.
	globalS3Peers = s3Peers{
		rpcClients: make(map[string]*AuthRPCClient),
		mutex:      &sync.RWMutex{},
	}
	// Initialize each peer connection.
	for _, peer := range peers {
		globalS3Peers.InitS3PeerClient(peer)
	}

	// Additionally setup a local peer if one does not exist.
	if globalS3Peers.GetPeerClient(globalMinioAddr) == nil {
		globalS3Peers.InitS3PeerClient(globalMinioAddr)
		peers = append(peers, globalMinioAddr)
	}

	globalS3Peers.peers = peers
}

func (s3p *s3Peers) GetPeers() []string {
	return s3p.peers
}

func (s3p *s3Peers) GetPeerClient(peer string) *AuthRPCClient {
	// Take a read lock
	s3p.mutex.RLock()
	defer s3p.mutex.RUnlock()
	return s3p.rpcClients[peer]
}

// Initializes a new RPC connection (or closes and re-opens if it
// already exists) to a peer. Note that peer address is in `host:port`
// format.
func (s3p *s3Peers) InitS3PeerClient(peer string) {
	// Take a write lock
	s3p.mutex.Lock()
	defer s3p.mutex.Unlock()

	if s3p.rpcClients[peer] != nil {
		_ = s3p.rpcClients[peer].Close()
		delete(s3p.rpcClients, peer)
	}
	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		address:     peer,
		secureConn:  isSSL(),
		path:        path.Join(reservedBucket, s3Path),
		loginMethod: "S3.LoginHandler",
	}
	s3p.rpcClients[peer] = newAuthClient(authCfg)
}

func (s3p *s3Peers) Close() error {
	// Take a write lock
	s3p.mutex.Lock()
	defer s3p.mutex.Unlock()

	for _, v := range s3p.rpcClients {
		if err := v.Close(); err != nil {
			return err
		}
	}
	s3p.rpcClients = nil
	s3p.peers = nil
	return nil
}

// returns the network addresses of all Minio servers in the cluster
// in `host:port` format.
func getAllPeers(disks []storageEndPoint) []string {
	res := []string{}
	for _, disk := range disks {
		res = append(res, fmt.Sprintf("%s:%d", disk.host, disk.port))
	}
	return res
}

// Make RPC calls with the given method and arguments to all the given
// peers (in parallel), and collects the results. Since the methods
// intended for use here, have only a success or failure response, we
// do not return/inspect the `reply` parameter in the RPC call. The
// function attempts to connect to a peer only once, and returns a map
// of peer address to error response. If the error is nil, it means
// the RPC succeeded.
func (s3p *s3Peers) SendRPC(peers []string, method string, args interface {
	SetToken(token string)
	SetTimestamp(tstamp time.Time)
}) map[string]error {

	// peer error responses array
	errArr := make([]error, len(peers))

	// Start a wait group and make RPC requests to peers.
	var wg sync.WaitGroup
	for i, target := range peers {
		wg.Add(1)
		go func(ix int, target string) {
			defer wg.Done()
			reply := &GenericReply{}
			// Get RPC client object safely.
			client := s3p.GetPeerClient(target)
			var err error
			if client == nil {
				err = fmt.Errorf("Requested client was not initialized - %v",
					target)
			} else {
				err = client.Call(method, args, reply)
				// Check for network errors and try
				// again just once.
				if err != nil {
					if err.Error() == rpc.ErrShutdown.Error() {
						err = client.Call(method, args, reply)
					}
				}
			}
			errArr[ix] = err
		}(i, target)
	}

	// Wait for requests to complete.
	wg.Wait()

	// Map of errors
	errsMap := make(map[string]error)
	for i, errVal := range errArr {
		if errVal != nil {
			errsMap[peers[i]] = errVal
		}
	}

	return errsMap
}

// S3PeersUpdateBucketNotification - Sends Update Bucket notification
// request to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketNotification(bucket string, ncfg *notificationConfig) {
	setBNPArgs := &SetBNPArgs{Bucket: bucket, NCfg: ncfg}
	peers := globalS3Peers.GetPeers()
	errsMap := globalS3Peers.SendRPC(peers, "S3.SetBucketNotificationPeer",
		setBNPArgs)
	for peer, err := range errsMap {
		errorIf(err, "Error sending peer update bucket notification to %s - %v", peer, err)
	}
}

// S3PeersUpdateBucketListener - Sends Update Bucket listeners request
// to all peers. Currently we log an error and continue.
func S3PeersUpdateBucketListener(bucket string, lcfg []listenerConfig) {
	setBLPArgs := &SetBLPArgs{Bucket: bucket, LCfg: lcfg}
	peers := globalS3Peers.GetPeers()
	errsMap := globalS3Peers.SendRPC(peers, "S3.SetBucketListenerPeer",
		setBLPArgs)
	for peer, err := range errsMap {
		errorIf(err, "Error sending peer update bucket listener to %s - %v", peer, err)
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
	setBPPArgs := &SetBPPArgs{Bucket: bucket, PChBytes: byts}
	peers := globalS3Peers.GetPeers()
	errsMap := globalS3Peers.SendRPC(peers, "S3.SetBucketPolicyPeer", setBPPArgs)
	for peer, err := range errsMap {
		errorIf(err, "Error sending peer update bucket policy to %s - %v", peer, err)
	}
}
