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
	"fmt"
	"path"
	"time"

	"github.com/minio/minio-go/pkg/set"
)

type s3Peers struct {
	// A map of peer server address (in `host:port` format) to RPC
	// client connections
	rpcClient map[string]*AuthRPCClient

	// slice of all peer addresses (in `host:port` format)
	peers []string
}

func initGlobalS3Peers(disks []string) {
	// get list of de-duplicated peers
	peers := getAllPeers(disks)
	globalS3Peers = s3Peers{make(map[string]*AuthRPCClient), nil}
	for _, peer := range peers {
		globalS3Peers.InitS3PeerClient(peer)
	}

	// Additionally setup a local peer if one does not exist
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
	return s3p.rpcClient[peer]
}

// Initializes a new RPC connection (or closes and re-opens if it
// already exists) to a peer. Note that peer address is in `host:port`
// format.
func (s3p *s3Peers) InitS3PeerClient(peer string) {
	if s3p.rpcClient[peer] != nil {
		s3p.rpcClient[peer].Close()
		delete(s3p.rpcClient, peer)
	}
	authCfg := &authConfig{
		accessKey:   serverConfig.GetCredential().AccessKeyID,
		secretKey:   serverConfig.GetCredential().SecretAccessKey,
		address:     peer,
		path:        path.Join(reservedBucket, s3Path),
		loginMethod: "S3.LoginHandler",
	}
	s3p.rpcClient[peer] = newAuthClient(authCfg)
}

func (s3p *s3Peers) Close() error {
	for _, v := range s3p.rpcClient {
		if err := v.Close(); err != nil {
			return err
		}
	}
	s3p.rpcClient = nil
	s3p.peers = nil
	return nil
}

// returns the network addresses of all Minio servers in the cluster
// in `host:port` format.
func getAllPeers(disks []string) []string {
	res := []string{}
	// use set to de-duplicate
	sset := set.NewStringSet()
	for _, disk := range disks {
		netAddr, _, err := splitNetPath(disk)
		if err != nil || netAddr == "" {
			errorIf(err, "Unexpected error - most likely a bug.")
			continue
		}
		if !sset.Contains(netAddr) {
			res = append(
				res,
				fmt.Sprintf("%s:%d", netAddr, globalMinioPort),
			)
			sset.Add(netAddr)
		}
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
	// result type
	type callResult struct {
		target string
		err    error
	}
	// channel to collect results from goroutines
	resChan := make(chan callResult)
	// closure to make a single request.
	callTarget := func(target string) {
		reply := &GenericReply{}
		err := s3p.rpcClient[target].Call(method, args, reply)
		resChan <- callResult{target, err}
	}
	// map of errors
	errsMap := make(map[string]error)
	// make network calls in parallel
	for _, target := range peers {
		go callTarget(target)
	}
	// wait on channel and collect all results
	for range peers {
		res := <-resChan
		if res.err != nil {
			errsMap[res.target] = res.err
		}
	}
	// return errors map
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
