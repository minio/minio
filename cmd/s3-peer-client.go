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
	"net/url"
	"path"
)

// s3Peer structs contains the address of a peer in the cluster, and
// its BucketMetaState interface objects.
type s3Peer struct {
	// address in `host:port` format
	addr string
	// Generic list of registered APIs
	APIs map[string]interface{}
}

// type representing all peers in the cluster
type s3Peers []s3Peer

// interface to satisfy to implement new S3 Peer API
type s3PeersAPI interface {
	// Return Client API for local use
	NewLocalClient() interface{}
	// Return Client API to use with a remote node
	NewRemoteClient(*AuthRPCClient) interface{}
	// Return the API Name
	APIName() string
}

// The list of registered APIs to use for local and remote nodes
var registeredPeersAPI = []s3PeersAPI{
	&s3PeerBucketMetaStateAPI{},
}

// makeS3Peers makes an s3Peers struct value from the given urls
// slice. The urls slice is assumed to be non-empty and free of nil
// values.
func makeS3Peers(eps []*url.URL) s3Peers {
	var ret []s3Peer

	// map to store peers that are already added to ret
	seenAddr := make(map[string]bool)

	// add local (self) as peer in the array
	localPeer := s3Peer{addr: globalMinioAddr, APIs: make(map[string]interface{})}
	for _, api := range registeredPeersAPI {
		localPeer.APIs[api.APIName()] = api.NewLocalClient()
	}
	ret = append(ret, localPeer)
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
				accessKey:   serverConfig.GetCredential().AccessKey,
				secretKey:   serverConfig.GetCredential().SecretKey,
				address:     ep.Host,
				secureConn:  isSSL(),
				path:        path.Join(reservedBucket, s3Path),
				loginMethod: "S3.LoginHandler",
			}
			remotePeer := s3Peer{addr: ep.Host, APIs: make(map[string]interface{})}
			for _, api := range registeredPeersAPI {
				remotePeer.APIs[api.APIName()] = api.NewRemoteClient(newAuthClient(&cfg))
			}
			ret = append(ret, remotePeer)
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

// GetPeerClientByAPI - fetch client interface by peer address
func (s3p s3Peers) GetPeerClientByAPI(peer string, api string) interface{} {
	for _, p := range s3p {
		if p.addr == peer {
			client, ok := p.APIs[api]
			if ok {
				return client
			}
		}
	}
	return nil
}
