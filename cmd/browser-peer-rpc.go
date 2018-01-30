/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"sync"
	"time"

	"github.com/minio/minio/pkg/auth"
)

// SetAuthPeerArgs - Arguments collection for SetAuth RPC call
type SetAuthPeerArgs struct {
	// For Auth
	AuthRPCArgs

	// New credentials that receiving peer should update to.
	Creds auth.Credentials
}

// SetAuthPeer - Update to new credentials sent from a peer Minio
// server. Since credentials are already validated on the sending
// peer, here we just persist to file and update in-memory config. All
// subsequently running isAuthTokenValid() calls will fail, and clients
// will be forced to re-establish connections. Connections will be
// re-established only when the sending client has also updated its
// credentials.
func (br *browserPeerAPIHandlers) SetAuthPeer(args SetAuthPeerArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	if !args.Creds.IsValid() {
		return fmt.Errorf("Invalid credential passed")
	}

	// Acquire lock before updating global configuration.
	globalServerConfigMu.Lock()
	defer globalServerConfigMu.Unlock()

	// Update credentials in memory
	prevCred := globalServerConfig.SetCredential(args.Creds)

	// Save credentials to config file
	if err := globalServerConfig.Save(); err != nil {
		// Save the current creds when failed to update.
		globalServerConfig.SetCredential(prevCred)

		errorIf(err, "Unable to update the config with new credentials sent from browser RPC.")
		return err
	}

	return nil
}

// Sends SetAuthPeer RPCs to all peers in the Minio cluster
func updateCredsOnPeers(creds auth.Credentials) map[string]error {
	// Get list of peer addresses (from globalS3Peers)
	peers := []string{}
	for _, p := range globalS3Peers {
		peers = append(peers, p.addr)
	}

	// Array of errors for each peer
	errs := make([]error, len(peers))
	var wg sync.WaitGroup

	serverCred := globalServerConfig.GetCredential()
	// Launch go routines to send request to each peer in parallel.
	for ix := range peers {
		wg.Add(1)
		go func(ix int) {
			defer wg.Done()

			// Exclude self to avoid race with
			// invalidating the RPC token.
			if peers[ix] == globalMinioAddr {
				errs[ix] = nil
				return
			}

			// Initialize client
			client := newAuthRPCClient(authConfig{
				accessKey:       serverCred.AccessKey,
				secretKey:       serverCred.SecretKey,
				serverAddr:      peers[ix],
				secureConn:      globalIsSSL,
				serviceEndpoint: path.Join(minioReservedBucketPath, browserPeerPath),
				serviceName:     "BrowserPeer",
			})

			// Construct RPC call arguments.
			args := SetAuthPeerArgs{Creds: creds}

			// Make RPC call - we only care about error
			// response and not the reply.
			err := client.Call("BrowserPeer.SetAuthPeer", &args, &AuthRPCReply{})

			// We try a bit hard (3 attempts with 1 second delay)
			// to set creds on peers in case of failure.
			if err != nil {
				for i := 0; i < 2; i++ {
					time.Sleep(1 * time.Second) // 1 second delay.
					err = client.Call("BrowserPeer.SetAuthPeer", &args, &AuthRPCReply{})
					if err == nil {
						break
					}
				}
			}

			// Send result down the channel
			errs[ix] = err
		}(ix)
	}

	// Wait for requests to complete.
	wg.Wait()

	// Put errors into map.
	errsMap := make(map[string]error)
	for i, err := range errs {
		if err != nil {
			errsMap[peers[i]] = err
		}
	}

	return errsMap
}
