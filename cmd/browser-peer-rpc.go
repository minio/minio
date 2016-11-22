/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"path"
	"sync"
	"time"
)

// Login handler implements JWT login token generator, which upon login request
// along with username and password is generated.
func (br *browserPeerAPIHandlers) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultInterNodeJWTExpiry, serverConfig.GetCredential())
	if err != nil {
		return err
	}
	if err = jwt.Authenticate(args.Username, args.Password); err != nil {
		return err
	}
	token, err := jwt.GenerateToken(args.Username)
	if err != nil {
		return err
	}
	reply.Token = token
	reply.ServerVersion = Version
	reply.Timestamp = time.Now().UTC()
	return nil
}

// SetAuthPeerArgs - Arguments collection for SetAuth RPC call
type SetAuthPeerArgs struct {
	// For Auth
	GenericArgs

	// New credentials that receiving peer should update to.
	Creds credential
}

// SetAuthPeer - Update to new credentials sent from a peer Minio
// server. Since credentials are already validated on the sending
// peer, here we just persist to file and update in-memory config. All
// subsequently running isRPCTokenValid() calls will fail, and clients
// will be forced to re-establish connections. Connections will be
// re-established only when the sending client has also updated its
// credentials.
func (br *browserPeerAPIHandlers) SetAuthPeer(args SetAuthPeerArgs, reply *GenericReply) error {
	// Check auth
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}

	// Update credentials in memory
	serverConfig.SetCredential(args.Creds)

	// Save credentials to config file
	if err := serverConfig.Save(); err != nil {
		errorIf(err, "Error updating config file with new credentials sent from browser RPC.")
		return err
	}

	return nil
}

// Sends SetAuthPeer RPCs to all peers in the Minio cluster
func updateCredsOnPeers(creds credential) map[string]error {
	// Get list of peer addresses (from globalS3Peers)
	peers := []string{}
	for _, p := range globalS3Peers {
		peers = append(peers, p.addr)
	}

	// Array of errors for each peer
	errs := make([]error, len(peers))
	var wg sync.WaitGroup

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
			client := newAuthClient(&authConfig{
				accessKey:   serverConfig.GetCredential().AccessKeyID,
				secretKey:   serverConfig.GetCredential().SecretAccessKey,
				address:     peers[ix],
				secureConn:  isSSL(),
				path:        path.Join(reservedBucket, browserPeerPath),
				loginMethod: "Browser.LoginHandler",
			})

			// Construct RPC call arguments.
			args := SetAuthPeerArgs{Creds: creds}

			// Make RPC call - we only care about error
			// response and not the reply.
			err := client.Call("Browser.SetAuthPeer", &args, &GenericReply{})

			// We try a bit hard (3 attempts with 1 second delay)
			// to set creds on peers in case of failure.
			if err != nil {
				for i := 0; i < 2; i++ {
					time.Sleep(1 * time.Second) // 1 second delay.
					err = client.Call("Browser.SetAuthPeer", &args, &GenericReply{})
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
