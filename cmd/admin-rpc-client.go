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
	"sync"
)

// localAdminClient - represents admin operation to be executed locally.
type localAdminClient struct {
}

// remoteAdminClient - represents admin operation to be executed
// remotely, via RPC.
type remoteAdminClient struct {
	*AuthRPCClient
}

// stopRestarter - abstracts stop and restart operations for both
// local and remote execution.
type stopRestarter interface {
	Stop() error
	Restart() error
}

// Stop - Sends a message over channel to the go-routine responsible
// for stopping the process.
func (lc localAdminClient) Stop() error {
	globalServiceSignalCh <- serviceStop
	return nil
}

// Restart - Sends a message over channel to the go-routine
// responsible for restarting the process.
func (lc localAdminClient) Restart() error {
	globalServiceSignalCh <- serviceRestart
	return nil
}

// Stop - Sends stop command to remote server via RPC.
func (rc remoteAdminClient) Stop() error {
	args := AuthRPCArgs{}
	reply := AuthRPCReply{}
	return rc.Call("Service.Shutdown", &args, &reply)
}

// Restart - Sends restart command to remote server via RPC.
func (rc remoteAdminClient) Restart() error {
	args := AuthRPCArgs{}
	reply := AuthRPCReply{}
	return rc.Call("Service.Restart", &args, &reply)
}

// adminPeer - represents an entity that implements Stop and Restart methods.
type adminPeer struct {
	addr    string
	svcClnt stopRestarter
}

// type alias for a collection of adminPeer.
type adminPeers []adminPeer

// makeAdminPeers - helper function to construct a collection of adminPeer.
func makeAdminPeers(eps []*url.URL) adminPeers {
	var servicePeers []adminPeer

	// map to store peers that are already added to ret
	seenAddr := make(map[string]bool)

	// add local (self) as peer in the array
	servicePeers = append(servicePeers, adminPeer{
		globalMinioAddr,
		localAdminClient{},
	})
	seenAddr[globalMinioAddr] = true

	serverCred := serverConfig.GetCredential()
	// iterate over endpoints to find new remote peers and add
	// them to ret.
	for _, ep := range eps {
		if ep.Host == "" {
			continue
		}

		// Check if the remote host has been added already
		if !seenAddr[ep.Host] {
			cfg := authConfig{
				accessKey:       serverCred.AccessKey,
				secretKey:       serverCred.SecretKey,
				serverAddr:      ep.Host,
				secureConn:      isSSL(),
				serviceEndpoint: path.Join(reservedBucket, servicePath),
				serviceName:     "Service",
			}

			servicePeers = append(servicePeers, adminPeer{
				addr:    ep.Host,
				svcClnt: &remoteAdminClient{newAuthRPCClient(cfg)},
			})
			seenAddr[ep.Host] = true
		}
	}

	return servicePeers
}

// Initialize global adminPeer collection.
func initGlobalAdminPeers(eps []*url.URL) {
	globalAdminPeers = makeAdminPeers(eps)
}

// invokeServiceCmd - Invoke Stop/Restart command.
func invokeServiceCmd(cp adminPeer, cmd serviceSignal) (err error) {
	switch cmd {
	case serviceStop:
		err = cp.svcClnt.Stop()
	case serviceRestart:
		err = cp.svcClnt.Restart()
	}
	return err
}

// sendServiceCmd - Invoke Stop/Restart command on remote peers
// adminPeer followed by on the local peer.
func sendServiceCmd(cps adminPeers, cmd serviceSignal) {
	// Send service command like stop or restart to all remote nodes and finally run on local node.
	errs := make([]error, len(cps))
	var wg sync.WaitGroup
	remotePeers := cps[1:]
	for i := range remotePeers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = invokeServiceCmd(remotePeers[idx], cmd)
		}(i)
	}
	wg.Wait()
	errs[0] = invokeServiceCmd(cps[0], cmd)
}
