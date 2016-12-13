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
	"net/rpc"
	"net/url"
	"path"
	"sync"
)

type localAdminClient struct {
}

type remoteAdminClient struct {
	*AuthRPCClient
}

type stopRestarter interface {
	Stop() error
	Restart() error
}

func (lc localAdminClient) Stop() error {
	globalServiceSignalCh <- serviceStop
	return nil
}

func (lc localAdminClient) Restart() error {
	globalServiceSignalCh <- serviceRestart
	return nil
}

func (rc remoteAdminClient) Stop() error {
	args := GenericArgs{}
	reply := GenericReply{}
	err := rc.Call("Service.Shutdown", &args, &reply)
	if err != nil && err == rpc.ErrShutdown {
		rc.Close()
	}
	return err
}

func (rc remoteAdminClient) Restart() error {
	args := GenericArgs{}
	reply := GenericReply{}
	err := rc.Call("Service.Restart", &args, &reply)
	if err != nil && err == rpc.ErrShutdown {
		rc.Close()
	}
	return err
}

type controlPeer struct {
	addr    string
	svcClnt stopRestarter
}
type controlPeers []controlPeer

func makeControlPeers(eps []*url.URL) controlPeers {
	var ret []controlPeer

	// map to store peers that are already added to ret
	seenAddr := make(map[string]bool)

	// add local (self) as peer in the array
	ret = append(ret, controlPeer{
		globalMinioAddr,
		localAdminClient{},
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
				path:        path.Join(reservedBucket, servicePath),
				loginMethod: "Service.LoginHandler",
			}

			ret = append(ret, controlPeer{
				addr:    ep.Host,
				svcClnt: &remoteAdminClient{newAuthClient(&cfg)},
			})
			seenAddr[ep.Host] = true
		}
	}

	return ret
}

func initGlobalAdminPeers(eps []*url.URL) {
	globalAdminPeers = makeControlPeers(eps)
}

func invokeServiceCmd(cp controlPeer, cmd serviceSignal) (err error) {
	switch cmd {
	case serviceStop:
		err = cp.svcClnt.Stop()
	case serviceRestart:
		err = cp.svcClnt.Restart()
	}
	return err
}

func sendServiceCmd(cps controlPeers, cmd serviceSignal) {
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
