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
	"fmt"
	"net/rpc"
	"path"

	router "github.com/gorilla/mux"
)

// Routes paths for "minio control" commands.
const (
	controlPath = "/control"
)

// Initializes remote control clients for making remote requests.
func initRemoteControlClients(srvCmdConfig serverCmdConfig) []*AuthRPCClient {
	if !srvCmdConfig.isDistXL {
		return nil
	}
	// Initialize auth rpc clients.
	var remoteControlClnts []*AuthRPCClient
	localMap := make(map[storageEndPoint]int)
	for _, ep := range srvCmdConfig.endPoints {
		// Set path to "" so that it is not used for filtering the
		// unique entries.
		ep.path = ""
		// Validates if remote disk is local.
		if isLocalStorage(ep) {
			continue
		}
		if localMap[ep] == 1 {
			continue
		}
		localMap[ep]++
		remoteControlClnts = append(remoteControlClnts, newAuthClient(&authConfig{
			accessKey:   serverConfig.GetCredential().AccessKeyID,
			secretKey:   serverConfig.GetCredential().SecretAccessKey,
			secureConn:  isSSL(),
			address:     fmt.Sprintf("%s:%d", ep.host, ep.port),
			path:        path.Join(reservedBucket, controlPath),
			loginMethod: "Control.LoginHandler",
		}))
	}
	return remoteControlClnts
}

// Represents control object which provides handlers for control
// operations on server.
type controlAPIHandlers struct {
	ObjectAPI      func() ObjectLayer
	IsXL           bool
	RemoteControls []*AuthRPCClient
	LocalNode      string
	StorageDisks   []StorageAPI
}

// Register control RPC handlers.
func registerControlRPCRouter(mux *router.Router, srvCmdConfig serverCmdConfig) (err error) {
	// Initialize Control.
	ctrlHandlers := &controlAPIHandlers{
		ObjectAPI:      newObjectLayerFn,
		IsXL:           srvCmdConfig.isDistXL || len(srvCmdConfig.storageDisks) > 1,
		RemoteControls: initRemoteControlClients(srvCmdConfig),
		LocalNode:      getLocalAddress(srvCmdConfig),
		StorageDisks:   srvCmdConfig.storageDisks,
	}

	ctrlRPCServer := rpc.NewServer()
	err = ctrlRPCServer.RegisterName("Control", ctrlHandlers)
	if err != nil {
		return traceError(err)
	}

	ctrlRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	ctrlRouter.Path(controlPath).Handler(ctrlRPCServer)
	return nil
}
