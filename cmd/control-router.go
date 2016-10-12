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
	"strings"

	router "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/set"
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
	var newExports []string
	// Initialize auth rpc clients.
	exports := srvCmdConfig.disks
	remoteHosts := set.NewStringSet()

	var remoteControlClnts []*AuthRPCClient
	for _, export := range exports {
		// Validates if remote disk is local.
		if isLocalStorage(export) {
			continue
		}
		newExports = append(newExports, export)
	}
	for _, export := range newExports {
		var host string
		if idx := strings.LastIndex(export, ":"); idx != -1 {
			host = export[:idx]
		}
		remoteHosts.Add(fmt.Sprintf("%s:%d", host, globalMinioPort))
	}
	for host := range remoteHosts {
		remoteControlClnts = append(remoteControlClnts, newAuthClient(&authConfig{
			accessKey:   serverConfig.GetCredential().AccessKeyID,
			secretKey:   serverConfig.GetCredential().SecretAccessKey,
			secureConn:  isSSL(),
			address:     host,
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
	StorageDisks   []StorageAPI
	RemoteControls []*AuthRPCClient
	LocalNode      string
}

// Register control RPC handlers.
func registerControlRPCRouter(mux *router.Router, srvCmdConfig serverCmdConfig) {
	// Initialize Control.
	ctrlHandlers := &controlAPIHandlers{
		ObjectAPI:      newObjectLayerFn,
		RemoteControls: initRemoteControlClients(srvCmdConfig),
		LocalNode:      getLocalAddress(srvCmdConfig),
		StorageDisks:   srvCmdConfig.storageDisks,
	}

	ctrlRPCServer := rpc.NewServer()
	ctrlRPCServer.RegisterName("Control", ctrlHandlers)

	ctrlRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	ctrlRouter.Path(controlPath).Handler(ctrlRPCServer)
}
