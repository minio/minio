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
	"time"

	router "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/set"
)

// Routes paths for "minio control" commands.
const (
	controlPath = "/controller"
)

// Initializes remote controller clients for making remote requests.
func initRemoteControllerClients(srvCmdConfig serverCmdConfig) []*AuthRPCClient {
	if !srvCmdConfig.isDistXL {
		return nil
	}
	var newExports []string
	// Initialize auth rpc clients.
	exports := srvCmdConfig.disks
	ignoredExports := srvCmdConfig.ignoredDisks
	remoteHosts := set.NewStringSet()

	// Initialize ignored disks in a new set.
	ignoredSet := set.NewStringSet()
	if len(ignoredExports) > 0 {
		ignoredSet = set.CreateStringSet(ignoredExports...)
	}
	var authRPCClients []*AuthRPCClient
	for _, export := range exports {
		if ignoredSet.Contains(export) {
			// Ignore initializing ignored export.
			continue
		}
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
		authRPCClients = append(authRPCClients, newAuthClient(&authConfig{
			accessKey:   serverConfig.GetCredential().AccessKeyID,
			secretKey:   serverConfig.GetCredential().SecretAccessKey,
			secureConn:  isSSL(),
			address:     host,
			path:        path.Join(reservedBucket, controlPath),
			loginMethod: "Controller.LoginHandler",
		}))
	}
	return authRPCClients
}

// Register controller RPC handlers.
func registerControllerRPCRouter(mux *router.Router, srvCmdConfig serverCmdConfig) {
	// Initialize controller.
	ctrlHandlers := &controllerAPIHandlers{
		ObjectAPI:    newObjectLayerFn,
		StorageDisks: srvCmdConfig.storageDisks,
		timestamp:    time.Now().UTC(),
	}

	// Initializes remote controller clients.
	ctrlHandlers.RemoteControllers = initRemoteControllerClients(srvCmdConfig)

	ctrlRPCServer := rpc.NewServer()
	ctrlRPCServer.RegisterName("Controller", ctrlHandlers)

	ctrlRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	ctrlRouter.Path(controlPath).Handler(ctrlRPCServer)
}

// Handler for object healing.
type controllerAPIHandlers struct {
	ObjectAPI         func() ObjectLayer
	StorageDisks      []StorageAPI
	RemoteControllers []*AuthRPCClient
	timestamp         time.Time
}
