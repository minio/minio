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

package main

import (
	"net/http"
	"path"

	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
)

// Node RPC prefix.
const multiNodeRPCPrefix = restrictedBucket

// nodeAPI container for internode communication API.
type nodeAPI struct{}

// initNode instantiate a new Node.
func initNode(conf cloudServerConfig) *nodeAPI {
	return &nodeAPI{}
}

// Handlers requests coming in for different rpc versions.
type rpcVersionDifferentHandler struct {
	Version string
}

func (rpc rpcVersionDifferentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	jsonrpc.WriteError(w, http.StatusBadRequest, "Unsupported version string "+rpc.Version)
}

// registerNodeRPCRouter - registers RPC router for inter node communication.
func registerNodeRPCRouter(mux *router.Router, node *nodeAPI) {
	// Initialize a new json2 codec.
	codec := json2.NewCodec()

	// MultiNode internal rpc router.
	multiNodeRouter := mux.NewRoute().PathPrefix(multiNodeRPCPrefix).Subrouter()

	// Initialize node rpc handlers.
	nodeRPC := jsonrpc.NewServer()
	nodeRPC.RegisterCodec(codec, "application/json")
	nodeRPC.RegisterCodec(codec, "application/json; charset=UTF-8")
	nodeRPC.RegisterService(node, "Node")

	// RPC handler at URI - webBrowserPrefix/rpc.
	multiNodeRouter.Path(path.Join("/rpc", minioVersion)).Handler(nodeRPC)
	multiNodeRouter.Path("/rpc/{unsupported:.*}").Handler(rpcVersionDifferentHandler{Version: minioVersion})
}
