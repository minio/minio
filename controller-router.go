/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json"
)

// getRPCHandler rpc handler
func getRPCCtrlHandler() http.Handler {
	s := jsonrpc.NewServer()
	s.RegisterCodec(json.NewCodec(), "application/json")
	s.RegisterService(new(VersionService), "Version")
	s.RegisterService(new(DonutService), "Donut")
	s.RegisterService(new(AuthService), "Auth")
	s.RegisterService(new(controllerServerRPCService), "Server")
	// Add new RPC services here
	return registerRPC(router.NewRouter(), s)
}

// registerRPC - register rpc handlers
func registerRPC(mux *router.Router, s *jsonrpc.Server) http.Handler {
	mux.Handle("/rpc", s)
	return mux
}
