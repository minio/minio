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

package controller

import (
	"net/http"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/controller/rpc"
)

// getRPCHandler rpc handler
func getRPCHandler() http.Handler {
	s := rpc.NewServer()
	s.RegisterJSONCodec()
	s.RegisterService(new(rpc.VersionService), "Version")
	s.RegisterService(new(rpc.SysInfoService), "SysInfo")
	s.RegisterService(new(rpc.MemStatsService), "MemStats")
	s.RegisterService(new(rpc.DonutService), "Donut")
	s.RegisterService(new(rpc.AuthService), "Auth")
	// Add new RPC services here
	return registerRPC(router.NewRouter(), s)
}

// registerRPC - register rpc handlers
func registerRPC(mux *router.Router, s *rpc.Server) http.Handler {
	mux.Handle("/rpc", s)
	return mux
}
