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
	"net/rpc"

	router "github.com/gorilla/mux"
)

const servicePath = "/admin/service"

// serviceCmd - exports RPC methods for service status, stop and
// restart commands.
type serviceCmd struct {
	AuthRPCServer
}

// Shutdown - Shutdown this instance of minio server.
func (s *serviceCmd) Shutdown(args *AuthRPCArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	globalServiceSignalCh <- serviceStop
	return nil
}

// Restart - Restart this instance of minio server.
func (s *serviceCmd) Restart(args *AuthRPCArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	globalServiceSignalCh <- serviceRestart
	return nil
}

// registerAdminRPCRouter - registers RPC methods for service status,
// stop and restart commands.
func registerAdminRPCRouter(mux *router.Router) error {
	adminRPCHandler := &serviceCmd{}
	adminRPCServer := rpc.NewServer()
	err := adminRPCServer.RegisterName("Service", adminRPCHandler)
	if err != nil {
		return traceError(err)
	}
	adminRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	adminRouter.Path(servicePath).Handler(adminRPCServer)
	return nil
}
