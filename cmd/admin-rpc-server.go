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
	"time"

	router "github.com/gorilla/mux"
)

const adminPath = "/admin"

// adminCmd - exports RPC methods for service status, stop and
// restart commands.
type adminCmd struct {
	AuthRPCServer
}

// ListLocksQuery - wraps ListLocks API's query values to send over RPC.
type ListLocksQuery struct {
	AuthRPCArgs
	bucket  string
	prefix  string
	relTime time.Duration
}

// ListLocksReply - wraps ListLocks response over RPC.
type ListLocksReply struct {
	AuthRPCReply
	volLocks []VolumeLockInfo
}

// Shutdown - Shutdown this instance of minio server.
func (s *adminCmd) Shutdown(args *AuthRPCArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	globalServiceSignalCh <- serviceStop
	return nil
}

// Restart - Restart this instance of minio server.
func (s *adminCmd) Restart(args *AuthRPCArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	globalServiceSignalCh <- serviceRestart
	return nil
}

// ListLocks - lists locks held by requests handled by this server instance.
func (s *adminCmd) ListLocks(query *ListLocksQuery, reply *ListLocksReply) error {
	volLocks := listLocksInfo(query.bucket, query.prefix, query.relTime)
	*reply = ListLocksReply{volLocks: volLocks}
	return nil
}

// registerAdminRPCRouter - registers RPC methods for service status,
// stop and restart commands.
func registerAdminRPCRouter(mux *router.Router) error {
	adminRPCHandler := &adminCmd{}
	adminRPCServer := rpc.NewServer()
	err := adminRPCServer.RegisterName("Admin", adminRPCHandler)
	if err != nil {
		return traceError(err)
	}
	adminRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	adminRouter.Path(adminPath).Handler(adminRPCServer)
	return nil
}
