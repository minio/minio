/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	xrpc "github.com/minio/minio/cmd/rpc"
)

const adminServiceName = "Admin"
const adminServiceSubPath = "/admin"

var adminServicePath = path.Join(minioReservedBucketPath, adminServiceSubPath)

// adminRPCReceiver - Admin RPC receiver for admin RPC server.
type adminRPCReceiver struct {
	local *localAdminClient
}

// SignalServiceArgs - provides the signal argument to SignalService RPC
type SignalServiceArgs struct {
	AuthArgs
	Sig serviceSignal
}

// SignalService - Send a restart or stop signal to the service
func (receiver *adminRPCReceiver) SignalService(args *SignalServiceArgs, reply *VoidReply) error {
	return receiver.local.SignalService(args.Sig)
}

// ServerInfo - returns the server info when object layer was initialized on this server.
func (receiver *adminRPCReceiver) ServerInfo(args *AuthArgs, reply *ServerInfoData) (err error) {
	*reply, err = receiver.local.ServerInfo()
	return err
}

// StartProfilingArgs - holds the RPC argument for StartingProfiling RPC call
type StartProfilingArgs struct {
	AuthArgs
	Profiler string
}

// StartProfiling - starts profiling of this server
func (receiver *adminRPCReceiver) StartProfiling(args *StartProfilingArgs, reply *VoidReply) error {
	return receiver.local.StartProfiling(args.Profiler)
}

// DownloadProfilingData - stops and returns profiling data of this server
func (receiver *adminRPCReceiver) DownloadProfilingData(args *AuthArgs, reply *[]byte) (err error) {
	*reply, err = receiver.local.DownloadProfilingData()
	return
}

// NewAdminRPCServer - returns new admin RPC server.
func NewAdminRPCServer() (*xrpc.Server, error) {
	rpcServer := xrpc.NewServer()
	if err := rpcServer.RegisterName(adminServiceName, &adminRPCReceiver{&localAdminClient{}}); err != nil {
		return nil, err
	}
	return rpcServer, nil
}

// registerAdminRPCRouter - creates and registers Admin RPC server and its router.
func registerAdminRPCRouter(router *mux.Router) {
	rpcServer, err := NewAdminRPCServer()
	logger.FatalIf(err, "Unable to initialize Lock RPC Server")
	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(adminServiceSubPath).HandlerFunc(httpTraceHdrs(rpcServer.ServeHTTP))
}
