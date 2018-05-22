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
	"context"
	"path"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	xrpc "github.com/minio/minio/cmd/rpc"
)

const adminServiceName = "Admin"
const adminServiceSubPath = "/admin"

var adminServicePath = path.Join(minioReservedBucketPath, peerServiceSubPath)

// adminRPCReceiver - Admin RPC receiver for admin RPC server.
type adminRPCReceiver struct {
	local *localAdminClient
}

// SendSignalArgs - arguments for SendSignal RPC request.
type SendSignalArgs struct {
	AuthArgs
	Signal serviceSignal
}

// SendSignal - handles SendSignal RPC request.
func (receiver *adminRPCReceiver) SendSignal(args *SendSignalArgs, reply *VoidReply) error {
	return receiver.local.SendSignal(args.Signal)
}

// ReloadFormatArgs - arguments for ReloadFormat RPC request.
type ReloadFormatArgs struct {
	AuthArgs
	DryRun bool
}

// ReloadFormat - handles ReloadFormat RPC request.
func (receiver *adminRPCReceiver) ReloadFormat(args *ReloadFormatArgs, reply *VoidReply) error {
	return receiver.local.ReloadFormat(args.DryRun)
}

// ListLocksArgs - arguments for ListLocks RPC request.
type ListLocksArgs struct {
	AuthArgs
	Bucket   string
	Prefix   string
	Duration time.Duration
}

// ListLocks - handles ListLocks RPC request.
func (receiver *adminRPCReceiver) ListLocks(args *ListLocksArgs, reply *[]VolumeLockInfo) (err error) {
	*reply, err = receiver.local.ListLocks(args.Bucket, args.Prefix, args.Duration)
	return err
}

// GetServerInfo - handles GetServerInfo RPC request.
func (receiver *adminRPCReceiver) GetServerInfo(args *AuthArgs, reply *ServerInfoData) (err error) {
	*reply, err = receiver.local.GetServerInfo()
	return err
}

// GetConfig - handles GetConfig RPC request.
func (receiver *adminRPCReceiver) GetConfig(args *AuthArgs, reply *serverConfig) (err error) {
	*reply, err = receiver.local.GetConfig()
	return err
}

// SaveStageConfigArgs - arguments for SaveStageConfig RPC request.
type SaveStageConfigArgs struct {
	AuthArgs
	Filename string
	Config   serverConfig
}

// SaveStageConfig - handles SaveStageConfig RPC request.
func (receiver *adminRPCReceiver) SaveStageConfig(args *SaveStageConfigArgs, reply *VoidReply) error {
	return receiver.local.SaveStageConfig(args.Filename, args.Config)
}

// CommitConfigArgs - arguments for CommitConfig RPC request.
type CommitConfigArgs struct {
	AuthArgs
	FileName string
}

// CommitConfig - handles CommitConfig RPC request.
func (receiver *adminRPCReceiver) CommitConfig(args *CommitConfigArgs, reply *VoidReply) error {
	return receiver.local.CommitConfig(args.FileName)
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
	logger.CriticalIf(context.Background(), err)
	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(adminServiceSubPath).Handler(rpcServer)
}
