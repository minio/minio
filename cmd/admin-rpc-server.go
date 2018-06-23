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

// ListLocksQuery - wraps ListLocks API's query values to send over RPC.
type ListLocksQuery struct {
	AuthArgs
	Bucket   string
	Prefix   string
	Duration time.Duration
}

// ListLocks - lists locks held by requests handled by this server instance.
func (receiver *adminRPCReceiver) ListLocks(args *ListLocksQuery, reply *[]VolumeLockInfo) (err error) {
	*reply, err = receiver.local.ListLocks(args.Bucket, args.Prefix, args.Duration)
	return err
}

// ServerInfo - returns the server info when object layer was initialized on this server.
func (receiver *adminRPCReceiver) ServerInfo(args *AuthArgs, reply *ServerInfoData) (err error) {
	*reply, err = receiver.local.ServerInfo()
	return err
}

// GetConfig - returns the config.json of this server.
func (receiver *adminRPCReceiver) GetConfig(args *AuthArgs, reply *[]byte) (err error) {
	*reply, err = receiver.local.GetConfig()
	return err
}

// ReInitFormatArgs - provides dry-run information to re-initialize format.json
type ReInitFormatArgs struct {
	AuthArgs
	DryRun bool
}

// ReInitFormat - re-init 'format.json'
func (receiver *adminRPCReceiver) ReInitFormat(args *ReInitFormatArgs, reply *VoidReply) error {
	return receiver.local.ReInitFormat(args.DryRun)
}

// WriteConfigArgs - wraps the bytes to be written and temporary file name.
type WriteConfigArgs struct {
	AuthArgs
	TmpFileName string
	Buf         []byte
}

// WriteTmpConfig - writes the supplied config contents onto the
// supplied temporary file.
func (receiver *adminRPCReceiver) WriteTmpConfig(args *WriteConfigArgs, reply *VoidReply) error {
	return receiver.local.WriteTmpConfig(args.TmpFileName, args.Buf)
}

// CommitConfigArgs - wraps the config file name that needs to be
// committed into config.json on this node.
type CommitConfigArgs struct {
	AuthArgs
	FileName string
}

// CommitConfig - Renames the temporary file into config.json on this node.
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
	logger.FatalIf(err, "Unable to initialize Lock RPC Server", context.Background())
	subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
	subrouter.Path(adminServiceSubPath).Handler(rpcServer)
}
