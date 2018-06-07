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
	"io"
	"path"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	xrpc "github.com/minio/minio/cmd/rpc"
)

const storageServiceName = "Storage"
const storageServiceSubPath = "/storage"

var storageServicePath = path.Join(minioReservedBucketPath, storageServiceSubPath)

// storageRPCReceiver - Storage RPC receiver for storage RPC server
type storageRPCReceiver struct {
	local *posix
}

// VolArgs - generic volume args.
type VolArgs struct {
	AuthArgs
	Vol string
}

/// Storage operations handlers.

// Connect - authenticates remote connection.
func (receiver *storageRPCReceiver) Connect(args *AuthArgs, reply *VoidReply) (err error) {
	return args.Authenticate()
}

// DiskInfo - disk info handler is rpc wrapper for DiskInfo operation.
func (receiver *storageRPCReceiver) DiskInfo(args *AuthArgs, reply *DiskInfo) (err error) {
	*reply, err = receiver.local.DiskInfo()
	return err
}

/// Volume operations handlers.

// MakeVol - make vol handler is rpc wrapper for MakeVol operation.
func (receiver *storageRPCReceiver) MakeVol(args *VolArgs, reply *VoidReply) error {
	return receiver.local.MakeVol(args.Vol)
}

// ListVols - list vols handler is rpc wrapper for ListVols operation.
func (receiver *storageRPCReceiver) ListVols(args *AuthArgs, reply *[]VolInfo) (err error) {
	*reply, err = receiver.local.ListVols()
	return err
}

// StatVol - stat vol handler is a rpc wrapper for StatVol operation.
func (receiver *storageRPCReceiver) StatVol(args *VolArgs, reply *VolInfo) (err error) {
	*reply, err = receiver.local.StatVol(args.Vol)
	return err
}

// DeleteVol - delete vol handler is a rpc wrapper for
// DeleteVol operation.
func (receiver *storageRPCReceiver) DeleteVol(args *VolArgs, reply *VoidReply) error {
	return receiver.local.DeleteVol(args.Vol)
}

/// File operations

// StatFileArgs represents stat file RPC arguments.
type StatFileArgs struct {
	AuthArgs
	Vol  string
	Path string
}

// StatFile - stat file handler is rpc wrapper to stat file.
func (receiver *storageRPCReceiver) StatFile(args *StatFileArgs, reply *FileInfo) (err error) {
	*reply, err = receiver.local.StatFile(args.Vol, args.Path)
	return err
}

// ListDirArgs represents list contents RPC arguments.
type ListDirArgs struct {
	AuthArgs
	Vol   string
	Path  string
	Count int
}

// ListDir - list directory handler is rpc wrapper to list dir.
func (receiver *storageRPCReceiver) ListDir(args *ListDirArgs, reply *[]string) (err error) {
	*reply, err = receiver.local.ListDir(args.Vol, args.Path, args.Count)
	return err
}

// ReadAllArgs represents read all RPC arguments.
type ReadAllArgs struct {
	AuthArgs
	Vol  string
	Path string
}

// ReadAll - read all handler is rpc wrapper to read all storage API.
func (receiver *storageRPCReceiver) ReadAll(args *ReadAllArgs, reply *[]byte) (err error) {
	*reply, err = receiver.local.ReadAll(args.Vol, args.Path)
	return err
}

// ReadFileArgs represents read file RPC arguments.
type ReadFileArgs struct {
	AuthArgs
	Vol          string
	Path         string
	Offset       int64
	Buffer       []byte
	Algo         BitrotAlgorithm
	ExpectedHash []byte
	Verified     bool
}

// ReadFile - read file handler is rpc wrapper to read file.
func (receiver *storageRPCReceiver) ReadFile(args *ReadFileArgs, reply *[]byte) error {
	var verifier *BitrotVerifier
	if !args.Verified {
		verifier = NewBitrotVerifier(args.Algo, args.ExpectedHash)
	}

	n, err := receiver.local.ReadFile(args.Vol, args.Path, args.Offset, args.Buffer, verifier)
	// Ignore io.ErrEnexpectedEOF for short reads i.e. less content available than requested.
	if err == io.ErrUnexpectedEOF {
		err = nil
	}

	*reply = args.Buffer[0:n]
	return err
}

// PrepareFileArgs represents append file RPC arguments.
type PrepareFileArgs struct {
	AuthArgs
	Vol  string
	Path string
	Size int64
}

// PrepareFile - prepare file handler is rpc wrapper to prepare file.
func (receiver *storageRPCReceiver) PrepareFile(args *PrepareFileArgs, reply *VoidReply) error {
	return receiver.local.PrepareFile(args.Vol, args.Path, args.Size)
}

// AppendFileArgs represents append file RPC arguments.
type AppendFileArgs struct {
	AuthArgs
	Vol    string
	Path   string
	Buffer []byte
}

// AppendFile - append file handler is rpc wrapper to append file.
func (receiver *storageRPCReceiver) AppendFile(args *AppendFileArgs, reply *VoidReply) error {
	return receiver.local.AppendFile(args.Vol, args.Path, args.Buffer)
}

// DeleteFileArgs represents delete file RPC arguments.
type DeleteFileArgs struct {
	AuthArgs
	Vol  string
	Path string
}

// DeleteFile - delete file handler is rpc wrapper to delete file.
func (receiver *storageRPCReceiver) DeleteFile(args *DeleteFileArgs, reply *VoidReply) error {
	return receiver.local.DeleteFile(args.Vol, args.Path)
}

// RenameFileArgs represents rename file RPC arguments.
type RenameFileArgs struct {
	AuthArgs
	SrcVol  string
	SrcPath string
	DstVol  string
	DstPath string
}

// RenameFile - rename file handler is rpc wrapper to rename file.
func (receiver *storageRPCReceiver) RenameFile(args *RenameFileArgs, reply *VoidReply) error {
	return receiver.local.RenameFile(args.SrcVol, args.SrcPath, args.DstVol, args.DstPath)
}

// NewStorageRPCServer - returns new storage RPC server.
func NewStorageRPCServer(endpointPath string) (*xrpc.Server, error) {
	storage, err := newPosix(endpointPath)
	if err != nil {
		return nil, err
	}

	rpcServer := xrpc.NewServer()
	if err = rpcServer.RegisterName(storageServiceName, &storageRPCReceiver{storage}); err != nil {
		return nil, err
	}

	return rpcServer, nil
}

// registerStorageRPCRouter - register storage rpc router.
func registerStorageRPCRouters(router *mux.Router, endpoints EndpointList) {
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			rpcServer, err := NewStorageRPCServer(endpoint.Path)
			if err != nil {
				logger.Fatal(uiErrUnableToWriteInBackend(err), "Unable to configure one of server's RPC services")
			}
			subrouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
			subrouter.Path(path.Join(storageServiceSubPath, endpoint.Path)).Handler(rpcServer)
		}
	}
}
