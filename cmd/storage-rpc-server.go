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
	"context"
	"io"
	"path"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/disk"
)

// Storage server implements rpc primitives to facilitate exporting a
// disk over a network.
type storageServer struct {
	AuthRPCServer
	storage   StorageAPI
	path      string
	timestamp time.Time
}

/// Storage operations handlers.

// DiskInfoHandler - disk info handler is rpc wrapper for DiskInfo operation.
func (s *storageServer) DiskInfoHandler(args *AuthRPCArgs, reply *disk.Info) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	info, err := s.storage.DiskInfo()
	*reply = info
	return err
}

/// Volume operations handlers.

// MakeVolHandler - make vol handler is rpc wrapper for MakeVol operation.
func (s *storageServer) MakeVolHandler(args *GenericVolArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.MakeVol(args.Vol)
}

// ListVolsHandler - list vols handler is rpc wrapper for ListVols operation.
func (s *storageServer) ListVolsHandler(args *AuthRPCArgs, reply *ListVolsReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	vols, err := s.storage.ListVols()
	if err != nil {
		return err
	}
	reply.Vols = vols
	return nil
}

// StatVolHandler - stat vol handler is a rpc wrapper for StatVol operation.
func (s *storageServer) StatVolHandler(args *GenericVolArgs, reply *VolInfo) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	volInfo, err := s.storage.StatVol(args.Vol)
	if err != nil {
		return err
	}
	*reply = volInfo
	return nil
}

// DeleteVolHandler - delete vol handler is a rpc wrapper for
// DeleteVol operation.
func (s *storageServer) DeleteVolHandler(args *GenericVolArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.DeleteVol(args.Vol)
}

/// File operations

// StatFileHandler - stat file handler is rpc wrapper to stat file.
func (s *storageServer) StatFileHandler(args *StatFileArgs, reply *FileInfo) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	fileInfo, err := s.storage.StatFile(args.Vol, args.Path)
	if err != nil {
		return err
	}
	*reply = fileInfo
	return nil
}

// ListDirHandler - list directory handler is rpc wrapper to list dir.
func (s *storageServer) ListDirHandler(args *ListDirArgs, reply *[]string) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	entries, err := s.storage.ListDir(args.Vol, args.Path, args.Count)
	if err != nil {
		return err
	}
	*reply = entries
	return nil
}

// ReadAllHandler - read all handler is rpc wrapper to read all storage API.
func (s *storageServer) ReadAllHandler(args *ReadFileArgs, reply *[]byte) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	buf, err := s.storage.ReadAll(args.Vol, args.Path)
	if err != nil {
		return err
	}
	*reply = buf
	return nil
}

// ReadFileHandler - read file handler is rpc wrapper to read file.
func (s *storageServer) ReadFileHandler(args *ReadFileArgs, reply *[]byte) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	var verifier *BitrotVerifier
	if !args.Verified {
		verifier = NewBitrotVerifier(args.Algo, args.ExpectedHash)
	}

	var n int64
	n, err = s.storage.ReadFile(args.Vol, args.Path, args.Offset, args.Buffer, verifier)
	// Sending an error over the rpc layer, would cause unmarshalling to fail. In situations
	// when we have short read i.e `io.ErrUnexpectedEOF` treat it as good condition and copy
	// the buffer properly.
	if err == io.ErrUnexpectedEOF {
		// Reset to nil as good condition.
		err = nil
	}
	*reply = args.Buffer[0:n]
	return err
}

// PrepareFileHandler - prepare file handler is rpc wrapper to prepare file.
func (s *storageServer) PrepareFileHandler(args *PrepareFileArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.PrepareFile(args.Vol, args.Path, args.Size)
}

// AppendFileHandler - append file handler is rpc wrapper to append file.
func (s *storageServer) AppendFileHandler(args *AppendFileArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.AppendFile(args.Vol, args.Path, args.Buffer)
}

// DeleteFileHandler - delete file handler is rpc wrapper to delete file.
func (s *storageServer) DeleteFileHandler(args *DeleteFileArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.DeleteFile(args.Vol, args.Path)
}

// RenameFileHandler - rename file handler is rpc wrapper to rename file.
func (s *storageServer) RenameFileHandler(args *RenameFileArgs, reply *AuthRPCReply) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}

	return s.storage.RenameFile(args.SrcVol, args.SrcPath, args.DstVol, args.DstPath)
}

// Initialize new storage rpc.
func newStorageRPCServer(endpoints EndpointList) (servers []*storageServer, err error) {
	for _, endpoint := range endpoints {
		if endpoint.IsLocal {
			storage, err := newPosix(endpoint.Path)
			if err != nil && err != errDiskNotFound {
				return nil, err
			}

			servers = append(servers, &storageServer{
				storage: storage,
				path:    endpoint.Path,
			})
		}
	}

	return servers, nil
}

// registerStorageRPCRouter - register storage rpc router.
func registerStorageRPCRouters(router *mux.Router, endpoints EndpointList) error {
	// Initialize storage rpc servers for every disk that is hosted on this node.
	storageRPCs, err := newStorageRPCServer(endpoints)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}

	// Create a unique route for each disk exported from this node.
	for _, stServer := range storageRPCs {
		storageRPCServer := newRPCServer()
		err = storageRPCServer.RegisterName("Storage", stServer)
		if err != nil {
			logger.LogIf(context.Background(), err)
			return err
		}
		// Add minio storage routes.
		storageRouter := router.PathPrefix(minioReservedBucketPath).Subrouter()
		storageRouter.Path(path.Join(storageRPCPath, stServer.path)).Handler(storageRPCServer)
	}
	return nil
}
