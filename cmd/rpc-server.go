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
	"net/rpc"
	"path"
	"strings"

	router "github.com/gorilla/mux"
)

// Storage server implements rpc primitives to facilitate exporting a
// disk over a network.
type storageServer struct {
	storage StorageAPI
	path    string
}

/// Volume operations handlers

// MakeVolHandler - make vol handler is rpc wrapper for MakeVol operation.
func (s *storageServer) MakeVolHandler(arg *string, reply *GenericReply) error {
	err := s.storage.MakeVol(*arg)
	if err != nil {
		return err
	}
	return nil
}

// ListVolsHandler - list vols handler is rpc wrapper for ListVols operation.
func (s *storageServer) ListVolsHandler(arg *string, reply *ListVolsReply) error {
	vols, err := s.storage.ListVols()
	if err != nil {
		return err
	}
	reply.Vols = vols
	return nil
}

// StatVolHandler - stat vol handler is a rpc wrapper for StatVol operation.
func (s *storageServer) StatVolHandler(arg *string, reply *VolInfo) error {
	volInfo, err := s.storage.StatVol(*arg)
	if err != nil {
		return err
	}
	*reply = volInfo
	return nil
}

// DeleteVolHandler - delete vol handler is a rpc wrapper for
// DeleteVol operation.
func (s *storageServer) DeleteVolHandler(arg *string, reply *GenericReply) error {
	err := s.storage.DeleteVol(*arg)
	if err != nil {
		return err
	}
	return nil
}

/// File operations

// StatFileHandler - stat file handler is rpc wrapper to stat file.
func (s *storageServer) StatFileHandler(arg *StatFileArgs, reply *FileInfo) error {
	fileInfo, err := s.storage.StatFile(arg.Vol, arg.Path)
	if err != nil {
		return err
	}
	*reply = fileInfo
	return nil
}

// ListDirHandler - list directory handler is rpc wrapper to list dir.
func (s *storageServer) ListDirHandler(arg *ListDirArgs, reply *[]string) error {
	entries, err := s.storage.ListDir(arg.Vol, arg.Path)
	if err != nil {
		return err
	}
	*reply = entries
	return nil
}

// ReadAllHandler - read all handler is rpc wrapper to read all storage API.
func (s *storageServer) ReadAllHandler(arg *ReadFileArgs, reply *[]byte) error {
	buf, err := s.storage.ReadAll(arg.Vol, arg.Path)
	if err != nil {
		return err
	}
	reply = &buf
	return nil
}

// ReadFileHandler - read file handler is rpc wrapper to read file.
func (s *storageServer) ReadFileHandler(arg *ReadFileArgs, reply *int64) error {
	n, err := s.storage.ReadFile(arg.Vol, arg.Path, arg.Offset, arg.Buffer)
	if err != nil {
		return err
	}
	reply = &n
	return nil
}

// AppendFileHandler - append file handler is rpc wrapper to append file.
func (s *storageServer) AppendFileHandler(arg *AppendFileArgs, reply *GenericReply) error {
	return s.storage.AppendFile(arg.Vol, arg.Path, arg.Buffer)
}

// DeleteFileHandler - delete file handler is rpc wrapper to delete file.
func (s *storageServer) DeleteFileHandler(arg *DeleteFileArgs, reply *GenericReply) error {
	return s.storage.DeleteFile(arg.Vol, arg.Path)
}

// RenameFileHandler - rename file handler is rpc wrapper to rename file.
func (s *storageServer) RenameFileHandler(arg *RenameFileArgs, reply *GenericReply) error {
	return s.storage.RenameFile(arg.SrcVol, arg.SrcPath, arg.DstVol, arg.DstPath)
}

// Initialize new storage rpc.
func newRPCServer(serverConfig serverCmdConfig) (servers []*storageServer, err error) {
	// Initialize posix storage API.
	exports := serverConfig.disks
	ignoredExports := serverConfig.ignoredDisks

	// Save ignored disks in a map
	skipDisks := make(map[string]bool)
	for _, ignoredExport := range ignoredExports {
		skipDisks[ignoredExport] = true
	}
	for _, export := range exports {
		if skipDisks[export] {
			continue
		}
		// e.g server:/mnt/disk1
		if isLocalStorage(export) {
			if idx := strings.LastIndex(export, ":"); idx != -1 {
				export = export[idx+1:]
			}
			var storage StorageAPI
			storage, err = newPosix(export)
			if err != nil && err != errDiskNotFound {
				return nil, err
			}
			if idx := strings.LastIndex(export, ":"); idx != -1 {
				export = export[idx+1:]
			}
			servers = append(servers, &storageServer{
				storage: storage,
				path:    export,
			})
		}
	}
	return servers, err
}

// registerStorageRPCRouter - register storage rpc router.
func registerStorageRPCRouters(mux *router.Router, stServers []*storageServer) {
	storageRPCServer := rpc.NewServer()
	// Create a unique route for each disk exported from this node.
	for _, stServer := range stServers {
		storageRPCServer.RegisterName("Storage", stServer)
		// Add minio storage routes.
		storageRouter := mux.PathPrefix(reservedBucket).Subrouter()
		storageRouter.Path(path.Join("/storage", stServer.path)).Handler(storageRPCServer)
	}
}
