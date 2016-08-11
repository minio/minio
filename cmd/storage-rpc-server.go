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
	"errors"
	"fmt"
	"net/rpc"
	"path"
	"strings"

	jwtgo "github.com/dgrijalva/jwt-go"
	router "github.com/gorilla/mux"
)

// Storage server implements rpc primitives to facilitate exporting a
// disk over a network.
type storageServer struct {
	storage StorageAPI
	path    string
}

// Validates if incoming token is valid.
func isRPCTokenValid(tokenStr string) bool {
	jwt, err := newJWT(defaultWebTokenExpiry) // Expiry set to 24Hrs.
	if err != nil {
		errorIf(err, "Unable to initialize JWT")
		return false
	}
	token, err := jwtgo.Parse(tokenStr, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(jwt.SecretAccessKey), nil
	})
	if err != nil {
		errorIf(err, "Unable to parse JWT token string")
		return false
	}
	// Return if token is valid.
	return token.Valid
}

/// Auth operations

// Login - login handler.
func (s *storageServer) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultTokenExpiry)
	if err != nil {
		return err
	}
	if err = jwt.Authenticate(args.Username, args.Password); err != nil {
		return err
	}
	token, err := jwt.GenerateToken(args.Username)
	if err != nil {
		return err
	}
	reply.Token = token
	reply.ServerVersion = minioVersion
	return nil
}

/// Volume operations handlers

// MakeVolHandler - make vol handler is rpc wrapper for MakeVol operation.
func (s *storageServer) MakeVolHandler(args *GenericVolArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	return s.storage.MakeVol(args.Vol)
}

// ListVolsHandler - list vols handler is rpc wrapper for ListVols operation.
func (s *storageServer) ListVolsHandler(token *string, reply *ListVolsReply) error {
	if !isRPCTokenValid(*token) {
		return errors.New("Invalid token")
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
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
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
func (s *storageServer) DeleteVolHandler(args *GenericVolArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	return s.storage.DeleteVol(args.Vol)
}

/// File operations

// StatFileHandler - stat file handler is rpc wrapper to stat file.
func (s *storageServer) StatFileHandler(args *StatFileArgs, reply *FileInfo) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
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
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	entries, err := s.storage.ListDir(args.Vol, args.Path)
	if err != nil {
		return err
	}
	*reply = entries
	return nil
}

// ReadAllHandler - read all handler is rpc wrapper to read all storage API.
func (s *storageServer) ReadAllHandler(args *ReadFileArgs, reply *[]byte) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	buf, err := s.storage.ReadAll(args.Vol, args.Path)
	if err != nil {
		return err
	}
	*reply = buf
	return nil
}

// ReadFileHandler - read file handler is rpc wrapper to read file.
func (s *storageServer) ReadFileHandler(args *ReadFileArgs, reply *int64) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	n, err := s.storage.ReadFile(args.Vol, args.Path, args.Offset, args.Buffer)
	if err != nil {
		return err
	}
	*reply = n
	return nil
}

// AppendFileHandler - append file handler is rpc wrapper to append file.
func (s *storageServer) AppendFileHandler(args *AppendFileArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	return s.storage.AppendFile(args.Vol, args.Path, args.Buffer)
}

// DeleteFileHandler - delete file handler is rpc wrapper to delete file.
func (s *storageServer) DeleteFileHandler(args *DeleteFileArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	return s.storage.DeleteFile(args.Vol, args.Path)
}

// RenameFileHandler - rename file handler is rpc wrapper to rename file.
func (s *storageServer) RenameFileHandler(args *RenameFileArgs, reply *GenericReply) error {
	if !isRPCTokenValid(args.Token) {
		return errors.New("Invalid token")
	}
	return s.storage.RenameFile(args.SrcVol, args.SrcPath, args.DstVol, args.DstPath)
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
	// Create a unique route for each disk exported from this node.
	for _, stServer := range stServers {
		storageRPCServer := rpc.NewServer()
		storageRPCServer.RegisterName("Storage", stServer)
		// Add minio storage routes.
		storageRouter := mux.PathPrefix(reservedBucket).Subrouter()
		storageRouter.Path(path.Join("/storage", stServer.path)).Handler(storageRPCServer)
	}
}
