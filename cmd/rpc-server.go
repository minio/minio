package cmd

import (
	"net/rpc"

	router "github.com/gorilla/mux"
)

// Storage server implements rpc primitives to facilitate exporting a
// disk over a network.
type storageServer struct {
	storage StorageAPI
}

/// Volume operations handlers

// MakeVolHandler - make vol handler is rpc wrapper for MakeVol operation.
func (s *storageServer) MakeVolHandler(arg *string, reply *GenericReply) error {
	return s.storage.MakeVol(*arg)
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
	return s.storage.DeleteVol(*arg)
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
func newRPCServer(exportPath string) (*storageServer, error) {
	// Initialize posix storage API.
	storage, err := newPosix(exportPath)
	if err != nil && err != errDiskNotFound {
		return nil, err
	}
	return &storageServer{
		storage: storage,
	}, nil
}

// registerStorageRPCRouter - register storage rpc router.
func registerStorageRPCRouter(mux *router.Router, stServer *storageServer) {
	storageRPCServer := rpc.NewServer()
	storageRPCServer.RegisterName("Storage", stServer)
	storageRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	// Add minio storage routes.
	storageRouter.Path("/storage").Handler(storageRPCServer)
}
