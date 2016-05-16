package main

import (
	"io"
	"net/http"
	"net/rpc"
	"strconv"

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

// DeleteFileHandler - delete file handler is rpc wrapper to delete file.
func (s *storageServer) DeleteFileHandler(arg *DeleteFileArgs, reply *GenericReply) error {
	err := s.storage.DeleteFile(arg.Vol, arg.Path)
	if err != nil {
		return err
	}
	return nil
}

// RenameFileHandler - rename file handler is rpc wrapper to rename file.
func (s *storageServer) RenameFileHandler(arg *RenameFileArgs, reply *GenericReply) error {
	err := s.storage.RenameFile(arg.SrcVol, arg.SrcPath, arg.DstVol, arg.DstPath)
	if err != nil {
		return err
	}
	return nil
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
	// StreamUpload - stream upload handler.
	storageRouter.Methods("POST").Path("/storage/upload/{volume}/{path:.+}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := router.Vars(r)
		volume := vars["volume"]
		path := vars["path"]
		writeCloser, err := stServer.storage.CreateFile(volume, path)
		if err != nil {
			httpErr := http.StatusInternalServerError
			if err == errVolumeNotFound {
				httpErr = http.StatusNotFound
			} else if err == errIsNotRegular {
				httpErr = http.StatusConflict
			}
			http.Error(w, err.Error(), httpErr)
			return
		}
		reader := r.Body
		if _, err = io.Copy(writeCloser, reader); err != nil {
			safeCloseAndRemove(writeCloser)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeCloser.Close()
		reader.Close()
	})
	// StreamDownloadHandler - stream download handler.
	storageRouter.Methods("GET").Path("/storage/download/{volume}/{path:.+}").Queries("offset", "{offset:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := router.Vars(r)
		volume := vars["volume"]
		path := vars["path"]
		offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		readCloser, err := stServer.storage.ReadFile(volume, path, offset)
		if err != nil {
			httpErr := http.StatusBadRequest
			if err == errVolumeNotFound {
				httpErr = http.StatusNotFound
			} else if err == errFileNotFound {
				httpErr = http.StatusNotFound
			}
			http.Error(w, err.Error(), httpErr)
			return
		}

		// Copy reader to writer.
		io.Copy(w, readCloser)

		// Flush out any remaining buffers to client.
		w.(http.Flusher).Flush()

		// Close the reader.
		readCloser.Close()
	})
}
