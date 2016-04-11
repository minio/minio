package main

import (
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"path"
	"strconv"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
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

// ListFilesHandler - list files handler.
func (s *storageServer) ListFilesHandler(arg *ListFilesArgs, reply *ListFilesReply) error {
	files, eof, err := s.storage.ListFiles(arg.Vol, arg.Prefix, arg.Marker, arg.Recursive, arg.Count)
	if err != nil {
		return err
	}
	reply.Files = files
	reply.EOF = eof
	return nil
}

// ReadFileHandler - read file handler is a wrapper to provide
// destination URL for reading files.
func (s *storageServer) ReadFileHandler(arg *ReadFileArgs, reply *ReadFileReply) error {
	endpoint := "http://localhost:9000/minio/rpc/storage" // TODO fix this.
	newURL, err := url.Parse(fmt.Sprintf("%s/%s", endpoint, path.Join(arg.Vol, arg.Path)))
	if err != nil {
		return err
	}
	q := newURL.Query()
	q.Set("offset", fmt.Sprintf("%d", arg.Offset))
	newURL.RawQuery = q.Encode()
	reply.URL = newURL.String()
	return nil
}

// CreateFileHandler - create file handler is rpc wrapper to create file.
func (s *storageServer) CreateFileHandler(arg *CreateFileArgs, reply *CreateFileReply) error {
	endpoint := "http://localhost:9000/minio/rpc/storage" // TODO fix this.
	newURL, err := url.Parse(fmt.Sprintf("%s/%s", endpoint, path.Join(arg.Vol, arg.Path)))
	if err != nil {
		return err
	}
	reply.URL = newURL.String()
	return nil
}

// StatFileHandler - stat file handler is rpc wrapper to stat file.
func (s *storageServer) StatFileHandler(arg *StatFileArgs, reply *FileInfo) error {
	fileInfo, err := s.storage.StatFile(arg.Vol, arg.Path)
	if err != nil {
		return err
	}
	*reply = fileInfo
	return nil
}

// DeleteFileHandler - delete file handler is rpc wrapper to delete file.
func (s *storageServer) DeleteFileHandler(arg *DeleteFileArgs, reply *GenericReply) error {
	return s.storage.DeleteFile(arg.Vol, arg.Path)
}

// StreamUpload - stream upload handler.
func (s *storageServer) StreamUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	volume := vars["volume"]
	path := vars["path"]
	writeCloser, err := s.storage.CreateFile(volume, path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reader := r.Body
	if _, err = io.Copy(writeCloser, reader); err != nil {
		writeCloser.(*safe.File).CloseAndRemove()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeCloser.Close()
}

// StreamDownloadHandler - stream download handler.
func (s *storageServer) StreamDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := router.Vars(r)
	volume := vars["volume"]
	path := vars["path"]
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	readCloser, err := s.storage.ReadFile(volume, path, offset)
	if err != nil {
		httpErr := http.StatusBadRequest
		if os.IsNotExist(err) {
			httpErr = http.StatusNotFound
		}
		http.Error(w, err.Error(), httpErr)
		return
	}
	io.Copy(w, readCloser)
}

func registerStorageServer(mux *router.Router, diskPath string) {
	// Minio storage routes.
	fs, e := newFS(diskPath)
	fatalIf(probe.NewError(e), "Unable to initialize storage disk.", nil)
	storageRPCServer := rpc.NewServer()
	stServer := &storageServer{
		storage: fs,
	}
	storageRPCServer.RegisterName("Storage", stServer)
	storageRouter := mux.NewRoute().PathPrefix(reservedBucket).Subrouter()
	storageRouter.Path("/rpc/storage").Handler(storageRPCServer)
	storageRouter.Methods("POST").Path("/rpc/storage/upload/{volume}/{path:.+}").HandlerFunc(stServer.StreamUploadHandler)
	storageRouter.Methods("GET").Path("/rpc/storage/download/{volume}/{path:.+}").Queries("offset", "").HandlerFunc(stServer.StreamDownloadHandler)
}
