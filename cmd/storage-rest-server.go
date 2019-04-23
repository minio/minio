/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
)

var errConnectionStale = errors.New("connection stale, REST client/server instance-id mismatch")

// To abstract a disk over network.
type storageRESTServer struct {
	storage *posix
	// Used to detect reboot of servers so that peers revalidate format.json as
	// different disk might be available on the same mount point after reboot.
	instanceID string
}

func (s *storageRESTServer) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
	w.(http.Flusher).Flush()
}

// DefaultSkewTime - skew time is 15 minutes between minio peers.
const DefaultSkewTime = 15 * time.Minute

// Authenticates storage client's requests and validates for skewed time.
func storageServerRequestValidate(r *http.Request) error {
	_, owner, err := webRequestAuthenticate(r)
	if err != nil {
		return err
	}
	if !owner { // Disable access for non-admin users.
		return errAuthentication
	}

	requestTimeStr := r.Header.Get("X-Minio-Time")
	requestTime, err := time.Parse(time.RFC3339, requestTimeStr)
	if err != nil {
		return err
	}
	utcNow := UTCNow()
	delta := requestTime.Sub(utcNow)
	if delta < 0 {
		delta = delta * -1
	}
	if delta > DefaultSkewTime {
		return fmt.Errorf("client time %v is too apart with server time %v", requestTime, utcNow)
	}
	return nil
}

// IsValid - To authenticate and verify the time difference.
func (s *storageRESTServer) IsValid(w http.ResponseWriter, r *http.Request) bool {
	if err := storageServerRequestValidate(r); err != nil {
		s.writeErrorResponse(w, err)
		return false
	}
	instanceID := r.URL.Query().Get(storageRESTInstanceID)
	if instanceID != s.instanceID {
		// This will cause the peer to revalidate format.json using a new storage-rest-client instance.
		s.writeErrorResponse(w, errConnectionStale)
		return false
	}
	return true
}

// GetInstanceID - returns the instance ID of the server.
func (s *storageRESTServer) GetInstanceID(w http.ResponseWriter, r *http.Request) {
	if err := storageServerRequestValidate(r); err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(s.instanceID)))
	w.Write([]byte(s.instanceID))
	w.(http.Flusher).Flush()
}

// DiskInfoHandler - returns disk info.
func (s *storageRESTServer) DiskInfoHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	info, err := s.storage.DiskInfo()
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer w.(http.Flusher).Flush()
	gob.NewEncoder(w).Encode(info)
}

// MakeVolHandler - make a volume.
func (s *storageRESTServer) MakeVolHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	err := s.storage.MakeVol(volume)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// ListVolsHandler - list volumes.
func (s *storageRESTServer) ListVolsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	infos, err := s.storage.ListVols()
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer w.(http.Flusher).Flush()
	gob.NewEncoder(w).Encode(&infos)
}

// StatVolHandler - stat a volume.
func (s *storageRESTServer) StatVolHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	info, err := s.storage.StatVol(volume)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer w.(http.Flusher).Flush()
	gob.NewEncoder(w).Encode(info)
}

// DeleteVolumeHandler - delete a volume.
func (s *storageRESTServer) DeleteVolHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	err := s.storage.DeleteVol(volume)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// AppendFileHandler - append data from the request to the file specified.
func (s *storageRESTServer) AppendFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	buf := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	err = s.storage.AppendFile(volume, filePath, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// CreateFileHandler - fallocate() space for a file and copy the contents from the request.
func (s *storageRESTServer) CreateFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	fileSizeStr := vars[storageRESTLength]
	fileSize, err := strconv.Atoi(fileSizeStr)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	err = s.storage.CreateFile(volume, filePath, int64(fileSize), r.Body)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// WriteAllHandler - write to file all content.
func (s *storageRESTServer) WriteAllHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	buf := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	err = s.storage.WriteAll(volume, filePath, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// StatFileHandler - stat a file.
func (s *storageRESTServer) StatFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	info, err := s.storage.StatFile(volume, filePath)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer w.(http.Flusher).Flush()
	gob.NewEncoder(w).Encode(info)
}

// ReadAllHandler - read all the contents of a file.
func (s *storageRESTServer) ReadAllHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	buf, err := s.storage.ReadAll(volume, filePath)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.Write(buf)
	w.(http.Flusher).Flush()
}

// ReadFileHandler - read section of a file.
func (s *storageRESTServer) ReadFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]
	offset, err := strconv.Atoi(vars[storageRESTOffset])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	length, err := strconv.Atoi(vars[storageRESTLength])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	if offset < 0 || length < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}
	var verifier *BitrotVerifier
	if vars[storageRESTBitrotAlgo] != "" {
		hashStr := vars[storageRESTBitrotHash]
		var hash []byte
		hash, err = hex.DecodeString(hashStr)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
		verifier = NewBitrotVerifier(BitrotAlgorithmFromString(vars[storageRESTBitrotAlgo]), hash)
	}
	buf := make([]byte, length)
	_, err = s.storage.ReadFile(volume, filePath, int64(offset), buf, verifier)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.Write(buf)
	w.(http.Flusher).Flush()
}

// ReadFileHandler - read section of a file.
func (s *storageRESTServer) ReadFileStreamHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]
	offset, err := strconv.Atoi(vars[storageRESTOffset])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	length, err := strconv.Atoi(vars[storageRESTLength])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	rc, err := s.storage.ReadFileStream(volume, filePath, int64(offset), int64(length))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Length", strconv.Itoa(length))

	io.Copy(w, rc)
	w.(http.Flusher).Flush()
}

// ListDirHandler - list a directory.
func (s *storageRESTServer) ListDirHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	dirPath := vars[storageRESTDirPath]
	leafFile := vars[storageRESTLeafFile]
	count, err := strconv.Atoi(vars[storageRESTCount])
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	entries, err := s.storage.ListDir(volume, dirPath, count, leafFile)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer w.(http.Flusher).Flush()
	gob.NewEncoder(w).Encode(&entries)
}

// DeleteFileHandler - delete a file.
func (s *storageRESTServer) DeleteFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	filePath := vars[storageRESTFilePath]

	err := s.storage.DeleteFile(volume, filePath)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// RenameFileHandler - rename a file.
func (s *storageRESTServer) RenameFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	srcVolume := vars[storageRESTSrcVolume]
	srcFilePath := vars[storageRESTSrcPath]
	dstVolume := vars[storageRESTDstVolume]
	dstFilePath := vars[storageRESTDstPath]
	err := s.storage.RenameFile(srcVolume, srcFilePath, dstVolume, dstFilePath)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// registerStorageRPCRouter - register storage rpc router.
func registerStorageRESTHandlers(router *mux.Router, endpoints EndpointList) {
	for _, endpoint := range endpoints {
		if !endpoint.IsLocal {
			continue
		}
		storage, err := newPosix(endpoint.Path)
		if err != nil {
			logger.Fatal(uiErrUnableToWriteInBackend(err), "Unable to initialize posix backend")
		}

		server := &storageRESTServer{storage, mustGetUUID()}

		subrouter := router.PathPrefix(path.Join(storageRESTPath, endpoint.Path)).Subrouter()

		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodDiskInfo).HandlerFunc(httpTraceHdrs(server.DiskInfoHandler))
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodMakeVol).HandlerFunc(httpTraceHdrs(server.MakeVolHandler)).Queries(restQueries(storageRESTVolume)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodStatVol).HandlerFunc(httpTraceHdrs(server.StatVolHandler)).Queries(restQueries(storageRESTVolume)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodDeleteVol).HandlerFunc(httpTraceHdrs(server.DeleteVolHandler)).Queries(restQueries(storageRESTVolume)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodListVols).HandlerFunc(httpTraceHdrs(server.ListVolsHandler))

		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodAppendFile).HandlerFunc(httpTraceHdrs(server.AppendFileHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodWriteAll).HandlerFunc(httpTraceHdrs(server.WriteAllHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodCreateFile).HandlerFunc(httpTraceHdrs(server.CreateFileHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath, storageRESTLength)...)

		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodStatFile).HandlerFunc(httpTraceHdrs(server.StatFileHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodReadAll).HandlerFunc(httpTraceHdrs(server.ReadAllHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodReadFile).HandlerFunc(httpTraceHdrs(server.ReadFileHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath, storageRESTOffset, storageRESTLength, storageRESTBitrotAlgo, storageRESTBitrotHash)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodReadFileStream).HandlerFunc(httpTraceHdrs(server.ReadFileStreamHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath, storageRESTOffset, storageRESTLength)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodListDir).HandlerFunc(httpTraceHdrs(server.ListDirHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTDirPath, storageRESTCount, storageRESTLeafFile)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodDeleteFile).HandlerFunc(httpTraceHdrs(server.DeleteFileHandler)).
			Queries(restQueries(storageRESTVolume, storageRESTFilePath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodRenameFile).HandlerFunc(httpTraceHdrs(server.RenameFileHandler)).
			Queries(restQueries(storageRESTSrcVolume, storageRESTSrcPath, storageRESTDstVolume, storageRESTDstPath)...)
		subrouter.Methods(http.MethodPost).Path("/" + storageRESTMethodGetInstanceID).HandlerFunc(httpTraceAll(server.GetInstanceID))
	}

	router.NotFoundHandler = http.HandlerFunc(httpTraceAll(notFoundHandler))
}
