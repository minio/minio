// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/user"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/grid"
	"github.com/tinylib/msgp/msgp"

	jwtreq "github.com/golang-jwt/jwt/v4/request"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	xjwt "github.com/minio/minio/internal/jwt"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	xnet "github.com/minio/pkg/v3/net"
)

var errDiskStale = errors.New("drive stale")

// To abstract a disk over network.
type storageRESTServer struct {
	endpoint Endpoint
}

var (
	storageCheckPartsRPC       = grid.NewStream[*CheckPartsHandlerParams, grid.NoPayload, *CheckPartsResp](grid.HandlerCheckParts3, func() *CheckPartsHandlerParams { return &CheckPartsHandlerParams{} }, nil, func() *CheckPartsResp { return &CheckPartsResp{} })
	storageDeleteFileRPC       = grid.NewSingleHandler[*DeleteFileHandlerParams, grid.NoPayload](grid.HandlerDeleteFile, func() *DeleteFileHandlerParams { return &DeleteFileHandlerParams{} }, grid.NewNoPayload).AllowCallRequestPool(true)
	storageDeleteVersionRPC    = grid.NewSingleHandler[*DeleteVersionHandlerParams, grid.NoPayload](grid.HandlerDeleteVersion, func() *DeleteVersionHandlerParams { return &DeleteVersionHandlerParams{} }, grid.NewNoPayload)
	storageDiskInfoRPC         = grid.NewSingleHandler[*DiskInfoOptions, *DiskInfo](grid.HandlerDiskInfo, func() *DiskInfoOptions { return &DiskInfoOptions{} }, func() *DiskInfo { return &DiskInfo{} }).WithSharedResponse().AllowCallRequestPool(true)
	storageNSScannerRPC        = grid.NewStream[*nsScannerOptions, grid.NoPayload, *nsScannerResp](grid.HandlerNSScanner, func() *nsScannerOptions { return &nsScannerOptions{} }, nil, func() *nsScannerResp { return &nsScannerResp{} })
	storageReadAllRPC          = grid.NewSingleHandler[*ReadAllHandlerParams, *grid.Bytes](grid.HandlerReadAll, func() *ReadAllHandlerParams { return &ReadAllHandlerParams{} }, grid.NewBytes).AllowCallRequestPool(true)
	storageWriteAllRPC         = grid.NewSingleHandler[*WriteAllHandlerParams, grid.NoPayload](grid.HandlerWriteAll, func() *WriteAllHandlerParams { return &WriteAllHandlerParams{} }, grid.NewNoPayload)
	storageReadVersionRPC      = grid.NewSingleHandler[*grid.MSS, *FileInfo](grid.HandlerReadVersion, grid.NewMSS, func() *FileInfo { return &FileInfo{} })
	storageReadXLRPC           = grid.NewSingleHandler[*grid.MSS, *RawFileInfo](grid.HandlerReadXL, grid.NewMSS, func() *RawFileInfo { return &RawFileInfo{} })
	storageRenameDataRPC       = grid.NewSingleHandler[*RenameDataHandlerParams, *RenameDataResp](grid.HandlerRenameData2, func() *RenameDataHandlerParams { return &RenameDataHandlerParams{} }, func() *RenameDataResp { return &RenameDataResp{} })
	storageRenameDataInlineRPC = grid.NewSingleHandler[*RenameDataInlineHandlerParams, *RenameDataResp](grid.HandlerRenameDataInline, newRenameDataInlineHandlerParams, func() *RenameDataResp { return &RenameDataResp{} }).AllowCallRequestPool(false)
	storageRenameFileRPC       = grid.NewSingleHandler[*RenameFileHandlerParams, grid.NoPayload](grid.HandlerRenameFile, func() *RenameFileHandlerParams { return &RenameFileHandlerParams{} }, grid.NewNoPayload).AllowCallRequestPool(true)
	storageRenamePartRPC       = grid.NewSingleHandler[*RenamePartHandlerParams, grid.NoPayload](grid.HandlerRenamePart, func() *RenamePartHandlerParams { return &RenamePartHandlerParams{} }, grid.NewNoPayload)
	storageStatVolRPC          = grid.NewSingleHandler[*grid.MSS, *VolInfo](grid.HandlerStatVol, grid.NewMSS, func() *VolInfo { return &VolInfo{} })
	storageUpdateMetadataRPC   = grid.NewSingleHandler[*MetadataHandlerParams, grid.NoPayload](grid.HandlerUpdateMetadata, func() *MetadataHandlerParams { return &MetadataHandlerParams{} }, grid.NewNoPayload)
	storageWriteMetadataRPC    = grid.NewSingleHandler[*MetadataHandlerParams, grid.NoPayload](grid.HandlerWriteMetadata, func() *MetadataHandlerParams { return &MetadataHandlerParams{} }, grid.NewNoPayload)
	storageListDirRPC          = grid.NewStream[*grid.MSS, grid.NoPayload, *ListDirResult](grid.HandlerListDir, grid.NewMSS, nil, func() *ListDirResult { return &ListDirResult{} }).WithOutCapacity(1)
)

func getStorageViaEndpoint(endpoint Endpoint) StorageAPI {
	globalLocalDrivesMu.RLock()
	defer globalLocalDrivesMu.RUnlock()
	if len(globalLocalSetDrives) == 0 {
		return globalLocalDrivesMap[endpoint.String()]
	}
	return globalLocalSetDrives[endpoint.PoolIdx][endpoint.SetIdx][endpoint.DiskIdx]
}

func (s *storageRESTServer) getStorage() StorageAPI {
	return getStorageViaEndpoint(s.endpoint)
}

func (s *storageRESTServer) writeErrorResponse(w http.ResponseWriter, err error) {
	err = unwrapAll(err)
	switch err {
	case errDiskStale:
		w.WriteHeader(http.StatusPreconditionFailed)
	case errFileNotFound, errFileVersionNotFound:
		w.WriteHeader(http.StatusNotFound)
	case errInvalidAccessKeyID, errAccessKeyDisabled, errNoAuthToken, errMalformedAuth, errAuthentication, errSkewedAuthTime:
		w.WriteHeader(http.StatusUnauthorized)
	case context.Canceled, context.DeadlineExceeded:
		w.WriteHeader(499)
	default:
		w.WriteHeader(http.StatusForbidden)
	}
	w.Write([]byte(err.Error()))
}

// DefaultSkewTime - skew time is 15 minutes between minio peers.
const DefaultSkewTime = 15 * time.Minute

// validateStorageRequestToken will validate the token against the provided audience.
func validateStorageRequestToken(token string) error {
	claims := xjwt.NewStandardClaims()
	if err := xjwt.ParseWithStandardClaims(token, claims, []byte(globalActiveCred.SecretKey)); err != nil {
		return errAuthentication
	}

	owner := claims.AccessKey == globalActiveCred.AccessKey || claims.Subject == globalActiveCred.AccessKey
	if !owner {
		return errAuthentication
	}

	return nil
}

// Authenticates storage client's requests and validates for skewed time.
func storageServerRequestValidate(r *http.Request) error {
	token, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(r)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return errNoAuthToken
		}
		return errMalformedAuth
	}

	if err = validateStorageRequestToken(token); err != nil {
		return err
	}

	nanoTime, err := strconv.ParseInt(r.Header.Get("X-Minio-Time"), 10, 64)
	if err != nil {
		return errMalformedAuth
	}

	localTime := UTCNow()
	remoteTime := time.Unix(0, nanoTime)

	delta := remoteTime.Sub(localTime)
	if delta < 0 {
		delta *= -1
	}

	if delta > DefaultSkewTime {
		return errSkewedAuthTime
	}

	return nil
}

// IsAuthValid - To authenticate and verify the time difference.
func (s *storageRESTServer) IsAuthValid(w http.ResponseWriter, r *http.Request) bool {
	if s.getStorage() == nil {
		s.writeErrorResponse(w, errDiskNotFound)
		return false
	}

	if err := storageServerRequestValidate(r); err != nil {
		s.writeErrorResponse(w, err)
		return false
	}

	return true
}

// IsValid - To authenticate and check if the disk-id in the request corresponds to the underlying disk.
func (s *storageRESTServer) IsValid(w http.ResponseWriter, r *http.Request) bool {
	if !s.IsAuthValid(w, r) {
		return false
	}

	if err := r.ParseForm(); err != nil {
		s.writeErrorResponse(w, err)
		return false
	}

	diskID := r.Form.Get(storageRESTDiskID)
	if diskID == "" {
		// Request sent empty disk-id, we allow the request
		// as the peer might be coming up and trying to read format.json
		// or create format.json
		return true
	}

	storedDiskID, err := s.getStorage().GetDiskID()
	if err != nil {
		s.writeErrorResponse(w, err)
		return false
	}

	if diskID != storedDiskID {
		s.writeErrorResponse(w, errDiskStale)
		return false
	}

	// If format.json is available and request sent the right disk-id, we allow the request
	return true
}

// checkID - check if the disk-id in the request corresponds to the underlying disk.
func (s *storageRESTServer) checkID(wantID string) bool {
	if s.getStorage() == nil {
		return false
	}
	if wantID == "" {
		// Request sent empty disk-id, we allow the request
		// as the peer might be coming up and trying to read format.json
		// or create format.json
		return true
	}

	storedDiskID, err := s.getStorage().GetDiskID()
	if err != nil {
		return false
	}

	return wantID == storedDiskID
}

// HealthHandler handler checks if disk is stale
func (s *storageRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	s.IsValid(w, r)
}

// DiskInfoHandler - returns disk info.
func (s *storageRESTServer) DiskInfoHandler(opts *DiskInfoOptions) (*DiskInfo, *grid.RemoteErr) {
	if !s.checkID(opts.DiskID) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}
	info, err := s.getStorage().DiskInfo(context.Background(), *opts)
	if err != nil {
		info.Error = err.Error()
	}
	return &info, nil
}

func (s *storageRESTServer) NSScannerHandler(ctx context.Context, params *nsScannerOptions, out chan<- *nsScannerResp) *grid.RemoteErr {
	if !s.checkID(params.DiskID) {
		return grid.NewRemoteErr(errDiskNotFound)
	}
	if params.Cache == nil {
		return grid.NewRemoteErrString("NSScannerHandler: provided cache is nil")
	}

	// Collect updates, stream them before the full cache is sent.
	updates := make(chan dataUsageEntry, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for update := range updates {
			resp := storageNSScannerRPC.NewResponse()
			resp.Update = &update
			out <- resp
		}
	}()
	ui, err := s.getStorage().NSScanner(ctx, *params.Cache, updates, madmin.HealScanMode(params.ScanMode), nil)
	wg.Wait()
	if err != nil {
		return grid.NewRemoteErr(err)
	}
	// Send final response.
	resp := storageNSScannerRPC.NewResponse()
	resp.Final = &ui
	out <- resp
	return nil
}

// MakeVolHandler - make a volume.
func (s *storageRESTServer) MakeVolHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	err := s.getStorage().MakeVol(r.Context(), volume)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// MakeVolBulkHandler - create multiple volumes as a bulk operation.
func (s *storageRESTServer) MakeVolBulkHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volumes := strings.Split(r.Form.Get(storageRESTVolumes), ",")
	err := s.getStorage().MakeVolBulk(r.Context(), volumes...)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// StatVolHandler - stat a volume.
func (s *storageRESTServer) StatVolHandler(params *grid.MSS) (*VolInfo, *grid.RemoteErr) {
	if !s.checkID(params.Get(storageRESTDiskID)) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}
	info, err := s.getStorage().StatVol(context.Background(), params.Get(storageRESTVolume))
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	return &info, nil
}

// AppendFileHandler - append data from the request to the file specified.
func (s *storageRESTServer) AppendFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)

	buf := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	err = s.getStorage().AppendFile(r.Context(), volume, filePath, buf)
	if err != nil {
		s.writeErrorResponse(w, err)
	}
}

// CreateFileHandler - copy the contents from the request.
func (s *storageRESTServer) CreateFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	origvolume := r.Form.Get(storageRESTOrigVolume)

	fileSizeStr := r.Form.Get(storageRESTLength)
	fileSize, err := strconv.Atoi(fileSizeStr)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	done, body := keepHTTPReqResponseAlive(w, r)
	done(s.getStorage().CreateFile(r.Context(), origvolume, volume, filePath, int64(fileSize), body))
}

// DeleteVersionHandler delete updated metadata.
func (s *storageRESTServer) DeleteVersionHandler(p *DeleteVersionHandlerParams) (np grid.NoPayload, gerr *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return np, grid.NewRemoteErr(errDiskNotFound)
	}
	volume := p.Volume
	filePath := p.FilePath
	forceDelMarker := p.ForceDelMarker

	opts := DeleteOptions{}
	err := s.getStorage().DeleteVersion(context.Background(), volume, filePath, p.FI, forceDelMarker, opts)
	return np, grid.NewRemoteErr(err)
}

// ReadVersionHandlerWS read metadata of versionID
func (s *storageRESTServer) ReadVersionHandlerWS(params *grid.MSS) (*FileInfo, *grid.RemoteErr) {
	if !s.checkID(params.Get(storageRESTDiskID)) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}
	origvolume := params.Get(storageRESTOrigVolume)
	volume := params.Get(storageRESTVolume)
	filePath := params.Get(storageRESTFilePath)
	versionID := params.Get(storageRESTVersionID)

	healing, err := strconv.ParseBool(params.Get(storageRESTHealing))
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}

	inclFreeVersions, err := strconv.ParseBool(params.Get(storageRESTInclFreeVersions))
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}

	fi, err := s.getStorage().ReadVersion(context.Background(), origvolume, volume, filePath, versionID, ReadOptions{
		InclFreeVersions: inclFreeVersions,
		ReadData:         false,
		Healing:          healing,
	})
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}
	return &fi, nil
}

// ReadVersionHandler read metadata of versionID
func (s *storageRESTServer) ReadVersionHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	origvolume := r.Form.Get(storageRESTOrigVolume)
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	versionID := r.Form.Get(storageRESTVersionID)
	healing, err := strconv.ParseBool(r.Form.Get(storageRESTHealing))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	inclFreeVersions, err := strconv.ParseBool(r.Form.Get(storageRESTInclFreeVersions))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	fi, err := s.getStorage().ReadVersion(r.Context(), origvolume, volume, filePath, versionID, ReadOptions{
		InclFreeVersions: inclFreeVersions,
		ReadData:         true,
		Healing:          healing,
	})
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	storageLogIf(r.Context(), msgp.Encode(w, &fi))
}

// WriteMetadataHandler rpc handler to write new updated metadata.
func (s *storageRESTServer) WriteMetadataHandler(p *MetadataHandlerParams) (np grid.NoPayload, gerr *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}

	volume := p.Volume
	filePath := p.FilePath
	origvolume := p.OrigVolume

	err := s.getStorage().WriteMetadata(context.Background(), origvolume, volume, filePath, p.FI)
	return np, grid.NewRemoteErr(err)
}

// UpdateMetadataHandler update new updated metadata.
func (s *storageRESTServer) UpdateMetadataHandler(p *MetadataHandlerParams) (grid.NoPayload, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}
	volume := p.Volume
	filePath := p.FilePath

	return grid.NewNPErr(s.getStorage().UpdateMetadata(context.Background(), volume, filePath, p.FI, p.UpdateOpts))
}

// CheckPartsHandler - check if a file parts exists.
func (s *storageRESTServer) CheckPartsHandler(ctx context.Context, p *CheckPartsHandlerParams, out chan<- *CheckPartsResp) *grid.RemoteErr {
	if !s.checkID(p.DiskID) {
		return grid.NewRemoteErr(errDiskNotFound)
	}
	volume := p.Volume
	filePath := p.FilePath

	resp, err := s.getStorage().CheckParts(ctx, volume, filePath, p.FI)
	if err != nil {
		return grid.NewRemoteErr(err)
	}
	out <- resp
	return grid.NewRemoteErr(err)
}

func (s *storageRESTServer) WriteAllHandler(p *WriteAllHandlerParams) (grid.NoPayload, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}

	volume := p.Volume
	filePath := p.FilePath

	return grid.NewNPErr(s.getStorage().WriteAll(context.Background(), volume, filePath, p.Buf))
}

// ReadAllHandler - read all the contents of a file.
func (s *storageRESTServer) ReadAllHandler(p *ReadAllHandlerParams) (*grid.Bytes, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}

	volume := p.Volume
	filePath := p.FilePath

	buf, err := s.getStorage().ReadAll(context.Background(), volume, filePath)
	return grid.NewBytesWith(buf), grid.NewRemoteErr(err)
}

// ReadXLHandler - read xl.meta for an object at path.
func (s *storageRESTServer) ReadXLHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)

	rf, err := s.getStorage().ReadXL(r.Context(), volume, filePath, true)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	storageLogIf(r.Context(), msgp.Encode(w, &rf))
}

// ReadXLHandlerWS - read xl.meta for an object at path.
func (s *storageRESTServer) ReadXLHandlerWS(params *grid.MSS) (*RawFileInfo, *grid.RemoteErr) {
	if !s.checkID(params.Get(storageRESTDiskID)) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}

	volume := params.Get(storageRESTVolume)
	filePath := params.Get(storageRESTFilePath)
	rf, err := s.getStorage().ReadXL(context.Background(), volume, filePath, false)
	if err != nil {
		return nil, grid.NewRemoteErr(err)
	}

	return &rf, nil
}

// ReadPartsHandler - read section of a file.
func (s *storageRESTServer) ReadPartsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)

	var preq ReadPartsReq
	if err := msgp.Decode(r.Body, &preq); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	done := keepHTTPResponseAlive(w)
	infos, err := s.getStorage().ReadParts(r.Context(), volume, preq.Paths...)
	done(nil)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	presp := &ReadPartsResp{Infos: infos}
	storageLogIf(r.Context(), msgp.Encode(w, presp))
}

// ReadFileHandler - read section of a file.
func (s *storageRESTServer) ReadFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	offset, err := strconv.Atoi(r.Form.Get(storageRESTOffset))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	length, err := strconv.Atoi(r.Form.Get(storageRESTLength))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	if offset < 0 || length < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}
	var verifier *BitrotVerifier
	if r.Form.Get(storageRESTBitrotAlgo) != "" {
		hashStr := r.Form.Get(storageRESTBitrotHash)
		var hash []byte
		hash, err = hex.DecodeString(hashStr)
		if err != nil {
			s.writeErrorResponse(w, err)
			return
		}
		verifier = NewBitrotVerifier(BitrotAlgorithmFromString(r.Form.Get(storageRESTBitrotAlgo)), hash)
	}
	buf := make([]byte, length)
	defer metaDataPoolPut(buf) // Reuse if we can.
	_, err = s.getStorage().ReadFile(r.Context(), volume, filePath, int64(offset), buf, verifier)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(buf)))
	w.Write(buf)
}

// ReadFileStreamHandler - read section of a file.
func (s *storageRESTServer) ReadFileStreamHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	offset, err := strconv.ParseInt(r.Form.Get(storageRESTOffset), 10, 64)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	length, err := strconv.ParseInt(r.Form.Get(storageRESTLength), 10, 64)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	rc, err := s.getStorage().ReadFileStream(r.Context(), volume, filePath, offset, length)
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}
	defer rc.Close()

	_, err = xioutil.Copy(w, rc)
	if !xnet.IsNetworkOrHostDown(err, true) { // do not need to log disconnected clients
		storageLogIf(r.Context(), err)
	}
}

// ListDirHandler - list a directory.
func (s *storageRESTServer) ListDirHandler(ctx context.Context, params *grid.MSS, out chan<- *ListDirResult) *grid.RemoteErr {
	if !s.checkID(params.Get(storageRESTDiskID)) {
		return grid.NewRemoteErr(errDiskNotFound)
	}
	volume := params.Get(storageRESTVolume)
	dirPath := params.Get(storageRESTDirPath)
	origvolume := params.Get(storageRESTOrigVolume)
	count, err := strconv.Atoi(params.Get(storageRESTCount))
	if err != nil {
		return grid.NewRemoteErr(err)
	}

	entries, err := s.getStorage().ListDir(ctx, origvolume, volume, dirPath, count)
	if err != nil {
		return grid.NewRemoteErr(err)
	}
	out <- &ListDirResult{Entries: entries}
	return nil
}

// DeleteFileHandler - delete a file.
func (s *storageRESTServer) DeleteFileHandler(p *DeleteFileHandlerParams) (grid.NoPayload, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}
	return grid.NewNPErr(s.getStorage().Delete(context.Background(), p.Volume, p.FilePath, p.Opts))
}

// DeleteVersionsHandler - delete a set of a versions.
func (s *storageRESTServer) DeleteVersionsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	volume := r.Form.Get(storageRESTVolume)
	totalVersions, err := strconv.Atoi(r.Form.Get(storageRESTTotalVersions))
	if err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	versions := make([]FileInfoVersions, totalVersions)
	decoder := msgpNewReader(r.Body)
	defer readMsgpReaderPoolPut(decoder)
	for i := range totalVersions {
		dst := &versions[i]
		if err := dst.DecodeMsg(decoder); err != nil {
			s.writeErrorResponse(w, err)
			return
		}
	}

	done := keepHTTPResponseAlive(w)
	opts := DeleteOptions{}
	errs := s.getStorage().DeleteVersions(r.Context(), volume, versions, opts)
	done(nil)

	dErrsResp := &DeleteVersionsErrsResp{Errs: make([]string, totalVersions)}
	for idx := range versions {
		if errs[idx] != nil {
			dErrsResp.Errs[idx] = errs[idx].Error()
		}
	}

	buf, _ := dErrsResp.MarshalMsg(nil)
	w.Write(buf)
}

// RenameDataHandler - renames a meta object and data dir to destination.
func (s *storageRESTServer) RenameDataHandler(p *RenameDataHandlerParams) (*RenameDataResp, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return nil, grid.NewRemoteErr(errDiskNotFound)
	}

	resp, err := s.getStorage().RenameData(context.Background(), p.SrcVolume, p.SrcPath, p.FI, p.DstVolume, p.DstPath, p.Opts)
	return &resp, grid.NewRemoteErr(err)
}

// RenameDataInlineHandler - renames a meta object and data dir to destination.
func (s *storageRESTServer) RenameDataInlineHandler(p *RenameDataInlineHandlerParams) (*RenameDataResp, *grid.RemoteErr) {
	defer p.Recycle()
	return s.RenameDataHandler(&p.RenameDataHandlerParams)
}

// RenameFileHandler - rename a file from source to destination
func (s *storageRESTServer) RenameFileHandler(p *RenameFileHandlerParams) (grid.NoPayload, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}
	return grid.NewNPErr(s.getStorage().RenameFile(context.Background(), p.SrcVolume, p.SrcFilePath, p.DstVolume, p.DstFilePath))
}

// RenamePartHandler - rename a multipart part from source to destination
func (s *storageRESTServer) RenamePartHandler(p *RenamePartHandlerParams) (grid.NoPayload, *grid.RemoteErr) {
	if !s.checkID(p.DiskID) {
		return grid.NewNPErr(errDiskNotFound)
	}
	return grid.NewNPErr(s.getStorage().RenamePart(context.Background(), p.SrcVolume, p.SrcFilePath, p.DstVolume, p.DstFilePath, p.Meta, p.SkipParent))
}

// CleanAbandonedDataHandler - Clean unused data directories.
func (s *storageRESTServer) CleanAbandonedDataHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	if volume == "" || filePath == "" {
		return // Ignore
	}
	keepHTTPResponseAlive(w)(s.getStorage().CleanAbandonedData(r.Context(), volume, filePath))
}

// closeNotifier is itself a ReadCloser that will notify when either an error occurs or
// the Close() function is called.
type closeNotifier struct {
	rc   io.ReadCloser
	done chan struct{}
}

func (c *closeNotifier) Read(p []byte) (n int, err error) {
	n, err = c.rc.Read(p)
	if err != nil {
		if c.done != nil {
			xioutil.SafeClose(c.done)
			c.done = nil
		}
	}
	return n, err
}

func (c *closeNotifier) Close() error {
	if c.done != nil {
		xioutil.SafeClose(c.done)
		c.done = nil
	}
	return c.rc.Close()
}

// keepHTTPReqResponseAlive can be used to avoid timeouts with long storage
// operations, such as bitrot verification or data usage scanning.
// Every 10 seconds a space character is sent.
// keepHTTPReqResponseAlive will wait for the returned body to be read before starting the ticker.
// The returned function should always be called to release resources.
// An optional error can be sent which will be picked as text only error,
// without its original type by the receiver.
// waitForHTTPResponse should be used to the receiving side.
func keepHTTPReqResponseAlive(w http.ResponseWriter, r *http.Request) (resp func(error), body io.ReadCloser) {
	bodyDoneCh := make(chan struct{})
	doneCh := make(chan error)
	ctx := r.Context()
	go func() {
		canWrite := true
		write := func(b []byte) {
			if canWrite {
				n, err := w.Write(b)
				if err != nil || n != len(b) {
					canWrite = false
				}
			}
		}
		// Wait for body to be read.
		select {
		case <-ctx.Done():
		case <-bodyDoneCh:
		case err := <-doneCh:
			if err != nil {
				write([]byte{1})
				write([]byte(err.Error()))
			} else {
				write([]byte{0})
			}
			xioutil.SafeClose(doneCh)
			return
		}
		defer xioutil.SafeClose(doneCh)
		// Initiate ticker after body has been read.
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// The done() might have been called
				// concurrently, check for it before we
				// write the filler byte.
				select {
				case err := <-doneCh:
					if err != nil {
						write([]byte{1})
						write([]byte(err.Error()))
					} else {
						write([]byte{0})
					}
					return
				default:
				}

				// Response not ready, write a filler byte.
				write([]byte{32})
				if canWrite {
					xhttp.Flush(w)
				}
			case err := <-doneCh:
				if err != nil {
					write([]byte{1})
					write([]byte(err.Error()))
				} else {
					write([]byte{0})
				}
				return
			}
		}
	}()
	return func(err error) {
		if doneCh == nil {
			return
		}

		// Indicate we are ready to write.
		doneCh <- err

		// Wait for channel to be closed so we don't race on writes.
		<-doneCh

		// Clear so we can be called multiple times without crashing.
		doneCh = nil
	}, &closeNotifier{rc: r.Body, done: bodyDoneCh}
}

// keepHTTPResponseAlive can be used to avoid timeouts with long storage
// operations, such as bitrot verification or data usage scanning.
// keepHTTPResponseAlive may NOT be used until the request body has been read,
// use keepHTTPReqResponseAlive instead.
// Every 10 seconds a space character is sent.
// The returned function should always be called to release resources.
// An optional error can be sent which will be picked as text only error,
// without its original type by the receiver.
// waitForHTTPResponse should be used to the receiving side.
func keepHTTPResponseAlive(w http.ResponseWriter) func(error) {
	doneCh := make(chan error)
	go func() {
		canWrite := true
		write := func(b []byte) {
			if canWrite {
				n, err := w.Write(b)
				if err != nil || n != len(b) {
					canWrite = false
				}
			}
		}
		defer xioutil.SafeClose(doneCh)
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// The done() might have been called
				// concurrently, check for it before we
				// write the filler byte.
				select {
				case err := <-doneCh:
					if err != nil {
						write([]byte{1})
						write([]byte(err.Error()))
					} else {
						write([]byte{0})
					}
					return
				default:
				}

				// Response not ready, write a filler byte.
				write([]byte{32})
				if canWrite {
					xhttp.Flush(w)
				}
			case err := <-doneCh:
				if err != nil {
					write([]byte{1})
					write([]byte(err.Error()))
				} else {
					write([]byte{0})
				}
				return
			}
		}
	}()
	return func(err error) {
		if doneCh == nil {
			return
		}
		// Indicate we are ready to write.
		doneCh <- err

		// Wait for channel to be closed so we don't race on writes.
		<-doneCh

		// Clear so we can be called multiple times without crashing.
		doneCh = nil
	}
}

// waitForHTTPResponse will wait for responses where keepHTTPResponseAlive
// has been used.
// The returned reader contains the payload.
func waitForHTTPResponse(respBody io.Reader) (io.Reader, error) {
	reader := bufio.NewReader(respBody)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		// Check if we have a response ready or a filler byte.
		switch b {
		case 0:
			return reader, nil
		case 1:
			errorText, err := io.ReadAll(reader)
			if err != nil {
				return nil, err
			}
			return nil, errors.New(string(errorText))
		case 32:
			continue
		default:
			return nil, fmt.Errorf("unexpected filler byte: %d", b)
		}
	}
}

// httpStreamResponse allows streaming a response, but still send an error.
type httpStreamResponse struct {
	done  chan error
	block chan []byte
	err   error
}

// Write part of the streaming response.
// Note that upstream errors are currently not forwarded, but may be in the future.
func (h *httpStreamResponse) Write(b []byte) (int, error) {
	if len(b) == 0 || h.err != nil {
		// Ignore 0 length blocks
		return 0, h.err
	}
	tmp := make([]byte, len(b))
	copy(tmp, b)
	h.block <- tmp
	return len(b), h.err
}

// CloseWithError will close the stream and return the specified error.
// This can be done several times, but only the first error will be sent.
// After calling this the stream should not be written to.
func (h *httpStreamResponse) CloseWithError(err error) {
	if h.done == nil {
		return
	}
	h.done <- err
	h.err = err
	// Indicates that the response is done.
	<-h.done
	h.done = nil
}

// streamHTTPResponse can be used to avoid timeouts with long storage
// operations, such as bitrot verification or data usage scanning.
// Every 10 seconds a space character is sent.
// The returned function should always be called to release resources.
// An optional error can be sent which will be picked as text only error,
// without its original type by the receiver.
// waitForHTTPStream should be used to the receiving side.
func streamHTTPResponse(w http.ResponseWriter) *httpStreamResponse {
	doneCh := make(chan error)
	blockCh := make(chan []byte)
	h := httpStreamResponse{done: doneCh, block: blockCh}
	go func() {
		canWrite := true
		write := func(b []byte) {
			if canWrite {
				n, err := w.Write(b)
				if err != nil || n != len(b) {
					canWrite = false
				}
			}
		}

		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Response not ready, write a filler byte.
				write([]byte{32})
				if canWrite {
					xhttp.Flush(w)
				}
			case err := <-doneCh:
				if err != nil {
					write([]byte{1})
					write([]byte(err.Error()))
				} else {
					write([]byte{0})
				}
				xioutil.SafeClose(doneCh)
				return
			case block := <-blockCh:
				var tmp [5]byte
				tmp[0] = 2
				binary.LittleEndian.PutUint32(tmp[1:], uint32(len(block)))
				write(tmp[:])
				write(block)
				if canWrite {
					xhttp.Flush(w)
				}
			}
		}
	}()
	return &h
}

var poolBuf8k = bpool.Pool[*[]byte]{
	New: func() *[]byte {
		b := make([]byte, 8192)
		return &b
	},
}

// waitForHTTPStream will wait for responses where
// streamHTTPResponse has been used.
// The returned reader contains the payload and must be closed if no error is returned.
func waitForHTTPStream(respBody io.ReadCloser, w io.Writer) error {
	var tmp [1]byte
	// 8K copy buffer, reused for less allocs...
	bufp := poolBuf8k.Get()
	buf := *bufp
	defer poolBuf8k.Put(bufp)

	for {
		_, err := io.ReadFull(respBody, tmp[:])
		if err != nil {
			return err
		}
		// Check if we have a response ready or a filler byte.
		switch tmp[0] {
		case 0:
			// 0 is unbuffered, copy the rest.
			_, err := io.CopyBuffer(w, respBody, buf)
			if err == io.EOF {
				return nil
			}
			return err
		case 1:
			errorText, err := io.ReadAll(respBody)
			if err != nil {
				return err
			}
			return errors.New(string(errorText))
		case 2:
			// Block of data
			var tmp [4]byte
			_, err := io.ReadFull(respBody, tmp[:])
			if err != nil {
				return err
			}
			length := binary.LittleEndian.Uint32(tmp[:])
			n, err := io.CopyBuffer(w, io.LimitReader(respBody, int64(length)), buf)
			if err != nil {
				return err
			}
			if n != int64(length) {
				return io.ErrUnexpectedEOF
			}
			continue
		case 32:
			continue
		default:
			return fmt.Errorf("unexpected filler byte: %d", tmp[0])
		}
	}
}

// VerifyFileHandler - Verify all part of file for bitrot errors.
func (s *storageRESTServer) VerifyFileHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)

	if r.ContentLength < 0 {
		s.writeErrorResponse(w, errInvalidArgument)
		return
	}

	var fi FileInfo
	if err := msgp.Decode(r.Body, &fi); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	done := keepHTTPResponseAlive(w)
	resp, err := s.getStorage().VerifyFile(r.Context(), volume, filePath, fi)
	done(err)
	if err != nil {
		return
	}

	buf, _ := resp.MarshalMsg(nil)
	w.Write(buf)
}

func checkDiskFatalErrs(errs []error) error {
	// This returns a common error if all errors are
	// same errors, then there is no point starting
	// the server.
	if countErrs(errs, errUnsupportedDisk) == len(errs) {
		return errUnsupportedDisk
	}

	if countErrs(errs, errDiskAccessDenied) == len(errs) {
		return errDiskAccessDenied
	}

	if countErrs(errs, errFileAccessDenied) == len(errs) {
		return errFileAccessDenied
	}

	if countErrs(errs, errDiskNotDir) == len(errs) {
		return errDiskNotDir
	}

	if countErrs(errs, errFaultyDisk) == len(errs) {
		return errFaultyDisk
	}

	if countErrs(errs, errXLBackend) == len(errs) {
		return errXLBackend
	}

	return nil
}

// A single function to write certain errors to be fatal
// or informative based on the `exit` flag, please look
// at each implementation of error for added hints.
//
// FIXME: This is an unusual function but serves its purpose for
// now, need to revisit the overall erroring structure here.
// Do not like it :-(
func logFatalErrs(err error, endpoint Endpoint, exit bool) {
	switch {
	case errors.Is(err, errXLBackend):
		logger.Fatal(config.ErrInvalidXLValue(err), "Unable to initialize backend")
	case errors.Is(err, errUnsupportedDisk):
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Drive '%s' does not support O_DIRECT flags, MinIO erasure coding requires filesystems with O_DIRECT support", endpoint.Path)
		} else {
			hint = "Drives do not support O_DIRECT flags, MinIO erasure coding requires filesystems with O_DIRECT support"
		}
		logger.Fatal(config.ErrUnsupportedBackend(err).Hint("%s", hint), "Unable to initialize backend")
	case errors.Is(err, errDiskNotDir):
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Drive '%s' is not a directory, MinIO erasure coding needs a directory", endpoint.Path)
		} else {
			hint = "Drives are not directories, MinIO erasure coding needs directories"
		}
		logger.Fatal(config.ErrUnableToWriteInBackend(err).Hint("%s", hint), "Unable to initialize backend")
	case errors.Is(err, errDiskAccessDenied):
		// Show a descriptive error with a hint about how to fix it.
		var username string
		if u, err := user.Current(); err == nil {
			username = u.Username
		} else {
			username = "<your-username>"
		}
		var hint string
		if endpoint.URL != nil {
			hint = fmt.Sprintf("Run the following command to add write permissions: `sudo chown -R %s %s && sudo chmod u+rxw %s`",
				username, endpoint.Path, endpoint.Path)
		} else {
			hint = fmt.Sprintf("Run the following command to add write permissions: `sudo chown -R %s. <path> && sudo chmod u+rxw <path>`", username)
		}
		if !exit {
			storageLogOnceIf(GlobalContext, fmt.Errorf("Drive is not writable %s, %s", endpoint, hint), "log-fatal-errs")
		} else {
			logger.Fatal(config.ErrUnableToWriteInBackend(err).Hint("%s", hint), "Unable to initialize backend")
		}
	case errors.Is(err, errFaultyDisk):
		if !exit {
			storageLogOnceIf(GlobalContext, fmt.Errorf("Drive is faulty at %s, please replace the drive - drive will be offline", endpoint), "log-fatal-errs")
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	case errors.Is(err, errDiskFull):
		if !exit {
			storageLogOnceIf(GlobalContext, fmt.Errorf("Drive is already full at %s, incoming I/O will fail - drive will be offline", endpoint), "log-fatal-errs")
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	case errors.Is(err, errInconsistentDisk):
		if exit {
			logger.Fatal(err, "Unable to initialize backend")
		}
	default:
		if !exit {
			storageLogOnceIf(GlobalContext, fmt.Errorf("Drive %s returned an unexpected error: %w, please investigate - drive will be offline", endpoint, err), "log-fatal-errs")
		} else {
			logger.Fatal(err, "Unable to initialize backend")
		}
	}
}

// StatInfoFile returns file stat info.
func (s *storageRESTServer) StatInfoFile(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	volume := r.Form.Get(storageRESTVolume)
	filePath := r.Form.Get(storageRESTFilePath)
	glob := r.Form.Get(storageRESTGlob)
	done := keepHTTPResponseAlive(w)
	stats, err := s.getStorage().StatInfoFile(r.Context(), volume, filePath, glob == "true")
	done(err)
	if err != nil {
		return
	}
	for _, si := range stats {
		msgp.Encode(w, &si)
	}
}

func (s *storageRESTServer) DeleteBulkHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}

	var req DeleteBulkReq
	mr := msgpNewReader(r.Body)
	defer readMsgpReaderPoolPut(mr)

	if err := req.DecodeMsg(mr); err != nil {
		s.writeErrorResponse(w, err)
		return
	}

	volume := r.Form.Get(storageRESTVolume)
	keepHTTPResponseAlive(w)(s.getStorage().DeleteBulk(r.Context(), volume, req.Paths...))
}

// ReadMultiple returns multiple files
func (s *storageRESTServer) ReadMultiple(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	rw := streamHTTPResponse(w)
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			rw.CloseWithError(fmt.Errorf("panic: %v", r))
		}
	}()

	var req ReadMultipleReq
	mr := msgpNewReader(r.Body)
	defer readMsgpReaderPoolPut(mr)
	err := req.DecodeMsg(mr)
	if err != nil {
		rw.CloseWithError(err)
		return
	}

	mw := msgp.NewWriter(rw)
	responses := make(chan ReadMultipleResp, len(req.Files))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resp := range responses {
			err := resp.EncodeMsg(mw)
			if err != nil {
				rw.CloseWithError(err)
				return
			}
			mw.Flush()
		}
	}()
	err = s.getStorage().ReadMultiple(r.Context(), req, responses)
	wg.Wait()
	rw.CloseWithError(err)
}

// globalLocalSetDrives is used for local drive as well as remote REST
// API caller for other nodes to talk to this node.
//
// Any updates to this must be serialized via globalLocalDrivesMu (locker)
var globalLocalSetDrives [][][]StorageAPI

// registerStorageRESTHandlers - register storage rpc router.
func registerStorageRESTHandlers(router *mux.Router, endpointServerPools EndpointServerPools, gm *grid.Manager) {
	h := func(f http.HandlerFunc) http.HandlerFunc {
		return collectInternodeStats(httpTraceHdrs(f))
	}

	globalLocalDrivesMap = make(map[string]StorageAPI)
	globalLocalSetDrives = make([][][]StorageAPI, len(endpointServerPools))
	for pool := range globalLocalSetDrives {
		globalLocalSetDrives[pool] = make([][]StorageAPI, endpointServerPools[pool].SetCount)
		for set := range globalLocalSetDrives[pool] {
			globalLocalSetDrives[pool][set] = make([]StorageAPI, endpointServerPools[pool].DrivesPerSet)
		}
	}
	for _, serverPool := range endpointServerPools {
		for _, endpoint := range serverPool.Endpoints {
			if !endpoint.IsLocal {
				continue
			}

			server := &storageRESTServer{
				endpoint: endpoint,
			}

			subrouter := router.PathPrefix(path.Join(storageRESTPrefix, endpoint.Path)).Subrouter()

			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodHealth).HandlerFunc(h(server.HealthHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodAppendFile).HandlerFunc(h(server.AppendFileHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodCreateFile).HandlerFunc(h(server.CreateFileHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodDeleteVersions).HandlerFunc(h(server.DeleteVersionsHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodVerifyFile).HandlerFunc(h(server.VerifyFileHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodStatInfoFile).HandlerFunc(h(server.StatInfoFile))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodReadMultiple).HandlerFunc(h(server.ReadMultiple))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodCleanAbandoned).HandlerFunc(h(server.CleanAbandonedDataHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodDeleteBulk).HandlerFunc(h(server.DeleteBulkHandler))
			subrouter.Methods(http.MethodPost).Path(storageRESTVersionPrefix + storageRESTMethodReadParts).HandlerFunc(h(server.ReadPartsHandler))

			subrouter.Methods(http.MethodGet).Path(storageRESTVersionPrefix + storageRESTMethodReadFileStream).HandlerFunc(h(server.ReadFileStreamHandler))
			subrouter.Methods(http.MethodGet).Path(storageRESTVersionPrefix + storageRESTMethodReadVersion).HandlerFunc(h(server.ReadVersionHandler))
			subrouter.Methods(http.MethodGet).Path(storageRESTVersionPrefix + storageRESTMethodReadXL).HandlerFunc(h(server.ReadXLHandler))
			subrouter.Methods(http.MethodGet).Path(storageRESTVersionPrefix + storageRESTMethodReadFile).HandlerFunc(h(server.ReadFileHandler))

			logger.FatalIf(storageListDirRPC.RegisterNoInput(gm, server.ListDirHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageReadAllRPC.Register(gm, server.ReadAllHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageWriteAllRPC.Register(gm, server.WriteAllHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageRenameFileRPC.Register(gm, server.RenameFileHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageRenamePartRPC.Register(gm, server.RenamePartHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageRenameDataRPC.Register(gm, server.RenameDataHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageRenameDataInlineRPC.Register(gm, server.RenameDataInlineHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageDeleteFileRPC.Register(gm, server.DeleteFileHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageCheckPartsRPC.RegisterNoInput(gm, server.CheckPartsHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageReadVersionRPC.Register(gm, server.ReadVersionHandlerWS, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageWriteMetadataRPC.Register(gm, server.WriteMetadataHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageUpdateMetadataRPC.Register(gm, server.UpdateMetadataHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageDeleteVersionRPC.Register(gm, server.DeleteVersionHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageReadXLRPC.Register(gm, server.ReadXLHandlerWS, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageNSScannerRPC.RegisterNoInput(gm, server.NSScannerHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageDiskInfoRPC.Register(gm, server.DiskInfoHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(storageStatVolRPC.Register(gm, server.StatVolHandler, endpoint.Path), "unable to register handler")
			logger.FatalIf(gm.RegisterStreamingHandler(grid.HandlerWalkDir, grid.StreamHandler{
				Subroute:    endpoint.Path,
				Handle:      server.WalkDirHandler,
				OutCapacity: 1,
			}), "unable to register handler")

			createStorage := func(endpoint Endpoint) bool {
				xl, err := newXLStorage(endpoint, false)
				if err != nil {
					// if supported errors don't fail, we proceed to
					// printing message and moving forward.
					if errors.Is(err, errDriveIsRoot) {
						err = fmt.Errorf("major: %v: minor: %v: %w", xl.major, xl.minor, err)
					}
					logFatalErrs(err, endpoint, false)
					return false
				}
				storage := newXLStorageDiskIDCheck(xl, true)
				storage.SetDiskID(xl.diskID)
				// We do not have to do SetFormatData() since 'xl'
				// already captures formatData cached.

				globalLocalDrivesMu.Lock()
				defer globalLocalDrivesMu.Unlock()

				globalLocalDrivesMap[endpoint.String()] = storage
				globalLocalSetDrives[endpoint.PoolIdx][endpoint.SetIdx][endpoint.DiskIdx] = storage
				return true
			}

			if createStorage(endpoint) {
				continue
			}

			// Start async goroutine to create storage.
			go func(endpoint Endpoint) {
				for {
					time.Sleep(3 * time.Second)
					if createStorage(endpoint) {
						return
					}
				}
			}(endpoint)
		}
	}
}
