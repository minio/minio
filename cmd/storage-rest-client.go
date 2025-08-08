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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/grid"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/rest"
	xnet "github.com/minio/pkg/v3/net"
	xbufio "github.com/philhofer/fwd"
	"github.com/tinylib/msgp/msgp"
)

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	if nerr, ok := err.(*rest.NetworkError); ok {
		if down := xnet.IsNetworkOrHostDown(nerr.Err, false); down {
			return true
		}
		if errors.Is(nerr.Err, rest.ErrClientClosed) {
			return true
		}
	}
	if errors.Is(err, grid.ErrDisconnected) {
		return true
	}
	// More corner cases suitable for storage REST API
	switch {
	// A peer node can be in shut down phase and proactively
	// return 503 server closed error, consider it as an offline node
	case strings.Contains(err.Error(), http.ErrServerClosed.Error()):
		return true
	// Corner case, the server closed the connection with a keep-alive timeout
	// some requests are not retried internally, such as POST request with written body
	case strings.Contains(err.Error(), "server closed idle connection"):
		return true
	}

	return false
}

// Converts network error to storageErr. This function is
// written so that the storageAPI errors are consistent
// across network disks.
func toStorageErr(err error) error {
	if err == nil {
		return nil
	}

	if isNetworkError(err) {
		return errDiskNotFound
	}

	switch err.Error() {
	case errUploadIDNotFound.Error():
		return errUploadIDNotFound
	case errFaultyDisk.Error():
		return errFaultyDisk
	case errFaultyRemoteDisk.Error():
		return errFaultyRemoteDisk
	case errFileCorrupt.Error():
		return errFileCorrupt
	case errUnexpected.Error():
		return errUnexpected
	case errDiskFull.Error():
		return errDiskFull
	case errVolumeNotFound.Error():
		return errVolumeNotFound
	case errVolumeExists.Error():
		return errVolumeExists
	case errFileNotFound.Error():
		return errFileNotFound
	case errFileVersionNotFound.Error():
		return errFileVersionNotFound
	case errFileNameTooLong.Error():
		return errFileNameTooLong
	case errFileAccessDenied.Error():
		return errFileAccessDenied
	case errPathNotFound.Error():
		return errPathNotFound
	case errIsNotRegular.Error():
		return errIsNotRegular
	case errVolumeNotEmpty.Error():
		return errVolumeNotEmpty
	case errVolumeAccessDenied.Error():
		return errVolumeAccessDenied
	case errCorruptedFormat.Error():
		return errCorruptedFormat
	case errCorruptedBackend.Error():
		return errCorruptedBackend
	case errUnformattedDisk.Error():
		return errUnformattedDisk
	case errInvalidAccessKeyID.Error():
		return errInvalidAccessKeyID
	case errAuthentication.Error():
		return errAuthentication
	case errRPCAPIVersionUnsupported.Error():
		return errRPCAPIVersionUnsupported
	case errServerTimeMismatch.Error():
		return errServerTimeMismatch
	case io.EOF.Error():
		return io.EOF
	case io.ErrUnexpectedEOF.Error():
		return io.ErrUnexpectedEOF
	case errDiskStale.Error():
		return errDiskNotFound
	case errDiskNotFound.Error():
		return errDiskNotFound
	case errMaxVersionsExceeded.Error():
		return errMaxVersionsExceeded
	case errInconsistentDisk.Error():
		return errInconsistentDisk
	case errDriveIsRoot.Error():
		return errDriveIsRoot
	case errDiskOngoingReq.Error():
		return errDiskOngoingReq
	case grid.ErrUnknownHandler.Error():
		return errInconsistentDisk
	case grid.ErrDisconnected.Error():
		return errDiskNotFound
	}
	return err
}

// Abstracts a remote disk.
type storageRESTClient struct {
	endpoint   Endpoint
	restClient *rest.Client
	gridConn   *grid.Subroute
	diskID     atomic.Pointer[string]

	diskInfoCache *cachevalue.Cache[DiskInfo]
}

// Retrieve location indexes.
func (client *storageRESTClient) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return client.endpoint.PoolIdx, client.endpoint.SetIdx, client.endpoint.DiskIdx
}

// Wrapper to restClient.CallWithMethod to handle network errors, in case of network error the connection is disconnected
// and a healthcheck routine gets invoked that would reconnect.
func (client *storageRESTClient) callGet(ctx context.Context, rpcMethod string, values url.Values, body io.Reader, length int64) (io.ReadCloser, error) {
	if values == nil {
		values = make(url.Values)
	}
	values.Set(storageRESTDiskID, *client.diskID.Load())
	respBody, err := client.restClient.CallWithHTTPMethod(ctx, http.MethodGet, rpcMethod, values, body, length)
	if err != nil {
		return nil, toStorageErr(err)
	}
	return respBody, nil
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is disconnected
// and a healthcheck routine gets invoked that would reconnect.
func (client *storageRESTClient) call(ctx context.Context, rpcMethod string, values url.Values, body io.Reader, length int64) (io.ReadCloser, error) {
	if values == nil {
		values = make(url.Values)
	}
	values.Set(storageRESTDiskID, *client.diskID.Load())
	respBody, err := client.restClient.CallWithHTTPMethod(ctx, http.MethodPost, rpcMethod, values, body, length)
	if err != nil {
		return nil, toStorageErr(err)
	}
	return respBody, nil
}

// Stringer provides a canonicalized representation of network device.
func (client *storageRESTClient) String() string {
	return client.endpoint.String()
}

// IsOnline - returns whether client failed to connect or not.
func (client *storageRESTClient) IsOnline() bool {
	return client.restClient.IsOnline() || client.IsOnlineWS()
}

// IsOnlineWS - returns whether websocket client failed to connect or not.
func (client *storageRESTClient) IsOnlineWS() bool {
	return client.gridConn.State() == grid.StateConnected
}

// LastConn - returns when the disk is seen to be connected the last time
func (client *storageRESTClient) LastConn() time.Time {
	return client.restClient.LastConn()
}

func (client *storageRESTClient) IsLocal() bool {
	return false
}

func (client *storageRESTClient) Hostname() string {
	return client.endpoint.Host
}

func (client *storageRESTClient) Endpoint() Endpoint {
	return client.endpoint
}

func (client *storageRESTClient) Healing() *healingTracker {
	// This call is not implemented for remote client on purpose.
	// healing tracker is always for local disks.
	return nil
}

func (client *storageRESTClient) NSScanner(ctx context.Context, cache dataUsageCache, updates chan<- dataUsageEntry, scanMode madmin.HealScanMode, _ func() bool) (dataUsageCache, error) {
	defer xioutil.SafeClose(updates)

	st, err := storageNSScannerRPC.Call(ctx, client.gridConn, &nsScannerOptions{
		DiskID:   *client.diskID.Load(),
		ScanMode: int(scanMode),
		Cache:    &cache,
	})
	if err != nil {
		return cache, toStorageErr(err)
	}
	var final *dataUsageCache
	err = st.Results(func(resp *nsScannerResp) error {
		if resp.Update != nil {
			select {
			case <-ctx.Done():
			case updates <- *resp.Update:
			}
		}
		if resp.Final != nil {
			final = resp.Final
		}
		// We can't reuse the response since it is sent upstream.
		return nil
	})
	if err != nil {
		return cache, toStorageErr(err)
	}
	if final == nil {
		return cache, errors.New("no final cache")
	}
	return *final, nil
}

func (client *storageRESTClient) GetDiskID() (string, error) {
	if !client.IsOnlineWS() {
		// make sure to check if the disk is offline, since the underlying
		// value is cached we should attempt to invalidate it if such calls
		// were attempted. This can lead to false success under certain conditions
		// - this change attempts to avoid stale information if the underlying
		// transport is already down.
		return "", errDiskNotFound
	}

	// This call should never be over the network, this is always
	// a cached value - caller should make sure to use this
	// function on a fresh disk or make sure to look at the error
	// from a different networked call to validate the GetDiskID()
	return *client.diskID.Load(), nil
}

func (client *storageRESTClient) SetDiskID(id string) {
	client.diskID.Store(&id)
}

func (client *storageRESTClient) DiskInfo(ctx context.Context, opts DiskInfoOptions) (info DiskInfo, err error) {
	if !client.IsOnlineWS() {
		// make sure to check if the disk is offline, since the underlying
		// value is cached we should attempt to invalidate it if such calls
		// were attempted. This can lead to false success under certain conditions
		// - this change attempts to avoid stale information if the underlying
		// transport is already down.
		return info, errDiskNotFound
	}

	// if 'NoOp' we do not cache the value.
	if opts.NoOp {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		opts.DiskID = *client.diskID.Load()

		infop, err := storageDiskInfoRPC.Call(ctx, client.gridConn, &opts)
		if err != nil {
			return info, toStorageErr(err)
		}
		info = *infop
		if info.Error != "" {
			return info, toStorageErr(errors.New(info.Error))
		}
		return info, nil
	} // In all other cases cache the value upto 1sec.

	client.diskInfoCache.InitOnce(time.Second, cachevalue.Opts{},
		func(ctx context.Context) (info DiskInfo, err error) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			nopts := DiskInfoOptions{DiskID: *client.diskID.Load(), Metrics: true}
			infop, err := storageDiskInfoRPC.Call(ctx, client.gridConn, &nopts)
			if err != nil {
				return info, toStorageErr(err)
			}
			info = *infop
			if info.Error != "" {
				return info, toStorageErr(errors.New(info.Error))
			}
			return info, nil
		},
	)

	return client.diskInfoCache.GetWithCtx(ctx)
}

// MakeVolBulk - create multiple volumes in a bulk operation.
func (client *storageRESTClient) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	return errInvalidArgument
}

// MakeVol - create a volume on a remote disk.
func (client *storageRESTClient) MakeVol(ctx context.Context, volume string) (err error) {
	return errInvalidArgument
}

// ListVols - List all volumes on a remote disk.
func (client *storageRESTClient) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	return nil, errInvalidArgument
}

// StatVol - get volume info over the network.
func (client *storageRESTClient) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	v, err := storageStatVolRPC.Call(ctx, client.gridConn, grid.NewMSSWith(map[string]string{
		storageRESTDiskID: *client.diskID.Load(),
		storageRESTVolume: volume,
	}))
	if err != nil {
		return vol, toStorageErr(err)
	}
	vol = *v
	// Performs shallow copy, so we can reuse.
	storageStatVolRPC.PutResponse(v)
	return vol, nil
}

// DeleteVol - Deletes a volume over the network.
func (client *storageRESTClient) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	return errInvalidArgument
}

// AppendFile - append to a file.
func (client *storageRESTClient) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	reader := bytes.NewReader(buf)
	respBody, err := client.call(ctx, storageRESTMethodAppendFile, values, reader, -1)
	defer xhttp.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTLength, strconv.Itoa(int(size)))
	values.Set(storageRESTOrigVolume, origvolume)

	respBody, err := client.call(ctx, storageRESTMethodCreateFile, values, io.NopCloser(reader), size)
	defer xhttp.DrainBody(respBody)
	if err != nil {
		return err
	}
	_, err = waitForHTTPResponse(respBody)
	return toStorageErr(err)
}

func (client *storageRESTClient) WriteMetadata(ctx context.Context, origvolume, volume, path string, fi FileInfo) error {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err := storageWriteMetadataRPC.Call(ctx, client.gridConn, &MetadataHandlerParams{
		DiskID:     *client.diskID.Load(),
		OrigVolume: origvolume,
		Volume:     volume,
		FilePath:   path,
		FI:         fi,
	})
	return toStorageErr(err)
}

func (client *storageRESTClient) UpdateMetadata(ctx context.Context, volume, path string, fi FileInfo, opts UpdateMetadataOpts) error {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err := storageUpdateMetadataRPC.Call(ctx, client.gridConn, &MetadataHandlerParams{
		DiskID:     *client.diskID.Load(),
		Volume:     volume,
		FilePath:   path,
		UpdateOpts: opts,
		FI:         fi,
	})
	return toStorageErr(err)
}

func (client *storageRESTClient) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool, opts DeleteOptions) (err error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err = storageDeleteVersionRPC.Call(ctx, client.gridConn, &DeleteVersionHandlerParams{
		DiskID:         *client.diskID.Load(),
		Volume:         volume,
		FilePath:       path,
		ForceDelMarker: forceDelMarker,
		FI:             fi,
		Opts:           opts,
	})
	return toStorageErr(err)
}

// WriteAll - write all data to a file.
func (client *storageRESTClient) WriteAll(ctx context.Context, volume string, path string, b []byte) error {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err := storageWriteAllRPC.Call(ctx, client.gridConn, &WriteAllHandlerParams{
		DiskID:   *client.diskID.Load(),
		Volume:   volume,
		FilePath: path,
		Buf:      b,
	})
	return toStorageErr(err)
}

// CheckParts - stat all file parts.
func (client *storageRESTClient) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) (*CheckPartsResp, error) {
	var resp *CheckPartsResp
	st, err := storageCheckPartsRPC.Call(ctx, client.gridConn, &CheckPartsHandlerParams{
		DiskID:   *client.diskID.Load(),
		Volume:   volume,
		FilePath: path,
		FI:       fi,
	})
	if err != nil {
		return nil, toStorageErr(err)
	}
	err = st.Results(func(r *CheckPartsResp) error {
		resp = r
		return nil
	})
	return resp, toStorageErr(err)
}

// RenameData - rename source path to destination path atomically, metadata and data file.
func (client *storageRESTClient) RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo,
	dstVolume, dstPath string, opts RenameOptions,
) (res RenameDataResp, err error) {
	params := RenameDataHandlerParams{
		DiskID:    *client.diskID.Load(),
		SrcVolume: srcVolume,
		SrcPath:   srcPath,
		DstPath:   dstPath,
		DstVolume: dstVolume,
		FI:        fi,
		Opts:      opts,
	}
	var resp *RenameDataResp
	if fi.Data == nil {
		resp, err = storageRenameDataRPC.Call(ctx, client.gridConn, &params)
	} else {
		resp, err = storageRenameDataInlineRPC.Call(ctx, client.gridConn, &RenameDataInlineHandlerParams{params})
	}
	if err != nil {
		return res, toStorageErr(err)
	}

	defer storageRenameDataRPC.PutResponse(resp)
	return *resp, nil
}

// where we keep old *Readers
var readMsgpReaderPool = bpool.Pool[*msgp.Reader]{New: func() *msgp.Reader { return &msgp.Reader{} }}

// mspNewReader returns a *Reader that reads from the provided reader.
// The reader will be buffered.
// Return with readMsgpReaderPoolPut when done.
func msgpNewReader(r io.Reader) *msgp.Reader {
	p := readMsgpReaderPool.Get()
	if p.R == nil {
		p.R = xbufio.NewReaderSize(r, 32<<10)
	} else {
		p.R.Reset(r)
	}
	return p
}

// readMsgpReaderPoolPut can be used to reuse a *msgp.Reader.
func readMsgpReaderPoolPut(r *msgp.Reader) {
	if r != nil {
		readMsgpReaderPool.Put(r)
	}
}

func (client *storageRESTClient) ReadVersion(ctx context.Context, origvolume, volume, path, versionID string, opts ReadOptions) (fi FileInfo, err error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	// Use websocket when not reading data.
	if !opts.ReadData {
		resp, err := storageReadVersionRPC.Call(ctx, client.gridConn, grid.NewMSSWith(map[string]string{
			storageRESTDiskID:           *client.diskID.Load(),
			storageRESTOrigVolume:       origvolume,
			storageRESTVolume:           volume,
			storageRESTFilePath:         path,
			storageRESTVersionID:        versionID,
			storageRESTInclFreeVersions: strconv.FormatBool(opts.InclFreeVersions),
			storageRESTHealing:          strconv.FormatBool(opts.Healing),
		}))
		if err != nil {
			return fi, toStorageErr(err)
		}
		return *resp, nil
	}

	values := make(url.Values)
	values.Set(storageRESTOrigVolume, origvolume)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTVersionID, versionID)
	values.Set(storageRESTInclFreeVersions, strconv.FormatBool(opts.InclFreeVersions))
	values.Set(storageRESTHealing, strconv.FormatBool(opts.Healing))

	respBody, err := client.callGet(ctx, storageRESTMethodReadVersion, values, nil, -1)
	if err != nil {
		return fi, err
	}
	defer xhttp.DrainBody(respBody)

	dec := msgpNewReader(respBody)
	defer readMsgpReaderPoolPut(dec)

	err = fi.DecodeMsg(dec)
	return fi, err
}

// ReadXL - reads all contents of xl.meta of a file.
func (client *storageRESTClient) ReadXL(ctx context.Context, volume string, path string, readData bool) (rf RawFileInfo, err error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	// Use websocket when not reading data.
	if !readData {
		resp, err := storageReadXLRPC.Call(ctx, client.gridConn, grid.NewMSSWith(map[string]string{
			storageRESTDiskID:   *client.diskID.Load(),
			storageRESTVolume:   volume,
			storageRESTFilePath: path,
		}))
		if err != nil {
			return rf, toStorageErr(err)
		}
		return *resp, nil
	}

	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	respBody, err := client.callGet(ctx, storageRESTMethodReadXL, values, nil, -1)
	if err != nil {
		return rf, toStorageErr(err)
	}
	defer xhttp.DrainBody(respBody)

	dec := msgpNewReader(respBody)
	defer readMsgpReaderPoolPut(dec)

	err = rf.DecodeMsg(dec)
	return rf, err
}

// ReadAll - reads all contents of a file.
func (client *storageRESTClient) ReadAll(ctx context.Context, volume string, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	gridBytes, err := storageReadAllRPC.Call(ctx, client.gridConn, &ReadAllHandlerParams{
		DiskID:   *client.diskID.Load(),
		Volume:   volume,
		FilePath: path,
	})
	if err != nil {
		return nil, toStorageErr(err)
	}

	return *gridBytes, nil
}

// ReadFileStream - returns a reader for the requested file.
func (client *storageRESTClient) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTOffset, strconv.Itoa(int(offset)))
	values.Set(storageRESTLength, strconv.Itoa(int(length)))

	respBody, err := client.callGet(ctx, storageRESTMethodReadFileStream, values, nil, -1)
	if err != nil {
		return nil, toStorageErr(err)
	}
	return respBody, nil
}

// ReadFile - reads section of a file.
func (client *storageRESTClient) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (int64, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTOffset, strconv.Itoa(int(offset)))
	values.Set(storageRESTLength, strconv.Itoa(len(buf)))
	if verifier != nil {
		values.Set(storageRESTBitrotAlgo, verifier.algorithm.String())
		values.Set(storageRESTBitrotHash, hex.EncodeToString(verifier.sum))
	} else {
		values.Set(storageRESTBitrotAlgo, "")
		values.Set(storageRESTBitrotHash, "")
	}
	respBody, err := client.callGet(ctx, storageRESTMethodReadFile, values, nil, -1)
	if err != nil {
		return 0, err
	}
	defer xhttp.DrainBody(respBody)
	n, err := io.ReadFull(respBody, buf)
	return int64(n), toStorageErr(err)
}

// ListDir - lists a directory.
func (client *storageRESTClient) ListDir(ctx context.Context, origvolume, volume, dirPath string, count int) (entries []string, err error) {
	values := grid.NewMSS()
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTCount, strconv.Itoa(count))
	values.Set(storageRESTOrigVolume, origvolume)
	values.Set(storageRESTDiskID, *client.diskID.Load())

	st, err := storageListDirRPC.Call(ctx, client.gridConn, values)
	if err != nil {
		return nil, toStorageErr(err)
	}
	err = st.Results(func(resp *ListDirResult) error {
		entries = resp.Entries
		return nil
	})
	return entries, toStorageErr(err)
}

// DeleteFile - deletes a file.
func (client *storageRESTClient) Delete(ctx context.Context, volume string, path string, deleteOpts DeleteOptions) error {
	if !deleteOpts.Immediate {
		// add deadlines for all non-immediate purges
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
		defer cancel()
	}

	_, err := storageDeleteFileRPC.Call(ctx, client.gridConn, &DeleteFileHandlerParams{
		DiskID:   *client.diskID.Load(),
		Volume:   volume,
		FilePath: path,
		Opts:     deleteOpts,
	})
	return toStorageErr(err)
}

// DeleteVersions - deletes list of specified versions if present
func (client *storageRESTClient) DeleteVersions(ctx context.Context, volume string, versions []FileInfoVersions, opts DeleteOptions) (errs []error) {
	if len(versions) == 0 {
		return errs
	}

	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTTotalVersions, strconv.Itoa(len(versions)))

	var buffer bytes.Buffer
	encoder := msgp.NewWriter(&buffer)
	for _, version := range versions {
		version.EncodeMsg(encoder)
	}
	storageLogIf(ctx, encoder.Flush())

	errs = make([]error, len(versions))

	respBody, err := client.call(ctx, storageRESTMethodDeleteVersions, values, &buffer, -1)
	defer xhttp.DrainBody(respBody)
	if err != nil {
		if contextCanceled(ctx) {
			err = ctx.Err()
		}
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	reader, err := waitForHTTPResponse(respBody)
	if err != nil {
		for i := range errs {
			errs[i] = toStorageErr(err)
		}
		return errs
	}

	dErrResp := &DeleteVersionsErrsResp{}
	decoder := msgpNewReader(reader)
	defer readMsgpReaderPoolPut(decoder)
	if err = dErrResp.DecodeMsg(decoder); err != nil {
		for i := range errs {
			errs[i] = toStorageErr(err)
		}
		return errs
	}

	for i, dErr := range dErrResp.Errs {
		if dErr != "" {
			errs[i] = toStorageErr(errors.New(dErr))
		} else {
			errs[i] = nil
		}
	}

	return errs
}

// RenamePart - renames multipart part file
func (client *storageRESTClient) RenamePart(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string, meta []byte, skipParent string) (err error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err = storageRenamePartRPC.Call(ctx, client.gridConn, &RenamePartHandlerParams{
		DiskID:      *client.diskID.Load(),
		SrcVolume:   srcVolume,
		SrcFilePath: srcPath,
		DstVolume:   dstVolume,
		DstFilePath: dstPath,
		Meta:        meta,
		SkipParent:  skipParent,
	})
	return toStorageErr(err)
}

// ReadParts - reads various part.N.meta paths from a drive remotely and returns object part info for each of those part.N.meta if found
func (client *storageRESTClient) ReadParts(ctx context.Context, volume string, partMetaPaths ...string) ([]*ObjectPartInfo, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)

	rp := &ReadPartsReq{Paths: partMetaPaths}
	buf, err := rp.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}

	respBody, err := client.call(ctx, storageRESTMethodReadParts, values, bytes.NewReader(buf), -1)
	defer xhttp.DrainBody(respBody)
	if err != nil {
		return nil, err
	}

	respReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return nil, toStorageErr(err)
	}

	rd := msgpNewReader(respReader)
	defer readMsgpReaderPoolPut(rd)

	readPartsResp := &ReadPartsResp{}
	if err = readPartsResp.DecodeMsg(rd); err != nil {
		return nil, toStorageErr(err)
	}

	return readPartsResp.Infos, nil
}

// RenameFile - renames a file.
func (client *storageRESTClient) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	ctx, cancel := context.WithTimeout(ctx, globalDriveConfig.GetMaxTimeout())
	defer cancel()

	_, err = storageRenameFileRPC.Call(ctx, client.gridConn, &RenameFileHandlerParams{
		DiskID:      *client.diskID.Load(),
		SrcVolume:   srcVolume,
		SrcFilePath: srcPath,
		DstVolume:   dstVolume,
		DstFilePath: dstPath,
	})
	return toStorageErr(err)
}

func (client *storageRESTClient) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) (*CheckPartsResp, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := msgp.Encode(&reader, &fi); err != nil {
		return nil, err
	}

	respBody, err := client.call(ctx, storageRESTMethodVerifyFile, values, &reader, -1)
	defer xhttp.DrainBody(respBody)
	if err != nil {
		return nil, err
	}

	respReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return nil, toStorageErr(err)
	}

	dec := msgpNewReader(respReader)
	defer readMsgpReaderPoolPut(dec)

	verifyResp := CheckPartsResp{}
	err = verifyResp.DecodeMsg(dec)
	if err != nil {
		return nil, toStorageErr(err)
	}

	return &verifyResp, nil
}

func (client *storageRESTClient) DeleteBulk(ctx context.Context, volume string, paths ...string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)

	req := &DeleteBulkReq{Paths: paths}
	body, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}

	respBody, err := client.call(ctx, storageRESTMethodDeleteBulk, values, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)

	_, err = waitForHTTPResponse(respBody)
	return toStorageErr(err)
}

func (client *storageRESTClient) StatInfoFile(ctx context.Context, volume, path string, glob bool) (stat []StatInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTGlob, strconv.FormatBool(glob))
	respBody, err := client.call(ctx, storageRESTMethodStatInfoFile, values, nil, -1)
	if err != nil {
		return stat, err
	}
	defer xhttp.DrainBody(respBody)
	respReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return stat, toStorageErr(err)
	}
	rd := msgpNewReader(respReader)
	defer readMsgpReaderPoolPut(rd)

	for {
		var st StatInfo
		err = st.DecodeMsg(rd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			break
		}

		stat = append(stat, st)
	}

	return stat, toStorageErr(err)
}

// ReadMultiple will read multiple files and send each back as response.
// Files are read and returned in the given order.
// The resp channel is closed before the call returns.
// Only a canceled context or network errors returns an error.
func (client *storageRESTClient) ReadMultiple(ctx context.Context, req ReadMultipleReq, resp chan<- ReadMultipleResp) error {
	defer xioutil.SafeClose(resp)
	body, err := req.MarshalMsg(nil)
	if err != nil {
		return err
	}
	respBody, err := client.call(ctx, storageRESTMethodReadMultiple, nil, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)

	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(waitForHTTPStream(respBody, xioutil.NewDeadlineWriter(pw, globalDriveConfig.GetMaxTimeout())))
	}()
	mr := msgp.NewReader(pr)
	defer readMsgpReaderPoolPut(mr)
	for {
		var file ReadMultipleResp
		if err := file.DecodeMsg(mr); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			pr.CloseWithError(err)
			return toStorageErr(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp <- file:
		}
	}
}

// CleanAbandonedData will read metadata of the object on disk
// and delete any data directories and inline data that isn't referenced in metadata.
func (client *storageRESTClient) CleanAbandonedData(ctx context.Context, volume string, path string) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(ctx, storageRESTMethodCleanAbandoned, values, nil, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)
	_, err = waitForHTTPResponse(respBody)
	return toStorageErr(err)
}

// Close - marks the client as closed.
func (client *storageRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

var emptyDiskID = ""

// Returns a storage rest client.
func newStorageRESTClient(endpoint Endpoint, healthCheck bool, gm *grid.Manager) (*storageRESTClient, error) {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   path.Join(storageRESTPrefix, endpoint.Path, storageRESTVersion),
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	if healthCheck {
		// Use a separate client to avoid recursive calls.
		healthClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
		healthClient.NoMetrics = true
		restClient.HealthCheckFn = func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
			defer cancel()
			respBody, err := healthClient.Call(ctx, storageRESTMethodHealth, nil, nil, -1)
			xhttp.DrainBody(respBody)
			return toStorageErr(err) != errDiskNotFound
		}
	}
	conn := gm.Connection(endpoint.GridHost()).Subroute(endpoint.Path)
	if conn == nil {
		return nil, fmt.Errorf("unable to find connection for %s in targets: %v", endpoint.GridHost(), gm.Targets())
	}
	client := &storageRESTClient{
		endpoint:      endpoint,
		restClient:    restClient,
		gridConn:      conn,
		diskInfoCache: cachevalue.New[DiskInfo](),
	}
	client.SetDiskID(emptyDiskID)
	return client, nil
}
