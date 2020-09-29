/*
 * MinIO Cloud Storage, (C) 2018-2020 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/http"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/rest"
	xnet "github.com/minio/minio/pkg/net"
)

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if nerr, ok := err.(*rest.NetworkError); ok {
		return xnet.IsNetworkOrHostDown(nerr.Err)
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
	case errFaultyDisk.Error():
		return errFaultyDisk
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
	case errIsNotRegular.Error():
		return errIsNotRegular
	case errVolumeNotEmpty.Error():
		return errVolumeNotEmpty
	case errVolumeAccessDenied.Error():
		return errVolumeAccessDenied
	case errCorruptedFormat.Error():
		return errCorruptedFormat
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
	}
	return err
}

// Abstracts a remote disk.
type storageRESTClient struct {
	endpoint   Endpoint
	restClient *rest.Client
	diskID     string
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is makred disconnected
// permanently. The only way to restore the storage connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *storageRESTClient) call(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (io.ReadCloser, error) {
	if !client.IsOnline() {
		return nil, errDiskNotFound
	}
	if values == nil {
		values = make(url.Values)
	}
	values.Set(storageRESTDiskID, client.diskID)
	respBody, err := client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	err = toStorageErr(err)

	return nil, err
}

// Stringer provides a canonicalized representation of network device.
func (client *storageRESTClient) String() string {
	return client.endpoint.String()
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *storageRESTClient) IsOnline() bool {
	return client.restClient.IsOnline()
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

func (client *storageRESTClient) Healing() bool {
	// This call should never be called over the network
	// this function should always return 'false'
	//
	// To know if a remote disk is being healed
	// perform DiskInfo() call which would return
	// back the correct data if disk is being healed.
	return false
}

func (client *storageRESTClient) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	b := cache.serialize()
	respBody, err := client.call(ctx, storageRESTMethodCrawlAndGetDataUsage, url.Values{}, bytes.NewBuffer(b), int64(len(b)))
	defer http.DrainBody(respBody)
	if err != nil {
		return cache, err
	}
	reader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return cache, err
	}
	var newCache dataUsageCache
	return newCache, newCache.deserialize(reader)
}

func (client *storageRESTClient) GetDiskID() (string, error) {
	// This call should never be over the network, this is always
	// a cached value - caller should make sure to use this
	// function on a fresh disk or make sure to look at the error
	// from a different networked call to validate the GetDiskID()
	return client.diskID, nil
}

func (client *storageRESTClient) SetDiskID(id string) {
	client.diskID = id
}

// DiskInfo - fetch disk information for a remote disk.
func (client *storageRESTClient) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	respBody, err := client.call(ctx, storageRESTMethodDiskInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	if err != nil {
		return info, err
	}
	if info.Error != "" {
		return info, toStorageErr(errors.New(info.Error))
	}
	return info, nil
}

// MakeVolBulk - create multiple volumes in a bulk operation.
func (client *storageRESTClient) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolumes, strings.Join(volumes, ","))
	respBody, err := client.call(ctx, storageRESTMethodMakeVolBulk, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// MakeVol - create a volume on a remote disk.
func (client *storageRESTClient) MakeVol(ctx context.Context, volume string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	respBody, err := client.call(ctx, storageRESTMethodMakeVol, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// ListVols - List all volumes on a remote disk.
func (client *storageRESTClient) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	respBody, err := client.call(ctx, storageRESTMethodListVols, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&vols)
	return vols, err
}

// StatVol - get volume info over the network.
func (client *storageRESTClient) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	respBody, err := client.call(ctx, storageRESTMethodStatVol, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&vol)
	return vol, err
}

// DeleteVol - Deletes a volume over the network.
func (client *storageRESTClient) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	if forceDelete {
		values.Set(storageRESTForceDelete, "true")
	}
	respBody, err := client.call(ctx, storageRESTMethodDeleteVol, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// AppendFile - append to a file.
func (client *storageRESTClient) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	reader := bytes.NewReader(buf)
	respBody, err := client.call(ctx, storageRESTMethodAppendFile, values, reader, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTLength, strconv.Itoa(int(size)))
	respBody, err := client.call(ctx, storageRESTMethodCreateFile, values, ioutil.NopCloser(reader), size)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(ctx, storageRESTMethodWriteMetadata, values, &reader, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(ctx, storageRESTMethodDeleteVersion, values, &buffer, -1)
	defer http.DrainBody(respBody)
	return err
}

// WriteAll - write all data to a file.
func (client *storageRESTClient) WriteAll(ctx context.Context, volume string, path string, reader io.Reader) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(ctx, storageRESTMethodWriteAll, values, reader, -1)
	defer http.DrainBody(respBody)
	return err
}

// CheckFile - stat a file metadata.
func (client *storageRESTClient) CheckFile(ctx context.Context, volume string, path string) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(ctx, storageRESTMethodCheckFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// CheckParts - stat all file parts.
func (client *storageRESTClient) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}

	respBody, err := client.call(ctx, storageRESTMethodCheckParts, values, &reader, -1)
	defer http.DrainBody(respBody)
	return err
}

// RenameData - rename source path to destination path atomically, metadata and data file.
func (client *storageRESTClient) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTSrcVolume, srcVolume)
	values.Set(storageRESTSrcPath, srcPath)
	values.Set(storageRESTDataDir, dataDir)
	values.Set(storageRESTDstVolume, dstVolume)
	values.Set(storageRESTDstPath, dstPath)
	respBody, err := client.call(ctx, storageRESTMethodRenameData, values, nil, -1)
	defer http.DrainBody(respBody)

	return err
}

func (client *storageRESTClient) ReadVersion(ctx context.Context, volume, path, versionID string) (fi FileInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTVersionID, versionID)

	respBody, err := client.call(ctx, storageRESTMethodReadVersion, values, nil, -1)
	if err != nil {
		return fi, err
	}
	defer http.DrainBody(respBody)

	err = gob.NewDecoder(respBody).Decode(&fi)
	return fi, err
}

// ReadAll - reads all contents of a file.
func (client *storageRESTClient) ReadAll(ctx context.Context, volume string, path string) ([]byte, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(ctx, storageRESTMethodReadAll, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)
	return ioutil.ReadAll(respBody)
}

// ReadFileStream - returns a reader for the requested file.
func (client *storageRESTClient) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTOffset, strconv.Itoa(int(offset)))
	values.Set(storageRESTLength, strconv.Itoa(int(length)))
	respBody, err := client.call(ctx, storageRESTMethodReadFileStream, values, nil, -1)
	if err != nil {
		return nil, err
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
	respBody, err := client.call(ctx, storageRESTMethodReadFile, values, nil, -1)
	if err != nil {
		return 0, err
	}
	defer http.DrainBody(respBody)
	n, err := io.ReadFull(respBody, buf)
	return int64(n), err
}

func (client *storageRESTClient) WalkSplunk(ctx context.Context, volume, dirPath, marker string, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	respBody, err := client.call(ctx, storageRESTMethodWalkSplunk, values, nil, -1)
	if err != nil {
		return nil, err
	}

	ch := make(chan FileInfo)
	go func() {
		defer close(ch)
		defer http.DrainBody(respBody)

		decoder := gob.NewDecoder(respBody)
		for {
			var fi FileInfo
			if gerr := decoder.Decode(&fi); gerr != nil {
				// Upon error return
				return
			}
			select {
			case ch <- fi:
			case <-endWalkCh:
				return
			}

		}
	}()

	return ch, nil
}

func (client *storageRESTClient) WalkVersions(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	values.Set(storageRESTRecursive, strconv.FormatBool(recursive))
	respBody, err := client.call(ctx, storageRESTMethodWalkVersions, values, nil, -1)
	if err != nil {
		return nil, err
	}

	ch := make(chan FileInfoVersions)
	go func() {
		defer close(ch)
		defer http.DrainBody(respBody)

		decoder := gob.NewDecoder(respBody)
		for {
			var fi FileInfoVersions
			if gerr := decoder.Decode(&fi); gerr != nil {
				// Upon error return
				if gerr != io.EOF {
					logger.LogIf(GlobalContext, gerr)
				}
				return
			}
			select {
			case ch <- fi:
			case <-endWalkCh:
				return
			}
		}
	}()

	return ch, nil
}

func (client *storageRESTClient) Walk(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	values.Set(storageRESTRecursive, strconv.FormatBool(recursive))
	respBody, err := client.call(ctx, storageRESTMethodWalk, values, nil, -1)
	if err != nil {
		return nil, err
	}

	ch := make(chan FileInfo)
	go func() {
		defer close(ch)
		defer http.DrainBody(respBody)

		decoder := gob.NewDecoder(respBody)
		for {
			var fi FileInfo
			if gerr := decoder.Decode(&fi); gerr != nil {
				// Upon error return
				return
			}
			select {
			case ch <- fi:
			case <-endWalkCh:
				return
			}

		}
	}()

	return ch, nil
}

// ListDir - lists a directory.
func (client *storageRESTClient) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTCount, strconv.Itoa(count))
	respBody, err := client.call(ctx, storageRESTMethodListDir, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&entries)
	return entries, err
}

// DeleteFile - deletes a file.
func (client *storageRESTClient) DeleteFile(ctx context.Context, volume string, path string) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(ctx, storageRESTMethodDeleteFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// DeleteVersions - deletes list of specified versions if present
func (client *storageRESTClient) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) (errs []error) {
	if len(versions) == 0 {
		return errs
	}

	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTTotalVersions, strconv.Itoa(len(versions)))

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	for _, version := range versions {
		encoder.Encode(&version)
	}

	errs = make([]error, len(versions))

	respBody, err := client.call(ctx, storageRESTMethodDeleteVersions, values, &buffer, -1)
	defer http.DrainBody(respBody)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	reader, err := waitForHTTPResponse(respBody)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	dErrResp := &DeleteVersionsErrsResp{}
	if err = gob.NewDecoder(reader).Decode(dErrResp); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	for i, dErr := range dErrResp.Errs {
		errs[i] = toStorageErr(dErr)
	}

	return errs
}

// RenameFile - renames a file.
func (client *storageRESTClient) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTSrcVolume, srcVolume)
	values.Set(storageRESTSrcPath, srcPath)
	values.Set(storageRESTDstVolume, dstVolume)
	values.Set(storageRESTDstPath, dstPath)
	respBody, err := client.call(ctx, storageRESTMethodRenameFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(ctx, storageRESTMethodVerifyFile, values, &reader, -1)
	defer http.DrainBody(respBody)
	if err != nil {
		return err
	}

	respReader, err := waitForHTTPResponse(respBody)
	if err != nil {
		return err
	}

	verifyResp := &VerifyFileResp{}
	if err = gob.NewDecoder(respReader).Decode(verifyResp); err != nil {
		return err
	}

	return toStorageErr(verifyResp.Err)
}

// Close - marks the client as closed.
func (client *storageRESTClient) Close() error {
	client.restClient.Close()
	return nil
}

// Returns a storage rest client.
func newStorageRESTClient(endpoint Endpoint) *storageRESTClient {
	serverURL := &url.URL{
		Scheme: endpoint.Scheme,
		Host:   endpoint.Host,
		Path:   path.Join(storageRESTPrefix, endpoint.Path, storageRESTVersion),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: endpoint.Hostname(),
			RootCAs:    globalRootCAs,
		}
	}

	trFn := newInternodeHTTPTransport(tlsConfig, rest.DefaultTimeout)
	restClient := rest.NewClient(serverURL, trFn, newAuthToken)
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(GlobalContext, restClient.HealthCheckTimeout)
		// Instantiate a new rest client for healthcheck
		// to avoid recursive healthCheckFn()
		respBody, err := rest.NewClient(serverURL, trFn, newAuthToken).Call(ctx, storageRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		cancel()
		return !errors.Is(err, context.DeadlineExceeded) && toStorageErr(err) != errDiskNotFound
	}

	return &storageRESTClient{endpoint: endpoint, restClient: restClient}
}
