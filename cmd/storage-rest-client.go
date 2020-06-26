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
	"time"

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
func (client *storageRESTClient) call(method string, values url.Values, body io.Reader, length int64) (io.ReadCloser, error) {
	if !client.IsOnline() {
		return nil, errDiskNotFound
	}
	if values == nil {
		values = make(url.Values)
	}
	values.Set(storageRESTDiskID, client.diskID)
	respBody, err := client.restClient.Call(method, values, body, length)
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

func (client *storageRESTClient) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	b := cache.serialize()
	respBody, err := client.call(storageRESTMethodCrawlAndGetDataUsage,
		url.Values{},
		bytes.NewBuffer(b), int64(len(b)))
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
func (client *storageRESTClient) DiskInfo() (info DiskInfo, err error) {
	respBody, err := client.call(storageRESTMethodDiskInfo, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
}

// MakeVolBulk - create multiple volumes in a bulk operation.
func (client *storageRESTClient) MakeVolBulk(volumes ...string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolumes, strings.Join(volumes, ","))
	respBody, err := client.call(storageRESTMethodMakeVolBulk, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// MakeVol - create a volume on a remote disk.
func (client *storageRESTClient) MakeVol(volume string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	respBody, err := client.call(storageRESTMethodMakeVol, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// ListVols - List all volumes on a remote disk.
func (client *storageRESTClient) ListVols() (volinfo []VolInfo, err error) {
	respBody, err := client.call(storageRESTMethodListVols, nil, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&volinfo)
	return volinfo, err
}

// StatVol - get volume info over the network.
func (client *storageRESTClient) StatVol(volume string) (volInfo VolInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	respBody, err := client.call(storageRESTMethodStatVol, values, nil, -1)
	if err != nil {
		return
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&volInfo)
	return volInfo, err
}

// DeleteVol - Deletes a volume over the network.
func (client *storageRESTClient) DeleteVol(volume string, forceDelete bool) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	if forceDelete {
		values.Set(storageRESTForceDelete, "true")
	}
	respBody, err := client.call(storageRESTMethodDeleteVol, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// AppendFile - append to a file.
func (client *storageRESTClient) AppendFile(volume, path string, buffer []byte) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	reader := bytes.NewReader(buffer)
	respBody, err := client.call(storageRESTMethodAppendFile, values, reader, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) CreateFile(volume, path string, length int64, r io.Reader) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTLength, strconv.Itoa(int(length)))
	respBody, err := client.call(storageRESTMethodCreateFile, values, ioutil.NopCloser(r), length)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) WriteMetadata(volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(storageRESTMethodWriteMetadata, values, &reader, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) DeleteVersion(volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(storageRESTMethodDeleteVersion, values, &buffer, -1)
	defer http.DrainBody(respBody)
	return err
}

// WriteAll - write all data to a file.
func (client *storageRESTClient) WriteAll(volume, path string, reader io.Reader) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(storageRESTMethodWriteAll, values, reader, -1)
	defer http.DrainBody(respBody)
	return err
}

// CheckFile - stat a file metadata.
func (client *storageRESTClient) CheckFile(volume, path string) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(storageRESTMethodCheckFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// CheckParts - stat all file parts.
func (client *storageRESTClient) CheckParts(volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(storageRESTMethodWriteMetadata, values, &reader, -1)
	defer http.DrainBody(respBody)
	return err
}

// RenameData - rename source path to destination path atomically, metadata and data file.
func (client *storageRESTClient) RenameData(srcVolume, srcPath, dataDir, dstVolume, dstPath string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTSrcVolume, srcVolume)
	values.Set(storageRESTSrcPath, srcPath)
	values.Set(storageRESTDataDir, dataDir)
	values.Set(storageRESTDstVolume, dstVolume)
	values.Set(storageRESTDstPath, dstPath)
	respBody, err := client.call(storageRESTMethodRenameData, values, nil, -1)
	defer http.DrainBody(respBody)

	return err
}

func (client *storageRESTClient) ReadVersion(volume, path, versionID string) (fi FileInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTVersionID, versionID)

	respBody, err := client.call(storageRESTMethodReadVersion, values, nil, -1)
	if err != nil {
		return fi, err
	}
	defer http.DrainBody(respBody)

	err = gob.NewDecoder(respBody).Decode(&fi)
	return fi, err
}

// ReadAll - reads all contents of a file.
func (client *storageRESTClient) ReadAll(volume, path string) ([]byte, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(storageRESTMethodReadAll, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)
	return ioutil.ReadAll(respBody)
}

// ReadFileStream - returns a reader for the requested file.
func (client *storageRESTClient) ReadFileStream(volume, path string, offset, length int64) (io.ReadCloser, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTOffset, strconv.Itoa(int(offset)))
	values.Set(storageRESTLength, strconv.Itoa(int(length)))
	respBody, err := client.call(storageRESTMethodReadFileStream, values, nil, -1)
	if err != nil {
		return nil, err
	}
	return respBody, nil
}

// ReadFile - reads section of a file.
func (client *storageRESTClient) ReadFile(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (int64, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	values.Set(storageRESTOffset, strconv.Itoa(int(offset)))
	values.Set(storageRESTLength, strconv.Itoa(len(buffer)))
	if verifier != nil {
		values.Set(storageRESTBitrotAlgo, verifier.algorithm.String())
		values.Set(storageRESTBitrotHash, hex.EncodeToString(verifier.sum))
	} else {
		values.Set(storageRESTBitrotAlgo, "")
		values.Set(storageRESTBitrotHash, "")
	}
	respBody, err := client.call(storageRESTMethodReadFile, values, nil, -1)
	if err != nil {
		return 0, err
	}
	defer http.DrainBody(respBody)
	n, err := io.ReadFull(respBody, buffer)
	return int64(n), err
}

func (client *storageRESTClient) WalkSplunk(volume, dirPath, marker string, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	respBody, err := client.call(storageRESTMethodWalkSplunk, values, nil, -1)
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

func (client *storageRESTClient) WalkVersions(volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	values.Set(storageRESTRecursive, strconv.FormatBool(recursive))
	respBody, err := client.call(storageRESTMethodWalk, values, nil, -1)
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

func (client *storageRESTClient) Walk(volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfo, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTMarkerPath, marker)
	values.Set(storageRESTRecursive, strconv.FormatBool(recursive))
	respBody, err := client.call(storageRESTMethodWalk, values, nil, -1)
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
func (client *storageRESTClient) ListDir(volume, dirPath string, count int) (entries []string, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTCount, strconv.Itoa(count))
	respBody, err := client.call(storageRESTMethodListDir, values, nil, -1)
	if err != nil {
		return nil, err
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&entries)
	return entries, err
}

// DeleteFile - deletes a file.
func (client *storageRESTClient) DeleteFile(volume, path string) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(storageRESTMethodDeleteFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// DeleteVersions - deletes list of specified versions if present
func (client *storageRESTClient) DeleteVersions(volume string, versions []FileInfo) (errs []error) {
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

	respBody, err := client.call(storageRESTMethodDeleteVersions, values, &buffer, -1)
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
func (client *storageRESTClient) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTSrcVolume, srcVolume)
	values.Set(storageRESTSrcPath, srcPath)
	values.Set(storageRESTDstVolume, dstVolume)
	values.Set(storageRESTDstPath, dstPath)
	respBody, err := client.call(storageRESTMethodRenameFile, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

func (client *storageRESTClient) VerifyFile(volume, path string, fi FileInfo) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)

	var reader bytes.Buffer
	if err := gob.NewEncoder(&reader).Encode(fi); err != nil {
		return err
	}

	respBody, err := client.call(storageRESTMethodVerifyFile, values, &reader, -1)
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
			NextProtos: []string{"http/1.1"}, // Force http1.1
		}
	}

	trFn := newCustomHTTPTransport(tlsConfig, rest.DefaultRESTTimeout)
	restClient, err := rest.NewClient(serverURL, trFn, newAuthToken)
	if err != nil {
		logger.Fatal(err, "Unable to initialize remote REST disks")
	}

	restClient.HealthCheckInterval = 500 * time.Millisecond
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(GlobalContext, restClient.HealthCheckTimeout)
		respBody, err := restClient.CallWithContext(ctx, storageRESTMethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		cancel()
		return !errors.Is(err, context.DeadlineExceeded) && toStorageErr(err) != errDiskNotFound
	}

	return &storageRESTClient{endpoint: endpoint, restClient: restClient}
}
