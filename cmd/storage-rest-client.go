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
	"bytes"
	"crypto/tls"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"path"
	"strconv"

	"encoding/gob"
	"encoding/hex"

	"fmt"
	"strings"

	"github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/rest"
	xnet "github.com/minio/minio/pkg/net"
)

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if err.Error() == errConnectionStale.Error() {
		return true
	}
	if strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}
	if uerr, isURLError := err.(*url.Error); isURLError {
		if uerr.Timeout() {
			return true
		}

		err = uerr.Err
	}

	_, isNetOpError := err.(*net.OpError)
	return isNetOpError
}

// Attempt to approximate network error with a
// typed network error, otherwise default to
// errDiskNotFound
func toNetworkError(err error) error {
	if err == nil {
		return err
	}
	if strings.Contains(err.Error(), "connection reset by peer") {
		return errNetworkConnReset
	}
	return errDiskNotFound
}

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func toStorageErr(err error) error {
	if err == nil {
		return nil
	}

	if isNetworkError(err) {
		return toNetworkError(err)
	}

	switch err.Error() {
	case io.EOF.Error():
		return io.EOF
	case io.ErrUnexpectedEOF.Error():
		return io.ErrUnexpectedEOF
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
	}
	if strings.Contains(err.Error(), "Bitrot verification mismatch") {
		var expected string
		var received string
		fmt.Sscanf(err.Error(), "Bitrot verification mismatch - expected %s received %s", &expected, &received)
		// Go's Sscanf %s scans "," that comes after the expected hash, hence remove it. Providing "," in the format string does not help.
		expected = strings.TrimSuffix(expected, ",")
		bitrotErr := hashMismatchError{expected, received}
		return bitrotErr
	}
	return err
}

// Abstracts a remote disk.
type storageRESTClient struct {
	endpoint   Endpoint
	restClient *rest.Client
	connected  bool
	lastError  error
	instanceID string // REST server's instanceID which is sent with every request for validation.
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is makred disconnected
// permanently. The only way to restore the storage connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *storageRESTClient) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if !client.connected {
		return nil, errDiskNotFound
	}
	if values == nil {
		values = make(url.Values)
	}
	values.Set(storageRESTInstanceID, client.instanceID)
	respBody, err = client.restClient.Call(method, values, body, length)
	if err == nil {
		return respBody, nil
	}
	client.lastError = err
	if isNetworkError(err) {
		client.connected = false
	}

	return nil, toStorageErr(err)
}

// Stringer provides a canonicalized representation of network device.
func (client *storageRESTClient) String() string {
	return client.endpoint.String()
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *storageRESTClient) IsOnline() bool {
	return client.connected
}

// LastError - returns the network error if any.
func (client *storageRESTClient) LastError() error {
	return client.lastError
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
func (client *storageRESTClient) DeleteVol(volume string) (err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	respBody, err := client.call(storageRESTMethodDeleteVol, values, nil, -1)
	defer http.DrainBody(respBody)
	return err
}

// AppendFile - append to a file.
func (client *storageRESTClient) AppendFile(volume, path string, buffer []byte) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	reader := bytes.NewBuffer(buffer)
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

// WriteAll - write all data to a file.
func (client *storageRESTClient) WriteAll(volume, path string, buffer []byte) error {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	reader := bytes.NewBuffer(buffer)
	respBody, err := client.call(storageRESTMethodWriteAll, values, reader, -1)
	defer http.DrainBody(respBody)
	return err
}

// StatFile - stat a file.
func (client *storageRESTClient) StatFile(volume, path string) (info FileInfo, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTFilePath, path)
	respBody, err := client.call(storageRESTMethodStatFile, values, nil, -1)
	if err != nil {
		return info, err
	}
	defer http.DrainBody(respBody)
	err = gob.NewDecoder(respBody).Decode(&info)
	return info, err
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

// ListDir - lists a directory.
func (client *storageRESTClient) ListDir(volume, dirPath string, count int, leafFile string) (entries []string, err error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, volume)
	values.Set(storageRESTDirPath, dirPath)
	values.Set(storageRESTCount, strconv.Itoa(count))
	values.Set(storageRESTLeafFile, leafFile)
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

// Gets peer storage server's instanceID - to be used with every REST call for validation.
func (client *storageRESTClient) getInstanceID() (err error) {
	// getInstanceID() does not use storageRESTClient.call()
	// function so we need to update lastError field here.
	defer func() {
		if err != nil {
			client.lastError = err
		}
	}()

	respBody, err := client.restClient.Call(storageRESTMethodGetInstanceID, nil, nil, -1)
	if err != nil {
		return err
	}
	defer http.DrainBody(respBody)
	instanceIDBuf := make([]byte, 64)
	n, err := io.ReadFull(respBody, instanceIDBuf)
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	client.instanceID = string(instanceIDBuf[:n])
	return nil
}

// Close - marks the client as closed.
func (client *storageRESTClient) Close() error {
	client.connected = false
	client.restClient.Close()
	return nil
}

// Returns a storage rest client.
func newStorageRESTClient(endpoint Endpoint) (*storageRESTClient, error) {
	host, err := xnet.ParseHost(endpoint.Host)
	if err != nil {
		return nil, err
	}

	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   endpoint.Host,
		Path:   path.Join(storageRESTPath, endpoint.Path),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
			NextProtos: []string{"http/1.1"}, // Force http1.1
		}
	}

	restClient, err := rest.NewClient(serverURL, tlsConfig, rest.DefaultRESTTimeout, newAuthToken)
	if err != nil {
		return nil, err
	}
	client := &storageRESTClient{endpoint: endpoint, restClient: restClient, connected: true}
	client.connected = client.getInstanceID() == nil
	return client, nil
}
