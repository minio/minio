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

package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/url"
	"path"
	"strings"

	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
)

func isNetworkDisconnectError(err error) bool {
	if err == nil {
		return false
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

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func toStorageErr(err error) error {
	if err == nil {
		return nil
	}

	if isNetworkDisconnectError(err) {
		return errDiskNotFound
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
	return err
}

// StorageRPCClient - storage RPC client.
type StorageRPCClient struct {
	*RPCClient
	connected bool
}

// Stringer provides a canonicalized representation of network device.
func (client *StorageRPCClient) String() string {
	url := client.ServiceURL()
	// Remove the storage RPC path prefix, internal paths are meaningless. why?
	url.Path = strings.TrimPrefix(url.Path, storageServicePath)
	return url.String()
}

// Close - closes underneath RPC client.
func (client *StorageRPCClient) Close() error {
	client.connected = false
	return toStorageErr(client.RPCClient.Close())
}

// IsOnline - returns whether RPC client failed to connect or not.
func (client *StorageRPCClient) IsOnline() bool {
	return client.connected
}

func (client *StorageRPCClient) call(handler string, args interface {
	SetAuthArgs(args AuthArgs)
}, reply interface{}) error {
	if !client.connected {
		return errDiskNotFound
	}

	err := client.Call(handler, args, reply)
	if err == nil {
		return nil
	}

	if isNetworkDisconnectError(err) {
		client.connected = false
	}

	return toStorageErr(err)
}

// DiskInfo - fetch disk information for a remote disk.
func (client *StorageRPCClient) DiskInfo() (info DiskInfo, err error) {
	err = client.call(storageServiceName+".DiskInfo", &AuthArgs{}, &info)
	return info, err
}

// MakeVol - create a volume on a remote disk.
func (client *StorageRPCClient) MakeVol(volume string) (err error) {
	return client.call(storageServiceName+".MakeVol", &VolArgs{Vol: volume}, &VoidReply{})
}

// ListVols - List all volumes on a remote disk.
func (client *StorageRPCClient) ListVols() ([]VolInfo, error) {
	var reply []VolInfo
	err := client.call(storageServiceName+".ListVols", &AuthArgs{}, &reply)
	return reply, err
}

// StatVol - get volume info over the network.
func (client *StorageRPCClient) StatVol(volume string) (volInfo VolInfo, err error) {
	err = client.call(storageServiceName+".StatVol", &VolArgs{Vol: volume}, &volInfo)
	return volInfo, err
}

// DeleteVol - Deletes a volume over the network.
func (client *StorageRPCClient) DeleteVol(volume string) (err error) {
	return client.call(storageServiceName+".DeleteVol", &VolArgs{Vol: volume}, &VoidReply{})
}

// File operations.

// PrepareFile - calls PrepareFile RPC.
func (client *StorageRPCClient) PrepareFile(volume, path string, length int64) (err error) {
	args := PrepareFileArgs{
		Vol:  volume,
		Path: path,
		Size: length,
	}
	reply := VoidReply{}

	return client.call(storageServiceName+".PrepareFile", &args, &reply)
}

// AppendFile - append file writes buffer to a remote network path.
func (client *StorageRPCClient) AppendFile(volume, path string, buffer []byte) (err error) {
	args := AppendFileArgs{
		Vol:    volume,
		Path:   path,
		Buffer: buffer,
	}
	reply := VoidReply{}

	return client.call(storageServiceName+".AppendFile", &args, &reply)
}

// StatFile - get latest Stat information for a file at path.
func (client *StorageRPCClient) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	err = client.call(storageServiceName+".StatFile", &StatFileArgs{Vol: volume, Path: path}, &fileInfo)
	return fileInfo, err
}

// ReadAll - reads entire contents of the file at path until EOF, returns the
// contents in a byte slice. Returns buf == nil if err != nil.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (client *StorageRPCClient) ReadAll(volume, path string) (buf []byte, err error) {
	err = client.call(storageServiceName+".ReadAll", &ReadAllArgs{Vol: volume, Path: path}, &buf)
	return buf, err
}

// ReadFile - reads a file at remote path and fills the buffer.
func (client *StorageRPCClient) ReadFile(volume string, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (m int64, err error) {
	// Recover from any panic and return error.
	defer func() {
		if r := recover(); r != nil {
			err = bytes.ErrTooLarge
		}
	}()

	args := ReadFileArgs{
		Vol:      volume,
		Path:     path,
		Offset:   offset,
		Buffer:   buffer,
		Verified: true, // mark read as verified by default
	}
	if verifier != nil {
		args.Algo = verifier.algorithm
		args.ExpectedHash = verifier.sum
		args.Verified = verifier.IsVerified()
	}
	var reply []byte

	err = client.call(storageServiceName+".ReadFile", &args, &reply)

	// Copy reply to buffer.
	copy(buffer, reply)

	// Return length of result, err if any.
	return int64(len(reply)), err
}

// ListDir - list all entries at prefix.
func (client *StorageRPCClient) ListDir(volume, path string, count int) (entries []string, err error) {
	err = client.call(storageServiceName+".ListDir", &ListDirArgs{Vol: volume, Path: path, Count: count}, &entries)
	return entries, err
}

// DeleteFile - Delete a file at path.
func (client *StorageRPCClient) DeleteFile(volume, path string) (err error) {
	args := DeleteFileArgs{
		Vol:  volume,
		Path: path,
	}
	reply := VoidReply{}

	return client.call(storageServiceName+".DeleteFile", &args, &reply)
}

// RenameFile - rename a remote file from source to destination.
func (client *StorageRPCClient) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	args := RenameFileArgs{
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}
	reply := VoidReply{}

	return client.call(storageServiceName+".RenameFile", &args, &reply)
}

// NewStorageRPCClient - returns new storage RPC client.
func NewStorageRPCClient(host *xnet.Host, endpointPath string) (*StorageRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   path.Join(storageServicePath, endpointPath),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	rpcClient, err := NewRPCClient(
		RPCClientArgs{
			NewAuthTokenFunc: newAuthToken,
			RPCVersion:       globalRPCAPIVersion,
			ServiceName:      storageServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &StorageRPCClient{RPCClient: rpcClient}, nil
}

// Initialize new storage rpc client.
func newStorageRPC(endpoint Endpoint) *StorageRPCClient {
	host, err := xnet.ParseHost(endpoint.Host)
	logger.FatalIf(err, "Unable to parse storage RPC Host", context.Background())
	rpcClient, err := NewStorageRPCClient(host, endpoint.Path)
	logger.FatalIf(err, "Unable to initialize storage RPC client", context.Background())
	rpcClient.connected = rpcClient.Call(storageServiceName+".Connect", &AuthArgs{}, &VoidReply{}) == nil
	return rpcClient
}
