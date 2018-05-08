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
	"io"
	"net"
	"net/rpc"
	"path"
	"strings"

	"github.com/minio/minio/pkg/disk"
)

type networkStorage struct {
	rpcClient *AuthRPCClient
	connected bool
}

const (
	storageRPCPath = "/storage"
)

func isErrorNetworkDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*net.OpError); ok {
		return true
	}
	if err == rpc.ErrShutdown {
		return true
	}
	return false
}

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func toStorageErr(err error) error {
	if err == nil {
		return nil
	}

	if isErrorNetworkDisconnect(err) {
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

// Initialize new storage rpc client.
func newStorageRPC(endpoint Endpoint) StorageAPI {
	// Dial minio rpc storage http path.
	rpcPath := path.Join(minioReservedBucketPath, storageRPCPath, endpoint.Path)
	serverCred := globalServerConfig.GetCredential()

	disk := &networkStorage{
		rpcClient: newAuthRPCClient(authConfig{
			accessKey:        serverCred.AccessKey,
			secretKey:        serverCred.SecretKey,
			serverAddr:       endpoint.Host,
			serviceEndpoint:  rpcPath,
			secureConn:       globalIsSSL,
			serviceName:      "Storage",
			disableReconnect: true,
		}),
	}
	// Attempt a remote login.
	disk.connected = disk.rpcClient.Login() == nil
	return disk
}

// Stringer provides a canonicalized representation of network device.
func (n *networkStorage) String() string {
	// Remove the storage RPC path prefix, internal paths are meaningless.
	serviceEndpoint := strings.TrimPrefix(n.rpcClient.ServiceEndpoint(),
		path.Join(minioReservedBucketPath, storageRPCPath))
	// Check for the transport layer being used.
	scheme := "http"
	if n.rpcClient.config.secureConn {
		scheme = "https"
	}
	// Finally construct the disk endpoint in http://<server>/<path> form.
	return scheme + "://" + n.rpcClient.ServerAddr() + path.Join("/", serviceEndpoint)
}

func (n *networkStorage) Close() error {
	n.connected = false
	return toStorageErr(n.rpcClient.Close())
}

func (n *networkStorage) IsOnline() bool {
	return n.connected
}

func (n *networkStorage) call(handler string, args interface {
	SetAuthToken(string)
	SetRPCAPIVersion(semVersion)
}, reply interface{}) error {
	if !n.connected {
		return errDiskNotFound
	}
	if err := n.rpcClient.Call(handler, args, reply); err != nil {
		if isErrorNetworkDisconnect(err) {
			n.connected = false
		}
		return toStorageErr(err)
	}
	return nil
}

// DiskInfo - fetch disk information for a remote disk.
func (n *networkStorage) DiskInfo() (info disk.Info, err error) {
	args := AuthRPCArgs{}
	if err = n.call("Storage.DiskInfoHandler", &args, &info); err != nil {
		return disk.Info{}, err
	}
	return info, nil
}

// MakeVol - create a volume on a remote disk.
func (n *networkStorage) MakeVol(volume string) (err error) {
	reply := AuthRPCReply{}
	args := GenericVolArgs{Vol: volume}
	return n.call("Storage.MakeVolHandler", &args, &reply)
}

// ListVols - List all volumes on a remote disk.
func (n *networkStorage) ListVols() (vols []VolInfo, err error) {
	ListVols := ListVolsReply{}
	if err = n.call("Storage.ListVolsHandler", &AuthRPCArgs{}, &ListVols); err != nil {
		return nil, err
	}
	return ListVols.Vols, nil
}

// StatVol - get volume info over the network.
func (n *networkStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	args := GenericVolArgs{Vol: volume}
	if err = n.call("Storage.StatVolHandler", &args, &volInfo); err != nil {
		return VolInfo{}, err
	}
	return volInfo, nil
}

// DeleteVol - Deletes a volume over the network.
func (n *networkStorage) DeleteVol(volume string) (err error) {
	reply := AuthRPCReply{}
	args := GenericVolArgs{Vol: volume}
	return n.call("Storage.DeleteVolHandler", &args, &reply)
}

// File operations.

func (n *networkStorage) PrepareFile(volume, path string, length int64) (err error) {
	reply := AuthRPCReply{}
	return n.call("Storage.PrepareFileHandler", &PrepareFileArgs{
		Vol:  volume,
		Path: path,
		Size: length,
	}, &reply)
}

// AppendFile - append file writes buffer to a remote network path.
func (n *networkStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	reply := AuthRPCReply{}
	return n.call("Storage.AppendFileHandler", &AppendFileArgs{
		Vol:    volume,
		Path:   path,
		Buffer: buffer,
	}, &reply)
}

// StatFile - get latest Stat information for a file at path.
func (n *networkStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	if err = n.call("Storage.StatFileHandler", &StatFileArgs{
		Vol:  volume,
		Path: path,
	}, &fileInfo); err != nil {
		return FileInfo{}, err
	}
	return fileInfo, nil
}

// ReadAll - reads entire contents of the file at path until EOF, returns the
// contents in a byte slice. Returns buf == nil if err != nil.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (n *networkStorage) ReadAll(volume, path string) (buf []byte, err error) {
	if err = n.call("Storage.ReadAllHandler", &ReadAllArgs{
		Vol:  volume,
		Path: path,
	}, &buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadFile - reads a file at remote path and fills the buffer.
func (n *networkStorage) ReadFile(volume string, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (m int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Recover any panic from allocation, and return error.
			err = bytes.ErrTooLarge
		}
	}() // Do not crash the server.

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

	var result []byte
	err = n.call("Storage.ReadFileHandler", &args, &result)

	// Copy results to buffer.
	copy(buffer, result)

	// Return length of result, err if any.
	return int64(len(result)), err
}

// ListDir - list all entries at prefix.
func (n *networkStorage) ListDir(volume, path string, count int) (entries []string, err error) {
	if err = n.call("Storage.ListDirHandler", &ListDirArgs{
		Vol:   volume,
		Path:  path,
		Count: count,
	}, &entries); err != nil {
		return nil, err
	}
	// Return successfully unmarshalled results.
	return entries, nil
}

// DeleteFile - Delete a file at path.
func (n *networkStorage) DeleteFile(volume, path string) (err error) {
	reply := AuthRPCReply{}
	return n.call("Storage.DeleteFileHandler", &DeleteFileArgs{
		Vol:  volume,
		Path: path,
	}, &reply)
}

// RenameFile - rename a remote file from source to destination.
func (n *networkStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	reply := AuthRPCReply{}
	return n.call("Storage.RenameFileHandler", &RenameFileArgs{
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}, &reply)
}
