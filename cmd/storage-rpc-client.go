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
	"net/url"
	"path"
	"sync/atomic"

	"github.com/minio/minio/pkg/disk"
)

type networkStorage struct {
	networkIOErrCount int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	netAddr           string
	netPath           string
	rpcClient         *AuthRPCClient
}

const (
	storageRPCPath = reservedBucket + "/storage"
)

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func toStorageErr(err error) error {
	if err == nil {
		return nil
	}

	switch err.(type) {
	case *net.OpError:
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
	case errServerVersionMismatch.Error():
		return errServerVersionMismatch
	case errServerTimeMismatch.Error():
		return errServerTimeMismatch
	}
	return err
}

// Initialize new storage rpc client.
func newStorageRPC(ep *url.URL) (StorageAPI, error) {
	if ep == nil {
		return nil, errInvalidArgument
	}

	// Dial minio rpc storage http path.
	rpcPath := path.Join(storageRPCPath, getPath(ep))
	rpcAddr := ep.Host

	// Initialize rpc client with network address and rpc path.
	accessKeyID := serverConfig.GetCredential().AccessKeyID
	secretAccessKey := serverConfig.GetCredential().SecretAccessKey
	if ep.User != nil {
		accessKeyID = ep.User.Username()
		if key, set := ep.User.Password(); set {
			secretAccessKey = key
		}
	}
	rpcClient := newAuthClient(&authConfig{
		accessKey:   accessKeyID,
		secretKey:   secretAccessKey,
		secureConn:  isSSL(),
		address:     rpcAddr,
		path:        rpcPath,
		loginMethod: "Storage.LoginHandler",
	})

	// Initialize network storage.
	ndisk := &networkStorage{
		netAddr:   ep.Host,
		netPath:   getPath(ep),
		rpcClient: rpcClient,
	}

	// Returns successfully here.
	return ndisk, nil
}

// Stringer interface compatible representation of network device.
func (n *networkStorage) String() string {
	return n.netAddr + ":" + n.netPath
}

// maximum allowed network IOError.
const maxAllowedNetworkIOError = 1024

// Initializes the remote RPC connection by attempting a login attempt.
func (n *networkStorage) Init() (err error) {
	// Attempt a login to reconnect.
	return n.rpcClient.Login()
}

// Closes the underlying RPC connection.
func (n *networkStorage) Close() (err error) {
	// Close the underlying connection.
	return n.rpcClient.Close()
}

// DiskInfo - fetch disk information for a remote disk.
func (n *networkStorage) DiskInfo() (info disk.Info, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return disk.Info{}, errFaultyRemoteDisk
	}

	args := GenericArgs{}
	if err = n.rpcClient.Call("Storage.DiskInfoHandler", &args, &info); err != nil {
		return disk.Info{}, toStorageErr(err)
	}
	return info, nil
}

// MakeVol - create a volume on a remote disk.
func (n *networkStorage) MakeVol(volume string) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	args := GenericVolArgs{Vol: volume}
	if err := n.rpcClient.Call("Storage.MakeVolHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// ListVols - List all volumes on a remote disk.
func (n *networkStorage) ListVols() (vols []VolInfo, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	ListVols := ListVolsReply{}
	err = n.rpcClient.Call("Storage.ListVolsHandler", &GenericArgs{}, &ListVols)
	if err != nil {
		return nil, toStorageErr(err)
	}
	return ListVols.Vols, nil
}

// StatVol - get volume info over the network.
func (n *networkStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return VolInfo{}, errFaultyRemoteDisk
	}

	args := GenericVolArgs{Vol: volume}
	if err = n.rpcClient.Call("Storage.StatVolHandler", &args, &volInfo); err != nil {
		return VolInfo{}, toStorageErr(err)
	}
	return volInfo, nil
}

// DeleteVol - Deletes a volume over the network.
func (n *networkStorage) DeleteVol(volume string) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	args := GenericVolArgs{Vol: volume}
	if err := n.rpcClient.Call("Storage.DeleteVolHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// File operations.

func (n *networkStorage) PrepareFile(volume, path string, length int64) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()
	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.PrepareFileHandler", &PrepareFileArgs{
		Vol:  volume,
		Path: path,
		Size: length,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// AppendFile - append file writes buffer to a remote network path.
func (n *networkStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.AppendFileHandler", &AppendFileArgs{
		Vol:    volume,
		Path:   path,
		Buffer: buffer,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// StatFile - get latest Stat information for a file at path.
func (n *networkStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return FileInfo{}, errFaultyRemoteDisk
	}

	if err = n.rpcClient.Call("Storage.StatFileHandler", &StatFileArgs{
		Vol:  volume,
		Path: path,
	}, &fileInfo); err != nil {
		return FileInfo{}, toStorageErr(err)
	}
	return fileInfo, nil
}

// ReadAll - reads entire contents of the file at path until EOF, returns the
// contents in a byte slice. Returns buf == nil if err != nil.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (n *networkStorage) ReadAll(volume, path string) (buf []byte, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	if err = n.rpcClient.Call("Storage.ReadAllHandler", &ReadAllArgs{
		Vol:  volume,
		Path: path,
	}, &buf); err != nil {
		return nil, toStorageErr(err)
	}
	return buf, nil
}

// ReadFile - reads a file at remote path and fills the buffer.
func (n *networkStorage) ReadFile(volume string, path string, offset int64, buffer []byte) (m int64, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	defer func() {
		if r := recover(); r != nil {
			// Recover any panic from allocation, and return error.
			err = bytes.ErrTooLarge
		}
	}() // Do not crash the server.

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return 0, errFaultyRemoteDisk
	}

	var result []byte
	err = n.rpcClient.Call("Storage.ReadFileHandler", &ReadFileArgs{
		Vol:    volume,
		Path:   path,
		Offset: offset,
		Buffer: buffer,
	}, &result)

	// Copy results to buffer.
	copy(buffer, result)

	// Return length of result, err if any.
	return int64(len(result)), toStorageErr(err)
}

// ListDir - list all entries at prefix.
func (n *networkStorage) ListDir(volume, path string) (entries []string, err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	if err = n.rpcClient.Call("Storage.ListDirHandler", &ListDirArgs{
		Vol:  volume,
		Path: path,
	}, &entries); err != nil {
		return nil, toStorageErr(err)
	}
	// Return successfully unmarshalled results.
	return entries, nil
}

// DeleteFile - Delete a file at path.
func (n *networkStorage) DeleteFile(volume, path string) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.DeleteFileHandler", &DeleteFileArgs{
		Vol:  volume,
		Path: path,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// RenameFile - rename a remote file from source to destination.
func (n *networkStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	defer func() {
		if err == errDiskNotFound || err == rpc.ErrShutdown {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.RenameFileHandler", &RenameFileArgs{
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}
