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
	"encoding/json"
	"io"
	"net"
	"net/rpc"
	"net/url"
	"path"
	"sync/atomic"
	"time"

	"github.com/minio/minio/pkg/disk"
)

type networkStorage struct {
	networkIOErrCount int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	rpcClient         *AuthRPCClient
	retryCfg          retryConfig
	verifyServerFunc  func() error
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

	if err == rpc.ErrShutdown {
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

var defaultStorageRetryConfig = retryConfig{
	isRetryableErr: func(err error) bool {
		return err == rpc.ErrShutdown || err == errDiskNotFound
	},
	maxAttempts:  1,
	timerUnit:    time.Millisecond,
	timerMaxTime: 5 * time.Millisecond,
}

// Initialize new storage rpc client.
func newStorageRPC(ep *url.URL, rCfg retryConfig) (StorageAPI, error) {
	if ep == nil {
		return nil, errInvalidArgument
	}

	// Dial minio rpc storage http path.
	rpcPath := path.Join(storageRPCPath, getPath(ep))
	rpcAddr := ep.Host

	serverCred := serverConfig.GetCredential()
	accessKey := serverCred.AccessKey
	secretKey := serverCred.SecretKey
	if ep.User != nil {
		accessKey = ep.User.Username()
		if password, ok := ep.User.Password(); ok {
			secretKey = password
		}
	}

	netStorage := &networkStorage{}
	netStorage.rpcClient = newAuthRPCClient(authConfig{
		accessKey:       accessKey,
		secretKey:       secretKey,
		serverAddr:      rpcAddr,
		serviceEndpoint: rpcPath,
		secureConn:      isSSL(),
		serviceName:     "Storage",
	})
	netStorage.retryCfg = rCfg
	netStorage.verifyServerFunc = func() error {
		// FIXME: This is basic server validation check by reading format.json.
		//        However there could be many possible misconfiguration currently
		//        we don't handle.
		// Read format config file to validate the server.
		buf, err := netStorage.readAll(minioMetaBucket, formatConfigFile, nil)
		if err != nil {
			// Unmarshal read config file.
			format := formatConfigV1{}
			err = json.Unmarshal(buf, &format)
		}

		return err
	}

	// Returns successfully here.
	return netStorage, nil
}

// Stringer interface compatible representation of network device.
func (n *networkStorage) String() string {
	return n.rpcClient.ServerAddr() + ":" + n.rpcClient.ServiceEndpoint()
}

// Network IO error count is kept at 256 with some simple
// math. Before we reject the disk completely. The combination
// of retry logic and total error count roughly comes around
// 2.5secs ( 2 * 5 * time.Millisecond * 256) which is when we
// basically take the disk offline completely. This is considered
// sufficient time tradeoff to avoid large delays in-terms of
// incoming i/o.
const maxAllowedNetworkIOError = 256 // maximum allowed network IOError.

// call executes RPC call with default verify server function and retry config.
func (n *networkStorage) call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
	SetRequestTime(requestTime time.Time)
}, reply interface{}) (err error) {
	return n.rpcClient.Call(serviceMethod, args, reply, n.verifyServerFunc, n.retryCfg)
}

// DiskInfo - fetch disk information for a remote disk.
func (n *networkStorage) DiskInfo() (info disk.Info, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return disk.Info{}, errFaultyRemoteDisk
	}

	args := AuthRPCArgs{}
	if err = n.call("Storage.DiskInfoHandler", &args, &info); err != nil {
		return disk.Info{}, toStorageErr(err)
	}
	return info, nil
}

// MakeVol - create a volume on a remote disk.
func (n *networkStorage) MakeVol(volume string) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := GenericVolArgs{Vol: volume}
	reply := AuthRPCReply{}
	if err := n.call("Storage.MakeVolHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// ListVols - List all volumes on a remote disk.
func (n *networkStorage) ListVols() (vols []VolInfo, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	args := AuthRPCArgs{}
	ListVols := ListVolsReply{}
	err = n.call("Storage.ListVolsHandler", &args, &ListVols)
	if err != nil {
		return nil, toStorageErr(err)
	}
	return ListVols.Vols, nil
}

// StatVol - get volume info over the network.
func (n *networkStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return VolInfo{}, errFaultyRemoteDisk
	}

	args := GenericVolArgs{Vol: volume}
	if err = n.call("Storage.StatVolHandler", &args, &volInfo); err != nil {
		return VolInfo{}, toStorageErr(err)
	}
	return volInfo, nil
}

// DeleteVol - Deletes a volume over the network.
func (n *networkStorage) DeleteVol(volume string) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := GenericVolArgs{Vol: volume}
	reply := AuthRPCReply{}
	if err := n.call("Storage.DeleteVolHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// File operations.

func (n *networkStorage) PrepareFile(volume, path string, length int64) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()
	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := PrepareFileArgs{Vol: volume, Path: path, Size: length}
	reply := AuthRPCReply{}
	if err = n.call("Storage.PrepareFileHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// AppendFile - append file writes buffer to a remote network path.
func (n *networkStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := AppendFileArgs{Vol: volume, Path: path, Buffer: buffer}
	reply := AuthRPCReply{}
	if err = n.call("Storage.AppendFileHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// StatFile - get latest Stat information for a file at path.
func (n *networkStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return FileInfo{}, errFaultyRemoteDisk
	}

	args := StatFileArgs{Vol: volume, Path: path}
	if err = n.call("Storage.StatFileHandler", &args, &fileInfo); err != nil {
		return FileInfo{}, toStorageErr(err)
	}
	return fileInfo, nil
}

// ReadAll - reads entire contents of the file at path until EOF, returns the
// contents in a byte slice. Returns buf == nil if err != nil.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (n *networkStorage) readAll(volume, path string, verifyServerFunc func() error) (buf []byte, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	args := ReadAllArgs{Vol: volume, Path: path}
	if err = n.rpcClient.Call("Storage.ReadAllHandler", &args, &buf, verifyServerFunc, n.retryCfg); err != nil {
		return nil, toStorageErr(err)
	}
	return buf, nil
}

func (n *networkStorage) ReadAll(volume, path string) (buf []byte, err error) {
	return n.readAll(volume, path, n.verifyServerFunc)
}

// ReadFile - reads a file at remote path and fills the buffer.
func (n *networkStorage) ReadFile(volume string, path string, offset int64, buffer []byte) (m int64, err error) {
	defer func() {
		if err == errDiskNotFound {
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
	args := ReadFileArgs{Vol: volume, Path: path, Offset: offset, Buffer: buffer}
	err = n.call("Storage.ReadFileHandler", &args, &result)

	// Copy results to buffer.
	copy(buffer, result)

	// Return length of result, err if any.
	return int64(len(result)), toStorageErr(err)
}

// ListDir - list all entries at prefix.
func (n *networkStorage) ListDir(volume, path string) (entries []string, err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return nil, errFaultyRemoteDisk
	}

	args := ListDirArgs{Vol: volume, Path: path}
	if err = n.call("Storage.ListDirHandler", &args, &entries); err != nil {
		return nil, toStorageErr(err)
	}
	// Return successfully unmarshalled results.
	return entries, nil
}

// DeleteFile - Delete a file at path.
func (n *networkStorage) DeleteFile(volume, path string) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := DeleteFileArgs{Vol: volume, Path: path}
	reply := AuthRPCReply{}
	if err = n.call("Storage.DeleteFileHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// RenameFile - rename a remote file from source to destination.
func (n *networkStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	defer func() {
		if err == errDiskNotFound {
			atomic.AddInt32(&n.networkIOErrCount, 1)
		}
	}()

	// Take remote disk offline if the total network errors.
	// are more than maximum allowable IO error limit.
	if n.networkIOErrCount > maxAllowedNetworkIOError {
		return errFaultyRemoteDisk
	}

	args := RenameFileArgs{
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}
	reply := AuthRPCReply{}
	if err = n.call("Storage.RenameFileHandler", &args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}
