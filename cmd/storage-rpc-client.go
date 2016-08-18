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
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
)

type networkStorage struct {
	netScheme string
	netAddr   string
	netPath   string
	rpcClient *RPCClient
	rpcToken  string
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
	}
	return err
}

// Login rpc client makes an authentication request to the rpc server.
// Receives a session token which will be used for subsequent requests.
// FIXME: Currently these tokens expire in 100yrs.
func loginRPCClient(rpcClient *RPCClient) (tokenStr string, err error) {
	cred := serverConfig.GetCredential()
	reply := RPCLoginReply{}
	if err = rpcClient.Call("Storage.LoginHandler", RPCLoginArgs{
		Username: cred.AccessKeyID,
		Password: cred.SecretAccessKey,
	}, &reply); err != nil {
		return "", err
	}
	if reply.ServerVersion != Version {
		return "", errors.New("Server version mismatch")
	}
	// Reply back server provided token.
	return reply.Token, nil
}

// Initialize new rpc client.
func newRPCClient(networkPath string) (StorageAPI, error) {
	// Input validation.
	if networkPath == "" || strings.LastIndex(networkPath, ":") == -1 {
		return nil, errInvalidArgument
	}

	// TODO validate netAddr and netPath.
	netAddr, netPath, err := splitNetPath(networkPath)
	if err != nil {
		return nil, err
	}

	// Dial minio rpc storage http path.
	rpcPath := path.Join(storageRPCPath, netPath)
	port := getPort(srvConfig.serverAddr)
	rpcAddr := netAddr + ":" + strconv.Itoa(port)
	// Initialize rpc client with network address and rpc path.
	rpcClient := newClient(rpcAddr, rpcPath)

	token, err := loginRPCClient(rpcClient)
	if err != nil {
		// Close the corresponding network connection w/ server to
		// avoid leaking socket file descriptor.
		rpcClient.Close()
		return nil, err
	}

	// Initialize network storage.
	ndisk := &networkStorage{
		netScheme: "http", // TODO: fix for ssl rpc support.
		netAddr:   netAddr,
		netPath:   netPath,
		rpcClient: rpcClient,
		rpcToken:  token,
	}

	// Returns successfully here.
	return ndisk, nil
}

// MakeVol - make a volume.
func (n networkStorage) MakeVol(volume string) error {
	if n.rpcClient == nil {
		return errVolumeBusy
	}
	reply := GenericReply{}
	args := GenericVolArgs{n.rpcToken, volume}
	if err := n.rpcClient.Call("Storage.MakeVolHandler", args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// ListVols - List all volumes.
func (n networkStorage) ListVols() (vols []VolInfo, err error) {
	if n.rpcClient == nil {
		return nil, errVolumeBusy
	}
	ListVols := ListVolsReply{}
	err = n.rpcClient.Call("Storage.ListVolsHandler", n.rpcToken, &ListVols)
	if err != nil {
		return nil, err
	}
	return ListVols.Vols, nil
}

// StatVol - get current Stat volume info.
func (n networkStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	if n.rpcClient == nil {
		return VolInfo{}, errVolumeBusy
	}
	args := GenericVolArgs{n.rpcToken, volume}
	if err = n.rpcClient.Call("Storage.StatVolHandler", args, &volInfo); err != nil {
		return VolInfo{}, toStorageErr(err)
	}
	return volInfo, nil
}

// DeleteVol - Delete a volume.
func (n networkStorage) DeleteVol(volume string) error {
	if n.rpcClient == nil {
		return errVolumeBusy
	}
	reply := GenericReply{}
	args := GenericVolArgs{n.rpcToken, volume}
	if err := n.rpcClient.Call("Storage.DeleteVolHandler", args, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// File operations.

// CreateFile - create file.
func (n networkStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	if n.rpcClient == nil {
		return errVolumeBusy
	}
	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.AppendFileHandler", AppendFileArgs{
		Token:  n.rpcToken,
		Vol:    volume,
		Path:   path,
		Buffer: buffer,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// StatFile - get latest Stat information for a file at path.
func (n networkStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	if n.rpcClient == nil {
		return FileInfo{}, errVolumeBusy
	}
	if err = n.rpcClient.Call("Storage.StatFileHandler", StatFileArgs{
		Token: n.rpcToken,
		Vol:   volume,
		Path:  path,
	}, &fileInfo); err != nil {
		return FileInfo{}, toStorageErr(err)
	}
	return fileInfo, nil
}

// ReadAll - reads entire contents of the file at path until EOF, returns the
// contents in a byte slice. Returns buf == nil if err != nil.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (n networkStorage) ReadAll(volume, path string) (buf []byte, err error) {
	if n.rpcClient == nil {
		return nil, errVolumeBusy
	}
	if err = n.rpcClient.Call("Storage.ReadAllHandler", ReadAllArgs{
		Token: n.rpcToken,
		Vol:   volume,
		Path:  path,
	}, &buf); err != nil {
		return nil, toStorageErr(err)
	}
	return buf, nil
}

// ReadFile - reads a file.
func (n networkStorage) ReadFile(volume string, path string, offset int64, buffer []byte) (m int64, err error) {
	if n.rpcClient == nil {
		return 0, errVolumeBusy
	}
	var result []byte
	err = n.rpcClient.Call("Storage.ReadFileHandler", ReadFileArgs{
		Token:  n.rpcToken,
		Vol:    volume,
		Path:   path,
		Offset: offset,
		Size:   len(buffer),
	}, &result)
	// Copy results to buffer.
	copy(buffer, result)
	// Return length of result, err if any.
	return int64(len(result)), toStorageErr(err)
}

// ListDir - list all entries at prefix.
func (n networkStorage) ListDir(volume, path string) (entries []string, err error) {
	if n.rpcClient == nil {
		return nil, errVolumeBusy
	}
	if err = n.rpcClient.Call("Storage.ListDirHandler", ListDirArgs{
		Token: n.rpcToken,
		Vol:   volume,
		Path:  path,
	}, &entries); err != nil {
		return nil, toStorageErr(err)
	}
	// Return successfully unmarshalled results.
	return entries, nil
}

// DeleteFile - Delete a file at path.
func (n networkStorage) DeleteFile(volume, path string) (err error) {
	if n.rpcClient == nil {
		return errVolumeBusy
	}
	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.DeleteFileHandler", DeleteFileArgs{
		Token: n.rpcToken,
		Vol:   volume,
		Path:  path,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}

// RenameFile - Rename file.
func (n networkStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	if n.rpcClient == nil {
		return errVolumeBusy
	}
	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.RenameFileHandler", RenameFileArgs{
		Token:   n.rpcToken,
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}
