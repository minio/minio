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

package main

import (
	"errors"
	"net/rpc"
	"path"
	"strconv"
	"strings"
)

type networkStorage struct {
	netScheme string
	netAddr   string
	netPath   string
	rpcClient *rpc.Client
	rpcToken  string
}

const (
	storageRPCPath = reservedBucket + "/storage"
)

// splits network path into its components Address and Path.
func splitNetPath(networkPath string) (netAddr, netPath string) {
	index := strings.LastIndex(networkPath, ":")
	netAddr = networkPath[:index]
	netPath = networkPath[index+1:]
	return netAddr, netPath
}

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func toStorageErr(err error) error {
	switch err.Error() {
	case errDiskFull.Error():
		return errDiskFull
	case errVolumeNotFound.Error():
		return errVolumeNotFound
	case errVolumeExists.Error():
		return errVolumeExists
	case errFileNotFound.Error():
		return errFileNotFound
	case errIsNotRegular.Error():
		return errIsNotRegular
	case errVolumeNotEmpty.Error():
		return errVolumeNotEmpty
	case errFileAccessDenied.Error():
		return errFileAccessDenied
	case errVolumeAccessDenied.Error():
		return errVolumeAccessDenied
	}
	return err
}

// Login rpc client makes an authentication request to the rpc server.
// Receives a session token which will be used for subsequent requests.
// FIXME: Currently these tokens expire in 100yrs.
func loginRPCClient(rpcClient *rpc.Client) (tokenStr string, err error) {
	cred := serverConfig.GetCredential()
	reply := RPCLoginReply{}
	if err = rpcClient.Call("Storage.LoginHandler", RPCLoginArgs{
		Username: cred.AccessKeyID,
		Password: cred.SecretAccessKey,
	}, &reply); err != nil {
		return "", err
	}
	if reply.ServerVersion != minioVersion {
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
	netAddr, netPath := splitNetPath(networkPath)

	// Dial minio rpc storage http path.
	rpcPath := path.Join(storageRPCPath, netPath)
	port := getPort(srvConfig.serverAddr)
	rpcAddr := netAddr + ":" + strconv.Itoa(port)
	rpcClient, err := rpc.DialHTTPPath("tcp", rpcAddr, rpcPath)
	if err != nil {
		return nil, err
	}

	token, err := loginRPCClient(rpcClient)
	if err != nil {
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
	if err = n.rpcClient.Call("Storage.StatVolHandler", volume, &volInfo); err != nil {
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
	if err := n.rpcClient.Call("Storage.DeleteVolHandler", volume, &reply); err != nil {
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
	if err = n.rpcClient.Call("Storage.StatFileHandler", StatFileArgs{
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
func (n networkStorage) ReadAll(volume, path string) (buf []byte, err error) {
	if err = n.rpcClient.Call("Storage.ReadAllHandler", ReadAllArgs{
		Vol:  volume,
		Path: path,
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
	if err = n.rpcClient.Call("Storage.ReadFileHandler", ReadFileArgs{
		Vol:    volume,
		Path:   path,
		Offset: offset,
		Buffer: buffer,
	}, &m); err != nil {
		return 0, toStorageErr(err)
	}
	return m, nil
}

// ListDir - list all entries at prefix.
func (n networkStorage) ListDir(volume, path string) (entries []string, err error) {
	if n.rpcClient == nil {
		return nil, errVolumeBusy
	}
	if err = n.rpcClient.Call("Storage.ListDirHandler", ListDirArgs{
		Vol:  volume,
		Path: path,
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
		Vol:  volume,
		Path: path,
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
		SrcVol:  srcVolume,
		SrcPath: srcPath,
		DstVol:  dstVolume,
		DstPath: dstPath,
	}, &reply); err != nil {
		return toStorageErr(err)
	}
	return nil
}
