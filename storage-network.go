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
	"io"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type networkStorage struct {
	address    string
	connection *rpc.Client
	httpClient *http.Client
}

const (
	connected       = "200 Connected to Go RPC"
	dialTimeoutSecs = 30 // 30 seconds.
)

// Initialize new network storage.
func newNetworkStorage(address string) (StorageAPI, error) {
	// Dial to the address with timeout of 30secs, this includes DNS resolution.
	conn, err := net.DialTimeout("tcp", address, dialTimeoutSecs*time.Second)
	if err != nil {
		return nil, err
	}

	// Initialize rpc client with dialed connection.
	rpcClient := rpc.NewClient(conn)

	// Initialize http client.
	httpClient := &http.Client{
		// Setting a sensible time out of 2minutes to wait for
		// response headers. Request is pro-actively cancelled
		// after 2minutes if no response was received from server.
		Timeout:   2 * time.Minute,
		Transport: http.DefaultTransport,
	}

	// Initialize network storage.
	ndisk := &networkStorage{
		address:    address,
		connection: rpcClient,
		httpClient: httpClient,
	}

	// Returns successfully here.
	return ndisk, nil
}

// MakeVol - make a volume.
func (n networkStorage) MakeVol(volume string) error {
	reply := GenericReply{}
	return n.connection.Call("Storage.MakeVolHandler", volume, &reply)
}

// ListVols - List all volumes.
func (n networkStorage) ListVols() (vols []VolInfo, err error) {
	ListVols := ListVolsReply{}
	err = n.connection.Call("Storage.ListVolsHandler", "", &ListVols)
	if err != nil {
		return nil, err
	}
	return ListVols.Vols, nil
}

// StatVol - get current Stat volume info.
func (n networkStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	if err = n.connection.Call("Storage.StatVolHandler", volume, &volInfo); err != nil {
		return VolInfo{}, err
	}
	return volInfo, nil
}

// DeleteVol - Delete a volume.
func (n networkStorage) DeleteVol(volume string) error {
	reply := GenericReply{}
	return n.connection.Call("Storage.DeleteVolHandler", volume, &reply)
}

// File operations.

// CreateFile - create file.
func (n networkStorage) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	createFileReply := CreateFileReply{}
	if err = n.connection.Call("Storage.CreateFileHandler", CreateFileArgs{
		Vol:  volume,
		Path: path,
	}, &createFileReply); err != nil {
		return nil, err
	}
	contentType := "application/octet-stream"
	readCloser, writeCloser := io.Pipe()
	defer readCloser.Close()
	go n.httpClient.Post(createFileReply.URL, contentType, readCloser)
	return writeCloser, nil
}

// StatFile - get latest Stat information for a file at path.
func (n networkStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	if err = n.connection.Call("Storage.StatFileHandler", StatFileArgs{
		Vol:  volume,
		Path: path,
	}, &fileInfo); err != nil {
		return FileInfo{}, err
	}
	return fileInfo, nil
}

// ReadFile - reads a file.
func (n networkStorage) ReadFile(volume string, path string, offset int64) (reader io.ReadCloser, err error) {
	readFileReply := ReadFileReply{}
	if err = n.connection.Call("Storage.ReadFileHandler", ReadFileArgs{
		Vol:    volume,
		Path:   path,
		Offset: offset,
	}, &readFileReply); err != nil {
		return nil, err
	}
	resp, err := n.httpClient.Get(readFileReply.URL)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("Invalid response")
	}
	return resp.Body, nil
}

// ListFiles - List all files in a volume.
func (n networkStorage) ListFiles(volume, prefix, marker string, recursive bool, count int) (files []FileInfo, eof bool, err error) {
	listFilesReply := ListFilesReply{}
	if err = n.connection.Call("Storage.ListFilesHandler", ListFilesArgs{
		Vol:       volume,
		Prefix:    prefix,
		Marker:    marker,
		Recursive: recursive,
		Count:     count,
	}, &listFilesReply); err != nil {
		return nil, true, err
	}
	// List of files.
	files = listFilesReply.Files
	// EOF.
	eof = listFilesReply.EOF
	return files, eof, nil
}

// DeleteFile - Delete a file at path.
func (n networkStorage) DeleteFile(volume, path string) (err error) {
	reply := GenericReply{}
	if err = n.connection.Call("Storage.DeleteFileHandler", DeleteFileArgs{
		Vol:  volume,
		Path: path,
	}, &reply); err != nil {
		return err
	}
	return nil
}
