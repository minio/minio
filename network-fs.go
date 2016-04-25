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
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"net/url"
	urlpath "path"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

type networkFS struct {
	netScheme  string
	netAddr    string
	netPath    string
	rpcClient  *rpc.Client
	httpClient *http.Client
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

// Initialize new network file system.
func newNetworkFS(networkPath string) (StorageAPI, error) {
	// Input validation.
	if networkPath == "" && strings.LastIndex(networkPath, ":") != -1 {
		log.WithFields(logrus.Fields{
			"networkPath": networkPath,
		}).Debugf("Network path is malformed, should be of form <ip>:<port>:<export_dir>")
		return nil, errInvalidArgument
	}

	// TODO validate netAddr and netPath.
	netAddr, netPath := splitNetPath(networkPath)

	// Dial minio rpc storage http path.
	rpcClient, err := rpc.DialHTTPPath("tcp", netAddr, storageRPCPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"netAddr":        netAddr,
			"storageRPCPath": storageRPCPath,
		}).Debugf("RPC HTTP dial failed with %s", err)
		return nil, err
	}

	// Initialize http client.
	httpClient := &http.Client{
		// Setting a sensible time out of 6minutes to wait for
		// response headers. Request is pro-actively cancelled
		// after 6minutes if no response was received from server.
		Timeout:   6 * time.Minute,
		Transport: http.DefaultTransport,
	}

	// Initialize network storage.
	ndisk := &networkFS{
		netScheme:  "http", // TODO: fix for ssl rpc support.
		netAddr:    netAddr,
		netPath:    netPath,
		rpcClient:  rpcClient,
		httpClient: httpClient,
	}

	// Returns successfully here.
	return ndisk, nil
}

// MakeVol - make a volume.
func (n networkFS) MakeVol(volume string) error {
	reply := GenericReply{}
	if err := n.rpcClient.Call("Storage.MakeVolHandler", volume, &reply); err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
		}).Debugf("Storage.MakeVolHandler returned an error %s", err)
		return toStorageErr(err)
	}
	return nil
}

// ListVols - List all volumes.
func (n networkFS) ListVols() (vols []VolInfo, err error) {
	ListVols := ListVolsReply{}
	err = n.rpcClient.Call("Storage.ListVolsHandler", "", &ListVols)
	if err != nil {
		log.Debugf("Storage.ListVolsHandler returned an error %s", err)
		return nil, err
	}
	return ListVols.Vols, nil
}

// StatVol - get current Stat volume info.
func (n networkFS) StatVol(volume string) (volInfo VolInfo, err error) {
	if err = n.rpcClient.Call("Storage.StatVolHandler", volume, &volInfo); err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
		}).Debugf("Storage.StatVolHandler returned an error %s", err)
		return VolInfo{}, toStorageErr(err)
	}
	return volInfo, nil
}

// DeleteVol - Delete a volume.
func (n networkFS) DeleteVol(volume string) error {
	reply := GenericReply{}
	if err := n.rpcClient.Call("Storage.DeleteVolHandler", volume, &reply); err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
		}).Debugf("Storage.DeleteVolHandler returned an error %s", err)
		return toStorageErr(err)
	}
	return nil
}

// File operations.

// CreateFile - create file.
func (n networkFS) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	writeURL := new(url.URL)
	writeURL.Scheme = n.netScheme
	writeURL.Host = n.netAddr
	writeURL.Path = fmt.Sprintf("%s/upload/%s", storageRPCPath, urlpath.Join(volume, path))

	contentType := "application/octet-stream"
	readCloser, writeCloser := io.Pipe()
	go func() {
		resp, err := n.httpClient.Post(writeURL.String(), contentType, readCloser)
		if err != nil {
			log.WithFields(logrus.Fields{
				"volume": volume,
				"path":   path,
			}).Debugf("CreateFile http POST failed to upload the data with error %s", err)
			readCloser.CloseWithError(err)
			return
		}
		if resp != nil {
			if resp.StatusCode != http.StatusOK {
				if resp.StatusCode == http.StatusNotFound {
					readCloser.CloseWithError(errFileNotFound)
					return
				}
				readCloser.CloseWithError(errors.New("Invalid response."))
				return
			}
			// Close the reader.
			readCloser.Close()
		}
	}()
	return writeCloser, nil
}

// StatFile - get latest Stat information for a file at path.
func (n networkFS) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	if err = n.rpcClient.Call("Storage.StatFileHandler", StatFileArgs{
		Vol:  volume,
		Path: path,
	}, &fileInfo); err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Storage.StatFileHandler failed with %s", err)
		return FileInfo{}, toStorageErr(err)
	}
	return fileInfo, nil
}

// ReadFile - reads a file.
func (n networkFS) ReadFile(volume string, path string, offset int64) (reader io.ReadCloser, err error) {
	readURL := new(url.URL)
	readURL.Scheme = n.netScheme
	readURL.Host = n.netAddr
	readURL.Path = fmt.Sprintf("%s/download/%s", storageRPCPath, urlpath.Join(volume, path))
	readQuery := make(url.Values)
	readQuery.Set("offset", strconv.FormatInt(offset, 10))
	readURL.RawQuery = readQuery.Encode()
	resp, err := n.httpClient.Get(readURL.String())
	if err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("ReadFile http Get failed with error %s", err)
		return nil, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				return nil, errFileNotFound
			}
			return nil, errors.New("Invalid response")
		}
	}
	return resp.Body, nil
}

// ListFiles - List all files in a volume.
func (n networkFS) ListFiles(volume, prefix, marker string, recursive bool, count int) (files []FileInfo, eof bool, err error) {
	listFilesReply := ListFilesReply{}
	if err = n.rpcClient.Call("Storage.ListFilesHandler", ListFilesArgs{
		Vol:       volume,
		Prefix:    prefix,
		Marker:    marker,
		Recursive: recursive,
		Count:     count,
	}, &listFilesReply); err != nil {
		log.WithFields(logrus.Fields{
			"volume":    volume,
			"prefix":    prefix,
			"marker":    marker,
			"recursive": recursive,
			"count":     count,
		}).Debugf("Storage.ListFilesHandlers failed with %s", err)
		return nil, true, toStorageErr(err)
	}
	// Return successfully unmarshalled results.
	return listFilesReply.Files, listFilesReply.EOF, nil
}

// DeleteFile - Delete a file at path.
func (n networkFS) DeleteFile(volume, path string) (err error) {
	reply := GenericReply{}
	if err = n.rpcClient.Call("Storage.DeleteFileHandler", DeleteFileArgs{
		Vol:  volume,
		Path: path,
	}, &reply); err != nil {
		log.WithFields(logrus.Fields{
			"volume": volume,
			"path":   path,
		}).Debugf("Storage.DeleteFileHandler failed with %s", err)
		return toStorageErr(err)
	}
	return nil
}
