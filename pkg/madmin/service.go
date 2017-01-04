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
 *
 */

package madmin

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

// BackendType - represents different backend types.
type BackendType int

// Enum for different backend types.
const (
	Unknown BackendType = iota
	// Filesystem backend.
	FS
	// Multi disk XL (single, distributed) backend.
	XL

	// Add your own backend.
)

// ServiceStatusMetadata - represents total capacity of underlying storage.
type ServiceStatusMetadata struct {
	// Total disk space.
	Total int64
	// Free available disk space.
	Free int64
	// Backend type.
	Backend struct {
		// Represents various backend types, currently on FS and XL.
		Type BackendType

		// Following fields are only meaningful if BackendType is XL.
		OnlineDisks  int // Online disks during server startup.
		OfflineDisks int // Offline disks during server startup.
		ReadQuorum   int // Minimum disks required for successful read operations.
		WriteQuorum  int // Minimum disks required for successful write operations.
	}
}

// ServiceStatus - Connect to a minio server and call Service Status Management API
// to fetch server's storage information represented by ServiceStatusMetadata structure
func (adm *AdminClient) ServiceStatus() (ServiceStatusMetadata, error) {

	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "status")

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("GET", reqData)

	defer closeResponse(resp)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return ServiceStatusMetadata{}, errors.New("Got HTTP Status: " + resp.Status)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	var storageInfo ServiceStatusMetadata

	err = json.Unmarshal(respBytes, &storageInfo)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	return storageInfo, nil
}

// ServiceStop - Call Service Stop Management API to stop a specified Minio server
func (adm *AdminClient) ServiceStop() error {
	//
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "stop")

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("Got HTTP Status: " + resp.Status)
	}

	return nil
}

// ServiceRestart - Call Service Restart API to restart a specified Minio server
func (adm *AdminClient) ServiceRestart() error {
	//
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "restart")

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("Got HTTP Status: " + resp.Status)
	}
	return nil
}
