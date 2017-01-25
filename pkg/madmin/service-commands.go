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
	"bytes"
	"encoding/json"
	"encoding/xml"
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

// StorageInfo - represents total capacity of underlying storage.
type StorageInfo struct {
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

// ServerVersion - server version
type ServerVersion struct {
	Version  string `json:"version"`
	CommitID string `json:"commitID"`
}

// ServiceStatusMetadata - contains the response of service status API
type ServiceStatusMetadata struct {
	StorageInfo   StorageInfo   `json:"storageInfo"`
	ServerVersion ServerVersion `json:"serverVersion"`
}

// ServiceStatus - Connect to a minio server and call Service Status Management API
// to fetch server's storage information represented by ServiceStatusMetadata structure
func (adm *AdminClient) ServiceStatus() (ServiceStatusMetadata, error) {

	// Prepare web service request
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

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return ServiceStatusMetadata{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var serviceStatus ServiceStatusMetadata

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	err = json.Unmarshal(respBytes, &serviceStatus)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	return serviceStatus, nil
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
		return httpRespToErrorResponse(resp)
	}
	return nil
}

// setCredsReq - xml to send to the server to set new credentials
type setCredsReq struct {
	Username string `xml:"username"`
	Password string `xml:"password"`
}

// ServiceSetCredentials - Call Service Set Credentials API to set new access and secret keys in the specified Minio server
func (adm *AdminClient) ServiceSetCredentials(access, secret string) error {

	// Disallow sending with the server if the connection is not secure
	if !adm.secure {
		return errors.New("setting new credentials requires HTTPS connection to the server")
	}

	// Setup new request
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "set-credentials")

	// Setup request's body
	body, err := xml.Marshal(setCredsReq{Username: access, Password: secret})
	if err != nil {
		return err
	}
	reqData.contentBody = bytes.NewReader(body)
	reqData.contentLength = int64(len(body))
	reqData.contentMD5Bytes = sumMD5(body)
	reqData.contentSHA256Bytes = sum256(body)

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	// Return error to the caller if http response code is different from 200
	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}
