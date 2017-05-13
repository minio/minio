/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// BackendType - represents different backend types.
type BackendType int

// Enum for different backend types.
const (
	Unknown BackendType = iota
	// Filesystem backend.
	FS
	// Multi disk Erasure (single, distributed) backend.
	Erasure

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
		// Represents various backend types, currently on FS and Erasure.
		Type BackendType

		// Following fields are only meaningful if BackendType is Erasure.
		OnlineDisks  int // Online disks during server startup.
		OfflineDisks int // Offline disks during server startup.
		ReadQuorum   int // Minimum disks required for successful read operations.
		WriteQuorum  int // Minimum disks required for successful write operations.
	}
}

// ServerProperties holds some of the server's information such as uptime,
// version, region, ..
type ServerProperties struct {
	Uptime   time.Duration `json:"uptime"`
	Version  string        `json:"version"`
	CommitID string        `json:"commitID"`
	Region   string        `json:"region"`
	SQSARN   []string      `json:"sqsARN"`
}

// ServerConnStats holds network information
type ServerConnStats struct {
	TotalInputBytes  uint64 `json:"transferred"`
	TotalOutputBytes uint64 `json:"received"`
}

// ServerInfoData holds storage, connections and other
// information of a given server
type ServerInfoData struct {
	StorageInfo StorageInfo      `json:"storage"`
	ConnStats   ServerConnStats  `json:"network"`
	Properties  ServerProperties `json:"server"`
}

// ServerInfo holds server information result of one node
type ServerInfo struct {
	Error string          `json:"error"`
	Addr  string          `json:"addr"`
	Data  *ServerInfoData `json:"data"`
}

// ServerInfo - Connect to a minio server and call Server Info Management API
// to fetch server's information represented by ServerInfo structure
func (adm *AdminClient) ServerInfo() ([]ServerInfo, error) {
	// Prepare web service request
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("info", "")
	reqData.customHeaders = make(http.Header)

	resp, err := adm.executeMethod("GET", reqData)
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var serversInfo []ServerInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &serversInfo)
	if err != nil {
		return nil, err
	}

	return serversInfo, nil
}
