/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"strconv"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/cpu"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mem"
)

const (
	// DefaultNetPerfSize - default payload size used for network performance.
	DefaultNetPerfSize = 100 * humanize.MiByte
	// DefaultDrivePerfSize - default file size for testing drive performance
	DefaultDrivePerfSize = 100 * humanize.MiByte
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

// DriveInfo - represents each drive info, describing
// status, uuid and endpoint.
type DriveInfo HealDriveInfo

// StorageInfo - represents total capacity of underlying storage.
type StorageInfo struct {
	Used []uint64 // Used total used per disk.

	Total []uint64 // Total disk space per disk.

	Available []uint64 // Total disk space available per disk.

	MountPaths []string // Disk mountpoints

	// Backend type.
	Backend struct {
		// Represents various backend types, currently on FS and Erasure.
		Type BackendType

		// Following fields are only meaningful if BackendType is Erasure.
		OnlineDisks      BackendDisks // Online disks during server startup.
		OfflineDisks     BackendDisks // Offline disks during server startup.
		StandardSCData   int          // Data disks for currently configured Standard storage class.
		StandardSCParity int          // Parity disks for currently configured Standard storage class.
		RRSCData         int          // Data disks for currently configured Reduced Redundancy storage class.
		RRSCParity       int          // Parity disks for currently configured Reduced Redundancy storage class.

		// List of all disk status, this is only meaningful if BackendType is Erasure.
		Sets [][]DriveInfo
	}
}

// BackendDisks - represents the map of endpoint-disks.
type BackendDisks map[string]int

// Sum - Return the sum of the disks in the endpoint-disk map.
func (d1 BackendDisks) Sum() (sum int) {
	for _, count := range d1 {
		sum += count
	}
	return sum
}

// Merge - Reduces two endpoint-disk maps.
func (d1 BackendDisks) Merge(d2 BackendDisks) BackendDisks {
	for i1, v1 := range d1 {
		if v2, ok := d2[i1]; ok {
			d2[i1] = v2 + v1
			continue
		}
		d2[i1] = v1
	}
	return d2
}

// ServerProperties holds some of the server's information such as uptime,
// version, region, ..
type ServerProperties struct {
	Uptime       time.Duration `json:"uptime"`
	Version      string        `json:"version"`
	CommitID     string        `json:"commitID"`
	DeploymentID string        `json:"deploymentID"`
	Region       string        `json:"region"`
	SQSARN       []string      `json:"sqsARN"`
}

// ServerConnStats holds network information
type ServerConnStats struct {
	TotalInputBytes  uint64 `json:"transferred"`
	TotalOutputBytes uint64 `json:"received"`
}

// ServerHTTPMethodStats holds total number of HTTP operations from/to the server,
// including the average duration the call was spent.
type ServerHTTPMethodStats struct {
	Count       uint64 `json:"count"`
	AvgDuration string `json:"avgDuration"`
}

// ServerHTTPStats holds all type of http operations performed to/from the server
// including their average execution time.
type ServerHTTPStats struct {
	TotalHEADStats     ServerHTTPMethodStats `json:"totalHEADs"`
	SuccessHEADStats   ServerHTTPMethodStats `json:"successHEADs"`
	TotalGETStats      ServerHTTPMethodStats `json:"totalGETs"`
	SuccessGETStats    ServerHTTPMethodStats `json:"successGETs"`
	TotalPUTStats      ServerHTTPMethodStats `json:"totalPUTs"`
	SuccessPUTStats    ServerHTTPMethodStats `json:"successPUTs"`
	TotalPOSTStats     ServerHTTPMethodStats `json:"totalPOSTs"`
	SuccessPOSTStats   ServerHTTPMethodStats `json:"successPOSTs"`
	TotalDELETEStats   ServerHTTPMethodStats `json:"totalDELETEs"`
	SuccessDELETEStats ServerHTTPMethodStats `json:"successDELETEs"`
}

// ServerInfoData holds storage, connections and other
// information of a given server
type ServerInfoData struct {
	StorageInfo StorageInfo      `json:"storage"`
	ConnStats   ServerConnStats  `json:"network"`
	HTTPStats   ServerHTTPStats  `json:"http"`
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
	resp, err := adm.executeMethod("GET", requestData{relPath: adminAPIPrefix + "/info"})
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

// StorageInfo - Connect to a minio server and call Storage Info Management API
// to fetch server's information represented by StorageInfo structure
func (adm *AdminClient) StorageInfo() (StorageInfo, error) {
	resp, err := adm.executeMethod("GET", requestData{relPath: adminAPIPrefix + "/storageinfo"})
	defer closeResponse(resp)
	if err != nil {
		return StorageInfo{}, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return StorageInfo{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var storageInfo StorageInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return StorageInfo{}, err
	}

	err = json.Unmarshal(respBytes, &storageInfo)
	if err != nil {
		return StorageInfo{}, err
	}

	return storageInfo, nil
}

// ServerDrivesPerfInfo holds informantion about address and write speed of
// all drives in a single server node
type ServerDrivesPerfInfo struct {
	Addr  string             `json:"addr"`
	Error string             `json:"error,omitempty"`
	Perf  []disk.Performance `json:"perf"`
	Size  int64              `json:"size,omitempty"`
}

// ServerDrivesPerfInfo - Returns drive's read and write performance information
func (adm *AdminClient) ServerDrivesPerfInfo(size int64) ([]ServerDrivesPerfInfo, error) {
	v := url.Values{}
	v.Set("perfType", string("drive"))

	v.Set("size", strconv.FormatInt(size, 10))

	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/performance",
		queryValues: v,
	})

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var info []ServerDrivesPerfInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &info)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// ServerCPULoadInfo holds information about address and cpu load of
// a single server node
type ServerCPULoadInfo struct {
	Addr         string     `json:"addr"`
	Error        string     `json:"error,omitempty"`
	Load         []cpu.Load `json:"load"`
	HistoricLoad []cpu.Load `json:"historicLoad"`
}

// ServerCPULoadInfo - Returns cpu utilization information
func (adm *AdminClient) ServerCPULoadInfo() ([]ServerCPULoadInfo, error) {
	v := url.Values{}
	v.Set("perfType", string("cpu"))
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/performance",
		queryValues: v,
	})

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var info []ServerCPULoadInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &info)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// ServerMemUsageInfo holds information about address and memory utilization of
// a single server node
type ServerMemUsageInfo struct {
	Addr          string      `json:"addr"`
	Error         string      `json:"error,omitempty"`
	Usage         []mem.Usage `json:"usage"`
	HistoricUsage []mem.Usage `json:"historicUsage"`
}

// ServerMemUsageInfo - Returns mem utilization information
func (adm *AdminClient) ServerMemUsageInfo() ([]ServerMemUsageInfo, error) {
	v := url.Values{}
	v.Set("perfType", string("mem"))
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/performance",
		queryValues: v,
	})

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var info []ServerMemUsageInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &info)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// NetPerfInfo network performance information.
type NetPerfInfo struct {
	Addr           string `json:"addr"`
	ReadThroughput uint64 `json:"readThroughput"`
	Error          string `json:"error,omitempty"`
}

// NetPerfInfo - Returns network performance information of all cluster nodes.
func (adm *AdminClient) NetPerfInfo(size int) (map[string][]NetPerfInfo, error) {
	v := url.Values{}
	v.Set("perfType", "net")
	if size > 0 {
		v.Set("size", strconv.Itoa(size))
	}
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/performance",
		queryValues: v,
	})

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	// Check response http status code
	if resp.StatusCode == http.StatusMethodNotAllowed {
		return nil, errors.New("NetPerfInfo is meant for multi-node MinIO deployments")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	info := map[string][]NetPerfInfo{}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &info)
	if err != nil {
		return nil, err
	}

	return info, nil
}
