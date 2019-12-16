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
	if len(d2) == 0 {
		d2 = make(BackendDisks)
	}
	for i1, v1 := range d1 {
		if v2, ok := d2[i1]; ok {
			d2[i1] = v2 + v1
			continue
		}
		d2[i1] = v1
	}
	return d2
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

type objectHistogramInterval struct {
	name       string
	start, end int64
}

// ObjectsHistogramIntervals contains the list of intervals
// of an histogram analysis of objects sizes.
var ObjectsHistogramIntervals = []objectHistogramInterval{
	{"LESS_THAN_1024_B", -1, 1024 - 1},
	{"BETWEEN_1024_B_AND_1_MB", 1024, 1024*1024 - 1},
	{"BETWEEN_1_MB_AND_10_MB", 1024 * 1024, 1024*1024*10 - 1},
	{"BETWEEN_10_MB_AND_64_MB", 1024 * 1024 * 10, 1024*1024*64 - 1},
	{"BETWEEN_64_MB_AND_128_MB", 1024 * 1024 * 64, 1024*1024*128 - 1},
	{"BETWEEN_128_MB_AND_512_MB", 1024 * 1024 * 128, 1024*1024*512 - 1},
	{"GREATER_THAN_512_MB", 1024 * 1024 * 512, -1},
}

// DataUsageInfo represents data usage of an Object API
type DataUsageInfo struct {
	LastUpdate            time.Time         `json:"lastUpdate"`
	ObjectsCount          uint64            `json:"objectsCount"`
	ObjectsTotalSize      uint64            `json:"objectsTotalSize"`
	ObjectsSizesHistogram map[string]uint64 `json:"objectsSizesHistogram"`

	BucketsCount uint64            `json:"bucketsCount"`
	BucketsSizes map[string]uint64 `json:"bucketsSizes"`
}

// DataUsageInfo - returns data usage of the current object API
func (adm *AdminClient) DataUsageInfo() (DataUsageInfo, error) {
	resp, err := adm.executeMethod("GET", requestData{relPath: adminAPIPrefix + "/datausageinfo"})
	defer closeResponse(resp)
	if err != nil {
		return DataUsageInfo{}, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return DataUsageInfo{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var dataUsageInfo DataUsageInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return DataUsageInfo{}, err
	}

	err = json.Unmarshal(respBytes, &dataUsageInfo)
	if err != nil {
		return DataUsageInfo{}, err
	}

	return dataUsageInfo, nil
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

// InfoMessage container to hold server admin related information.
type InfoMessage struct {
	Mode         string             `json:"mode,omitempty"`
	Domain       []string           `json:"domain,omitempty"`
	Region       string             `json:"region,omitempty"`
	SQSARN       []string           `json:"sqsARN,omitempty"`
	DeploymentID string             `json:"deploymentID,omitempty"`
	Buckets      Buckets            `json:"buckets,omitempty"`
	Objects      Objects            `json:"objects,omitempty"`
	Usage        Usage              `json:"usage,omitempty"`
	Services     Services           `json:"services,omitempty"`
	Backend      interface{}        `json:"backend,omitempty"`
	Servers      []ServerProperties `json:"servers,omitempty"`
}

// Services contains different services information
type Services struct {
	Vault         Vault                         `json:"vault,omitempty"`
	LDAP          LDAP                          `json:"ldap,omitempty"`
	Logger        []Logger                      `json:"logger,omitempty"`
	Audit         []Audit                       `json:"audit,omitempty"`
	Notifications []map[string][]TargetIDStatus `json:"notifications,omitempty"`
}

// Buckets contains the number of buckets
type Buckets struct {
	Count uint64 `json:"count,omitempty"`
}

// Objects contains the number of objects
type Objects struct {
	Count uint64 `json:"count,omitempty"`
}

// Usage contains the tottal size used
type Usage struct {
	Size uint64 `json:"size,omitempty"`
}

// Vault - Fetches the Vault status
type Vault struct {
	Status  string `json:"status,omitempty"`
	Encrypt string `json:"encryp,omitempty"`
	Decrypt string `json:"decrypt,omitempty"`
	Update  string `json:"update,omitempty"`
}

// LDAP contains ldap status
type LDAP struct {
	Status string `json:"status,omitempty"`
}

// Status of endpoint
type Status struct {
	Status string `json:"status,omitempty"`
}

// Audit contains audit logger status
type Audit map[string]Status

// Logger contains logger status
type Logger map[string]Status

// TargetIDStatus containsid and status
type TargetIDStatus map[string]Status

// backendType - indicates the type of backend storage
type backendType string

const (
	// FsType - Backend is FS Type
	FsType = backendType("FS")
	// ErasureType - Backend is Erasure type
	ErasureType = backendType("Erasure")
)

// FsBackend contains specific FS storage information
type FsBackend struct {
	Type backendType `json:"backendType,omitempty"`
}

// XlBackend contains specific erasure storage information
type XlBackend struct {
	Type         backendType `json:"backendType,omitempty"`
	OnlineDisks  int         `json:"onlineDisks,omitempty"`
	OfflineDisks int         `json:"offlineDisks,omitempty"`
	// Data disks for currently configured Standard storage class.
	StandardSCData int `json:"standardSCData,omitempty"`
	// Parity disks for currently configured Standard storage class.
	StandardSCParity int `json:"standardSCParity,omitempty"`
	// Data disks for currently configured Reduced Redundancy storage class.
	RRSCData int `json:"rrSCData,omitempty"`
	// Parity disks for currently configured Reduced Redundancy storage class.
	RRSCParity int `json:"rrSCParity,omitempty"`
}

// ServerProperties holds server information
type ServerProperties struct {
	State    string            `json:"state,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	Uptime   int64             `json:"uptime,omitempty"`
	Version  string            `json:"version,omitempty"`
	CommitID string            `json:"commitID,omitempty"`
	Network  map[string]string `json:"network,omitempty"`
	Disks    []Disk            `json:"disks,omitempty"`
}

// Disk holds Disk information
type Disk struct {
	DrivePath       string  `json:"path,omitempty"`
	State           string  `json:"state,omitempty"`
	UUID            string  `json:"uuid,omitempty"`
	Model           string  `json:"model,omitempty"`
	TotalSpace      uint64  `json:"totalspace,omitempty"`
	UsedSpace       uint64  `json:"usedspace,omitempty"`
	ReadThroughput  float64 `json:"readthroughput,omitempty"`
	WriteThroughPut float64 `json:"writethroughput,omitempty"`
	ReadLatency     float64 `json:"readlatency,omitempty"`
	WriteLatency    float64 `json:"writelatency,omitempty"`
	Utilization     float64 `json:"utilization,omitempty"`
}

// ServerInfo - Connect to a minio server and call Server Admin Info Management API
// to fetch server's information represented by infoMessage structure
func (adm *AdminClient) ServerInfo() (InfoMessage, error) {

	resp, err := adm.executeMethod("GET", requestData{relPath: adminAPIPrefix + "/info"})
	defer closeResponse(resp)
	if err != nil {
		return InfoMessage{}, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return InfoMessage{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var message InfoMessage

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return InfoMessage{}, err
	}

	err = json.Unmarshal(respBytes, &message)
	if err != nil {
		return InfoMessage{}, err
	}

	return message, nil
}
