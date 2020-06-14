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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
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
func (adm *AdminClient) StorageInfo(ctx context.Context) (StorageInfo, error) {
	resp, err := adm.executeMethod(ctx, http.MethodGet, requestData{relPath: adminAPIPrefix + "/storageinfo"})
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

// DataUsageInfo represents data usage of an Object API
type DataUsageInfo struct {
	// LastUpdate is the timestamp of when the data usage info was last updated.
	// This does not indicate a full scan.
	LastUpdate       time.Time `json:"lastUpdate"`
	ObjectsCount     uint64    `json:"objectsCount"`
	ObjectsTotalSize uint64    `json:"objectsTotalSize"`

	// ObjectsSizesHistogram contains information on objects across all buckets.
	// See ObjectsHistogramIntervals.
	ObjectsSizesHistogram map[string]uint64 `json:"objectsSizesHistogram"`

	BucketsCount uint64 `json:"bucketsCount"`

	// BucketsSizes is "bucket name" -> size.
	BucketsSizes map[string]uint64 `json:"bucketsSizes"`
}

// DataUsageInfo - returns data usage of the current object API
func (adm *AdminClient) DataUsageInfo(ctx context.Context) (DataUsageInfo, error) {
	resp, err := adm.executeMethod(ctx, http.MethodGet, requestData{relPath: adminAPIPrefix + "/datausageinfo"})
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

// FSBackend contains specific FS storage information
type FSBackend struct {
	Type backendType `json:"backendType,omitempty"`
}

// ErasureBackend contains specific erasure storage information
type ErasureBackend struct {
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
func (adm *AdminClient) ServerInfo(ctx context.Context) (InfoMessage, error) {
	resp, err := adm.executeMethod(ctx,
		http.MethodGet,
		requestData{relPath: adminAPIPrefix + "/info"},
	)
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
