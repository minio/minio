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
	"net/http"
	"runtime"
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
	// Gateway to other storage
	Gateway

	// Add your own backend.
)

// ItemState - represents the status of any item in offline,init,online state
type ItemState string

const (

	// ItemOffline indicates that the item is offline
	ItemOffline = ItemState("offline")
	// ItemInitializing indicates that the item is still in initialization phase
	ItemInitializing = ItemState("initializing")
	// ItemOnline indicates that the item is online
	ItemOnline = ItemState("online")
)

// StorageInfo - represents total capacity of underlying storage.
type StorageInfo struct {
	Disks []Disk

	// Backend type.
	Backend BackendInfo
}

// BackendInfo - contains info of the underlying backend
type BackendInfo struct {
	OnlineDisks  BackendDisks
	OfflineDisks BackendDisks
	RRSCData     []int // Following fields are only meaningful if BackendType is Erasure.

	StandardSCData []int // Following fields are only meaningful if BackendType is Erasure.

	RRSCParity       int
	StandardSCParity int
	Type             BackendType
	GatewayOnline    bool // Represents various backend types, currently on FS, Erasure and Gateway
	// Following fields are only meaningful if BackendType is Gateway.
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
	var merged = make(BackendDisks)
	for i1, v1 := range d1 {
		if v2, ok := d2[i1]; ok {
			merged[i1] = v2 + v1
			continue
		}
		merged[i1] = v1
	}
	return merged
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
	if err = json.NewDecoder(resp.Body).Decode(&storageInfo); err != nil {
		return StorageInfo{}, err
	}

	return storageInfo, nil
}

// BucketUsageInfo - bucket usage info provides
// - total size of the bucket
// - total objects in a bucket
// - object size histogram per bucket
type BucketUsageInfo struct {
	ObjectSizesHistogram    map[string]uint64 `json:"objectsSizesHistogram"`
	Size                    uint64            `json:"size"`
	ReplicationPendingSize  uint64            `json:"objectsPendingReplicationTotalSize"`
	ReplicatedSize          uint64            `json:"objectsReplicatedTotalSize"`
	ReplicaSize             uint64            `json:"objectReplicaTotalSize"`
	ReplicationPendingCount uint64            `json:"objectsPendingReplicationCount"`
	ReplicationFailedCount  uint64            `json:"objectsFailedReplicationCount"`
	ObjectsCount            uint64            `json:"objectsCount"`
	ReplicationFailedSize   uint64            `json:"objectsFailedReplicationTotalSize"`
}

// DataUsageInfo represents data usage stats of the underlying Object API
type DataUsageInfo struct {
	LastUpdate   time.Time `json:"lastUpdate"`
	BucketsUsage map[      // LastUpdate is the timestamp of when the data usage info was last updated.
	// This does not indicate a full scan.
	// Buckets usage info provides following information across all buckets
	// - total size of the bucket
	// - total objects in a bucket
	// - object size histogram per bucket
	string]BucketUsageInfo `json:"bucketsUsageInfo"`
	BucketSizes   map[ // Deprecated kept here for backward compatibility reasons.
	string]uint64      `json:"bucketsSizes"`
	ObjectsTotalCount       uint64 `json:"objectsCount"`
	ObjectsTotalSize        uint64 `json:"objectsTotalSize"`
	ReplicationPendingSize  uint64 `json:"objectsPendingReplicationTotalSize"`
	ReplicaSize             uint64 `json:"objectsReplicaTotalSize"`
	ReplicationPendingCount uint64 `json:"objectsPendingReplicationCount"`
	ReplicationFailedCount  uint64 `json:"objectsFailedReplicationCount"`
	BucketsCount            uint64 `json:"bucketsCount"`
	ReplicationFailedSize   uint64 `json:"objectsFailedReplicationTotalSize"`
	ReplicatedSize          uint64 `json:"objectsReplicatedTotalSize"` // Objects total count across all buckets
	// Total size for objects that have been replicated to destination
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
	if err = json.NewDecoder(resp.Body).Decode(&dataUsageInfo); err != nil {
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
	KMS           KMS                           `json:"kms,omitempty"`
	LDAP          LDAP                          `json:"ldap,omitempty"`
	Logger        []Logger                      `json:"logger,omitempty"`
	Audit         []Audit                       `json:"audit,omitempty"`
	Notifications []map[string][]TargetIDStatus `json:"notifications,omitempty"`
}

// Buckets contains the number of buckets
type Buckets struct {
	Error string `json:"error,omitempty"`
	Count uint64 `json:"count"`
}

// Objects contains the number of objects
type Objects struct {
	Error string `json:"error,omitempty"`
	Count uint64 `json:"count"`
}

// Usage contains the total size used
type Usage struct {
	Error string `json:"error,omitempty"`
	Size  uint64 `json:"size"`
}

// KMS contains KMS status information
type KMS struct {
	Status  string `json:"status,omitempty"`
	Encrypt string `json:"encrypt,omitempty"`
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
	// Parity disks for currently configured Standard storage class.
	StandardSCParity int `json:"standardSCParity,omitempty"`
	// Parity disks for currently configured Reduced Redundancy storage class.
	RRSCParity int `json:"rrSCParity,omitempty"`
}

// ServerProperties holds server information
type ServerProperties struct {
	Network    map[string]string `json:"network,omitempty"`
	State      string            `json:"state,omitempty"`
	Endpoint   string            `json:"endpoint,omitempty"`
	Version    string            `json:"version,omitempty"`
	CommitID   string            `json:"commitID,omitempty"`
	Disks      []Disk            `json:"drives,omitempty"`
	MemStats   runtime.MemStats  `json:"mem_stats"`
	PoolNumber int               `json:"poolNumber,omitempty"`
	Uptime     int64             `json:"uptime,omitempty"`
}

// DiskMetrics has the information about XL Storage APIs
// the number of calls of each API and the moving average of
// the duration of each API.
type DiskMetrics struct {
	APILatencies map[string]string `json:"apiLatencies,omitempty"`
	APICalls     map[string]uint64 `json:"apiCalls,omitempty"`
}

// Disk holds Disk information
type Disk struct {
	HealInfo        *HealingDisk `json:"heal_info,omitempty"`
	Metrics         *DiskMetrics `json:"metrics,omitempty"`
	Endpoint        string       `json:"endpoint,omitempty"`
	DrivePath       string       `json:"path,omitempty"`
	State           string       `json:"state,omitempty"`
	UUID            string       `json:"uuid,omitempty"`
	Model           string       `json:"model,omitempty"`
	ReadThroughput  float64      `json:"readthroughput,omitempty"`
	UsedSpace       uint64       `json:"usedspace,omitempty"`
	AvailableSpace  uint64       `json:"availspace,omitempty"`
	TotalSpace      uint64       `json:"totalspace,omitempty"`
	WriteThroughPut float64      `json:"writethroughput,omitempty"`
	ReadLatency     float64      `json:"readlatency,omitempty"`
	WriteLatency    float64      `json:"writelatency,omitempty"`
	Utilization     float64      `json:"utilization,omitempty"`
	PoolIndex       int          `json:"pool_index"`
	SetIndex        int          `json:"set_index"`
	DiskIndex       int          `json:"disk_index"`
	Healing         bool         `json:"healing,omitempty"`
	RootDisk        bool         `json:"rootDisk,omitempty"` // Indexes, will be -1 until assigned a set.
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
	if err = json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return InfoMessage{}, err
	}

	return message, nil
}
