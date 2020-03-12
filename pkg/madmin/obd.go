/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/net"
)

// OBDInfo - MinIO cluster's OBD Info
type OBDInfo struct {
	TimeStamp time.Time    `json:"timestamp,omitempty"`
	Error     string       `json:"error,omitempty"`
	Perf      PerfOBDInfo  `json:"perf,omitempty"`
	Minio     MinioOBDInfo `json:"minio,omitempty"`
	Sys       SysOBDInfo   `json:"sys,omitempty"`
}

// SysOBDInfo - Includes hardware and system information of the MinIO cluster
type SysOBDInfo struct {
}

// MinioOBDInfo - Includes MinIO confifuration information
type MinioOBDInfo struct {
	Info   InfoMessage `json:"info,omitempty"`
	Config interface{} `json:"config,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// PerfOBDInfo - Includes Drive and Net perf info for the entire MinIO cluster
type PerfOBDInfo struct {
	DriveInfo []ServerDrivesOBDInfo `json:"drives,omitempty"`
	Net       []ServerNetOBDInfo    `json:"net,omitempty"`
	Error     string                `json:"error,omitempty"`
}

// ServerDrivesOBDInfo - Drive OBD info about all drives in a single MinIO node
type ServerDrivesOBDInfo struct {
	Addr     string         `json:"addr"`
	Serial   []DriveOBDInfo `json:"serial,omitempty"`
	Parallel []DriveOBDInfo `json:"parallel,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// DriveOBDInfo - Stats about a single drive in a MinIO node
type DriveOBDInfo struct {
	Path       string          `json:"endpoint"`
	Latency    disk.Latency    `json:"latency,omitempty"`
	Throughput disk.Throughput `json:"throughput,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ServerNetOBDInfo - Network OBD info about a single MinIO node
type ServerNetOBDInfo struct {
	Addr  string       `json:"addr"`
	Net   []NetOBDInfo `json:"net,omitempty"`
	Error string       `json:"error,omitempty"`
}

// NetOBDInfo - one-to-one network connectivity Stats between 2 MinIO nodes
type NetOBDInfo struct {
	Addr       string         `json:"remote"`
	Latency    net.Latency    `json:"latency,omitempty"`
	Throughput net.Throughput `json:"throughput,omitempty"`
	Error      string         `json:"error,omitempty"`
}

// OBDDataType - Typed OBD data types
type OBDDataType string

// OBDDataTypes
const (
	OBDDataTypePerfDrive   OBDDataType = "perfdrive"
	OBDDataTypePerfNet     OBDDataType = "perfnet"
	OBDDataTypeMinioInfo   OBDDataType = "minioinfo"
	OBDDataTypeMinioConfig OBDDataType = "minioconfig"
	OBDDataTypeSysCPU      OBDDataType = "syscpu"
	OBDDataTypeSysDrive    OBDDataType = "sysdrive"
	OBDDataTypeSysDocker   OBDDataType = "sysdocker"
	OBDDataTypeSysHost     OBDDataType = "syshost"
	OBDDataTypeSysLoad     OBDDataType = "sysload"
	OBDDataTypeSysMem      OBDDataType = "sysmem"
	OBDDataTypeSysNet      OBDDataType = "sysnet"
	OBDDataTypeSysProcess  OBDDataType = "sysprocess"
)

// OBDDataTypesMap - Map of OBD datatypes
var OBDDataTypesMap = map[string]OBDDataType{
	"perfdrive":   OBDDataTypePerfDrive,
	"perfnet":     OBDDataTypePerfNet,
	"minioinfo":   OBDDataTypeMinioInfo,
	"minioconfig": OBDDataTypeMinioConfig,
	"syscpu":      OBDDataTypeSysCPU,
	"sysdrive":    OBDDataTypeSysDrive,
	"sysdocker":   OBDDataTypeSysDocker,
	"syshost":     OBDDataTypeSysHost,
	"sysload":     OBDDataTypeSysLoad,
	"sysmem":      OBDDataTypeSysMem,
	"sysnet":      OBDDataTypeSysNet,
	"sysprocess":  OBDDataTypeSysProcess,
}

// OBDDataTypesList - List of OBD datatypes
var OBDDataTypesList = []OBDDataType{
	OBDDataTypePerfDrive,
	OBDDataTypePerfNet,
	OBDDataTypeMinioInfo,
	OBDDataTypeMinioConfig,
	OBDDataTypeSysCPU,
	OBDDataTypeSysDrive,
	OBDDataTypeSysDocker,
	OBDDataTypeSysHost,
	OBDDataTypeSysLoad,
	OBDDataTypeSysMem,
	OBDDataTypeSysNet,
	OBDDataTypeSysProcess,
}

// ServerOBDInfo - Connect to a minio server and call OBD Info Management API
// to fetch server's information represented by OBDInfo structure
func (adm *AdminClient) ServerOBDInfo(obdDataTypes []OBDDataType) <-chan OBDInfo {
	respChan := make(chan OBDInfo)
	go func() {
		v := url.Values{}

		// start with all set to false
		for _, d := range OBDDataTypesList {
			v.Set(string(d), "false")
		}

		// only 'trueify' user provided values
		for _, d := range obdDataTypes {
			v.Set(string(d), "true")
		}
		var OBDInfoMessage OBDInfo

		if v.Get(string(OBDDataTypeMinioInfo)) == "true" {
			info, err := adm.ServerInfo()
			if err != nil {
				respChan <- OBDInfo{
					Error: err.Error(),
				}
				return
			}
			OBDInfoMessage.Minio.Info = info
			respChan <- OBDInfoMessage
		}

		resp, err := adm.executeMethod("GET", requestData{
			relPath:     adminAPIPrefix + "/obdinfo",
			queryValues: v,
		})

		defer closeResponse(resp)
		if err != nil {
			respChan <- OBDInfo{
				Error: err.Error(),
			}
			return
		}
		// Check response http status code
		if resp.StatusCode != http.StatusOK {
			respChan <- OBDInfo{
				Error: httpRespToErrorResponse(resp).Error(),
			}
			return
		}
		// Unmarshal the server's json response
		decoder := json.NewDecoder(resp.Body)
		for {
			err := decoder.Decode(&OBDInfoMessage)
			if err == io.EOF {
				break
			}
			if err != nil {
				respChan <- OBDInfo{
					Error: err.Error(),
				}
			}
			respChan <- OBDInfoMessage
		}

		OBDInfoMessage.TimeStamp = time.Now()
		respChan <- OBDInfoMessage
		close(respChan)
	}()
	return respChan

}
