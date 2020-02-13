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
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/net"
)

// OBDInfo - MinIO cluster's OBD Info
type OBDInfo struct {
	TimeStamp time.Time             `json:"timestamp,omitempty"`
	DriveInfo []ServerDrivesOBDInfo `json:"driveInfo,omitempty"`
	Config    interface{}           `json:"configInfo,omitempty"`
	Info      InfoMessage           `json:"adminInfo,omitempty"`
	Net       []ServerNetOBDInfo    `json:"netInfo,omitempty"`
	Error     string                `json:"error,omitempty"`
}

// ServerDrivesOBDInfo - Drive OBD info about all drives in a single MinIO node
type ServerDrivesOBDInfo struct {
	Addr   string         `json:"addr"`
	Drives []DriveOBDInfo `json:"drives,omitempty"`
	Error  string         `json:"error,omitempty"`
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
	OBDDataTypeDrive  OBDDataType = "drive"  // Drive
	OBDDataTypeNet    OBDDataType = "net"    // Net
	OBDDataTypeInfo   OBDDataType = "info"   // Admin Info
	OBDDataTypeConfig OBDDataType = "config" // Config
)

// OBDDataTypesMap - Map of OBD datatypes
var OBDDataTypesMap = map[string]OBDDataType{
	"drive":  OBDDataTypeDrive,
	"net":    OBDDataTypeNet,
	"info":   OBDDataTypeInfo,
	"config": OBDDataTypeConfig,
}

// OBDDataTypesList - List of OBD datatypes
var OBDDataTypesList = []OBDDataType{
	OBDDataTypeDrive,
	OBDDataTypeNet,
	OBDDataTypeInfo,
	OBDDataTypeConfig,
}

// ServerOBDInfo - Connect to a minio server and call OBD Info Management API
// to fetch server's information represented by OBDInfo structure
func (adm *AdminClient) ServerOBDInfo(obdDataTypes []OBDDataType) (OBDInfo, error) {
	v := url.Values{}

	// start with all set to false
	for _, d := range OBDDataTypesList {
		v.Set(string(d), "false")
	}

	// only 'trueify' user provided values
	for _, d := range obdDataTypes {
		v.Set(string(d), "true")
	}

	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/obdinfo",
		queryValues: v,
	})

	defer closeResponse(resp)
	if err != nil {
		return OBDInfo{}, err
	}
	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return OBDInfo{}, httpRespToErrorResponse(resp)
	}
	// Unmarshal the server's json response
	var OBDInfoMessage OBDInfo
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return OBDInfo{}, err
	}
	err = json.Unmarshal(respBytes, &OBDInfoMessage)
	if err != nil {
		return OBDInfo{}, err
	}

	info, err := adm.ServerInfo()
	if err != nil {
		return OBDInfo{}, err
	}

	if v.Get(string(OBDDataTypeInfo)) == "true" {
		OBDInfoMessage.Info = info
	}

	OBDInfoMessage.TimeStamp = time.Now()
	return OBDInfoMessage, nil
}
