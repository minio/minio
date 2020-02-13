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
)

type OBDInfo struct {
	TimeStamp time.Time             `json:"timestamp,omitempty"`
	DriveInfo []ServerDrivesOBDInfo `json:"driveInfo,omitempty"`
	Config    interface{}           `json:"config,omitempty"`
	Error     string                `json:"error,omitempty"`
}

type ServerDrivesOBDInfo struct {
	Addr   string         `json:"addr"`
	Drives []DriveOBDInfo `json:"drives,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type DriveOBDInfo struct {
	Path       string          `json:"endpoint"`
	Latency    disk.Latency    `json:"latency,omitempty"`
	Throughput disk.Throughput `json:"throughput,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type OBDDataType string

const (
	OBDDataTypeDrive  OBDDataType = "drive"
	OBDDataTypeNet    OBDDataType = "net"
	OBDDataTypeInfo   OBDDataType = "info"
	OBDDataTypeConfig OBDDataType = "config"
)

var OBDDataTypesMap = map[string]OBDDataType{
	"drive":  OBDDataTypeDrive,
	"net":    OBDDataTypeNet,
	"info":   OBDDataTypeInfo,
	"config": OBDDataTypeConfig,
}

var OBDDataTypesList = []OBDDataType{
	OBDDataTypeDrive,
	OBDDataTypeNet,
	OBDDataTypeInfo,
	OBDDataTypeConfig,
}

// OBDInfo - Connect to a minio server and call OBD Info Management API
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
	OBDInfoMessage.TimeStamp = time.Now()
	return OBDInfoMessage, nil
}
