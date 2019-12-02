/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"net"
	"net/http"
	"net/url"

	"github.com/shirou/gopsutil/cpu"
)

// HardwareType - type to hardware
type HardwareType string

const (
	// HARDWARE represents hardware type
	HARDWARE = "hwType"
	// CPU represents hardware as cpu
	CPU HardwareType = "cpu"
	// NETWORK hardware Info
	NETWORK HardwareType = "network"
)

// ServerCPUHardwareInfo holds informantion about cpu hardware
type ServerCPUHardwareInfo struct {
	Addr    string         `json:"addr"`
	Error   string         `json:"error,omitempty"`
	CPUInfo []cpu.InfoStat `json:"cpu"`
}

// ServerCPUHardwareInfo - Returns cpu hardware information
func (adm *AdminClient) ServerCPUHardwareInfo() ([]ServerCPUHardwareInfo, error) {
	v := url.Values{}
	v.Set(HARDWARE, string(CPU))
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     adminAPIPrefix + "/hardware",
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
	var cpuInfo []ServerCPUHardwareInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &cpuInfo)
	if err != nil {
		return nil, err
	}
	return cpuInfo, nil
}

// ServerNetworkHardwareInfo holds informantion about cpu hardware
type ServerNetworkHardwareInfo struct {
	Addr        string          `json:"addr"`
	Error       string          `json:"error,omitempty"`
	NetworkInfo []net.Interface `json:"network"`
}

// ServerNetworkHardwareInfo - Returns network hardware information
func (adm *AdminClient) ServerNetworkHardwareInfo() ([]ServerNetworkHardwareInfo, error) {
	v := url.Values{}
	v.Set(HARDWARE, string(NETWORK))
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     "/v1/hardware",
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
	var networkInfo []ServerNetworkHardwareInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(respBytes, &networkInfo)
	if err != nil {
		return nil, err
	}
	return networkInfo, nil
}
