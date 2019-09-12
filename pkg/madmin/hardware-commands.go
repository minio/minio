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
	"net/http"
	"net/url"

	"github.com/shirou/gopsutil/host"
)

// HardwareType - type to hardware
type HardwareType string

const (
	// Sensor represents hardware as sensor
	Sensor HardwareType = "sensor"
)

// ServerSensorTemp holds sensor temperature
type ServerSensorTemp struct {
	Addr        string                 `json:"addr"`
	Error       string                 `json:"error,omitempty"`
	Temperature []host.TemperatureStat `json:"temp"`
}

// ServerSensorTempInfo - Returns sensor temperatures
func (adm *AdminClient) ServerSensorTempInfo() ([]ServerSensorTemp, error) {
	v := url.Values{}
	v.Set("hwType", "sensor")
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
	var sensorTemp []ServerSensorTemp

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(respBytes, &sensorTemp)
	if err != nil {
		return nil, err
	}
	return sensorTemp, nil
}
