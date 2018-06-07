/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"time"
)

// ServerVersion - server version
type ServerVersion struct {
	Version  string `json:"version"`
	CommitID string `json:"commitID"`
}

// ServiceStatus - contains the response of service status API
type ServiceStatus struct {
	ServerVersion ServerVersion `json:"serverVersion"`
	Uptime        time.Duration `json:"uptime"`
}

// ServiceStatus - Connect to a minio server and call Service Status
// Management API to fetch server's storage information represented by
// ServiceStatusMetadata structure
func (adm *AdminClient) ServiceStatus() (ss ServiceStatus, err error) {
	// Request API to GET service status
	resp, err := adm.executeMethod("GET", requestData{relPath: "/v1/service"})
	defer closeResponse(resp)
	if err != nil {
		return ss, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return ss, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ss, err
	}

	err = json.Unmarshal(respBytes, &ss)
	return ss, err
}

// ServiceActionValue - type to restrict service-action values
type ServiceActionValue string

const (
	// ServiceActionValueRestart represents restart action
	ServiceActionValueRestart ServiceActionValue = "restart"
	// ServiceActionValueStop represents stop action
	ServiceActionValueStop = "stop"
)

// ServiceAction - represents POST body for service action APIs
type ServiceAction struct {
	Action ServiceActionValue `json:"action"`
}

// ServiceSendAction - Call Service Restart/Stop API to restart/stop a
// Minio server
func (adm *AdminClient) ServiceSendAction(action ServiceActionValue) error {
	body, err := json.Marshal(ServiceAction{action})
	if err != nil {
		return err
	}

	// Request API to Restart server
	resp, err := adm.executeMethod("POST", requestData{
		relPath: "/v1/service",
		content: body,
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}
