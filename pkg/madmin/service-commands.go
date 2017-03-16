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
	"net/url"
	"time"
)

// ServiceStatusMetadata - contains the response of service status API
type ServiceStatusMetadata struct {
	Uptime time.Duration `json:"uptime"`
}

// ServiceStatus - Connect to a minio server and call Service Status Management API
// to fetch server's storage information represented by ServiceStatusMetadata structure
func (adm *AdminClient) ServiceStatus() (ServiceStatusMetadata, error) {

	// Prepare web service request
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "status")

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("GET", reqData)
	defer closeResponse(resp)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return ServiceStatusMetadata{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var serviceStatus ServiceStatusMetadata

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	err = json.Unmarshal(respBytes, &serviceStatus)
	if err != nil {
		return ServiceStatusMetadata{}, err
	}

	return serviceStatus, nil
}

// ServiceRestart - Call Service Restart API to restart a specified Minio server
func (adm *AdminClient) ServiceRestart() error {
	//
	reqData := requestData{}
	reqData.queryValues = make(url.Values)
	reqData.queryValues.Set("service", "")
	reqData.customHeaders = make(http.Header)
	reqData.customHeaders.Set(minioAdminOpHeader, "restart")

	// Execute GET on bucket to list objects.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}
