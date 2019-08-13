/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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
	"strconv"
	"time"

	trace "github.com/minio/minio/pkg/trace"
)

// ServerVersion - server version
type ServerVersion struct {
	Version  string `json:"version"`
	CommitID string `json:"commitID"`
}

// ServiceUpdateStatus - contains the response of service update API
type ServiceUpdateStatus struct {
	CurrentVersion string `json:"currentVersion"`
	UpdatedVersion string `json:"updatedVersion"`
}

// ServiceStatus - contains the response of service status API
type ServiceStatus struct {
	ServerVersion ServerVersion `json:"serverVersion"`
	Uptime        time.Duration `json:"uptime"`
}

// ServiceStatus - Returns current server uptime and current
// running version of MinIO server.
func (adm *AdminClient) ServiceStatus() (ss ServiceStatus, err error) {
	respBytes, err := adm.serviceCallAction(ServiceActionStatus)
	if err != nil {
		return ss, err
	}
	err = json.Unmarshal(respBytes, &ss)
	return ss, err
}

// ServiceRestart - restarts the MinIO cluster
func (adm *AdminClient) ServiceRestart() error {
	_, err := adm.serviceCallAction(ServiceActionRestart)
	return err
}

// ServiceStop - stops the MinIO cluster
func (adm *AdminClient) ServiceStop() error {
	_, err := adm.serviceCallAction(ServiceActionStop)
	return err
}

// ServiceUpdate - updates and restarts the MinIO cluster to latest version.
func (adm *AdminClient) ServiceUpdate() (us ServiceUpdateStatus, err error) {
	respBytes, err := adm.serviceCallAction(ServiceActionUpdate)
	if err != nil {
		return us, err
	}
	err = json.Unmarshal(respBytes, &us)
	return us, err
}

// ServiceAction - type to restrict service-action values
type ServiceAction string

const (
	// ServiceActionStatus represents status action
	ServiceActionStatus ServiceAction = "status"
	// ServiceActionRestart represents restart action
	ServiceActionRestart = "restart"
	// ServiceActionStop represents stop action
	ServiceActionStop = "stop"
	// ServiceActionUpdate represents update action
	ServiceActionUpdate = "update"
)

// serviceCallAction - call service restart/update/stop API.
func (adm *AdminClient) serviceCallAction(action ServiceAction) ([]byte, error) {
	queryValues := url.Values{}
	queryValues.Set("action", string(action))

	// Request API to Restart server
	resp, err := adm.executeMethod("POST", requestData{
		relPath:     "/v1/service",
		queryValues: queryValues,
	})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return ioutil.ReadAll(resp.Body)
}

// ServiceTraceInfo holds http trace
type ServiceTraceInfo struct {
	Trace trace.Info
	Err   error `json:"-"`
}

// ServiceTrace - listen on http trace notifications.
func (adm AdminClient) ServiceTrace(allTrace, errTrace bool, doneCh <-chan struct{}) <-chan ServiceTraceInfo {
	traceInfoCh := make(chan ServiceTraceInfo)
	// Only success, start a routine to start reading line by line.
	go func(traceInfoCh chan<- ServiceTraceInfo) {
		defer close(traceInfoCh)
		for {
			urlValues := make(url.Values)
			urlValues.Set("all", strconv.FormatBool(allTrace))
			urlValues.Set("err", strconv.FormatBool(errTrace))
			reqData := requestData{
				relPath:     "/v1/trace",
				queryValues: urlValues,
			}
			// Execute GET to call trace handler
			resp, err := adm.executeMethod("GET", reqData)
			if err != nil {
				closeResponse(resp)
				return
			}

			if resp.StatusCode != http.StatusOK {
				traceInfoCh <- ServiceTraceInfo{Err: httpRespToErrorResponse(resp)}
				return
			}

			dec := json.NewDecoder(resp.Body)
			for {
				var info trace.Info
				if err = dec.Decode(&info); err != nil {
					break
				}
				select {
				case <-doneCh:
					return
				case traceInfoCh <- ServiceTraceInfo{Trace: info}:
				}
			}
		}
	}(traceInfoCh)

	// Returns the trace info channel, for caller to start reading from.
	return traceInfoCh
}
