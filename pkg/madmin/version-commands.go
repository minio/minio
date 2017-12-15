/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
)

// AdminAPIVersionInfo - contains admin API version information
type AdminAPIVersionInfo struct {
	Version string `json:"version"`
}

// VersionInfo - Connect to minio server and call the version API to
// retrieve the server API version
func (adm *AdminClient) VersionInfo() (verInfo AdminAPIVersionInfo, err error) {
	var resp *http.Response
	resp, err = adm.executeMethod("GET", requestData{relPath: "/version"})
	defer closeResponse(resp)
	if err != nil {
		return verInfo, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return verInfo, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return verInfo, err
	}

	// Unmarshal the server's json response
	err = json.Unmarshal(respBytes, &verInfo)
	return verInfo, err
}
