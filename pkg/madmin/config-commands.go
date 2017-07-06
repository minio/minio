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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

// NodeSummary - represents the result of an operation part of
// set-config on a node.
type NodeSummary struct {
	Name   string `json:"name"`
	ErrSet bool   `json:"errSet"`
	ErrMsg string `json:"errMsg"`
}

// SetConfigResult - represents detailed results of a set-config
// operation.
type SetConfigResult struct {
	NodeResults []NodeSummary `json:"nodeResults"`
	Status      bool          `json:"status"`
}

// GetConfig - returns the config.json of a minio setup.
func (adm *AdminClient) GetConfig() ([]byte, error) {
	// No TLS?
	if !adm.secure {
		return nil, fmt.Errorf("credentials/configuration cannot be retrieved over an insecure connection")
	}

	// Execute GET on /minio/admin/v1/config to get config of a setup.
	resp, err := adm.executeMethod("GET",
		requestData{relPath: "/v1/config"})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	// Return the JSON marshalled bytes to user.
	return ioutil.ReadAll(resp.Body)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(config io.Reader) (r SetConfigResult, err error) {
	// No TLS?
	if !adm.secure {
		return r, fmt.Errorf("credentials/configuration cannot be updated over an insecure connection")
	}

	// Read config bytes to calculate MD5, SHA256 and content length.
	configBytes, err := ioutil.ReadAll(config)
	if err != nil {
		return r, err
	}

	reqData := requestData{
		relPath:            "/v1/config",
		contentBody:        bytes.NewReader(configBytes),
		contentMD5Bytes:    sumMD5(configBytes),
		contentSHA256Bytes: sum256(configBytes),
	}

	// Execute PUT on /minio/admin/v1/config to set config.
	resp, err := adm.executeMethod("PUT", reqData)

	defer closeResponse(resp)
	if err != nil {
		return r, err
	}

	if resp.StatusCode != http.StatusOK {
		return r, httpRespToErrorResponse(resp)
	}

	jsonBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return r, err
	}

	err = json.Unmarshal(jsonBytes, &r)
	return r, err
}
