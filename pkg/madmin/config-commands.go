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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/minio/minio/pkg/quick"
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

	// Return the JSON marshaled bytes to user.
	return ioutil.ReadAll(resp.Body)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(config io.Reader) (r SetConfigResult, err error) {
	const maxConfigJSONSize = 256 * 1024 // 256KiB

	if !adm.secure { // No TLS?
		return r, fmt.Errorf("credentials/configuration cannot be updated over an insecure connection")
	}

	// Read configuration bytes
	configBuf := make([]byte, maxConfigJSONSize+1)
	n, err := io.ReadFull(config, configBuf)
	if err == nil {
		return r, fmt.Errorf("too large file")
	}
	if err != io.ErrUnexpectedEOF {
		return r, err
	}
	configBytes := configBuf[:n]

	type configVersion struct {
		Version string `json:"version,omitempty"`
	}
	var cfg configVersion

	// Check if read data is in json format
	if err = json.Unmarshal(configBytes, &cfg); err != nil {
		return r, errors.New("Invalid JSON format: " + err.Error())
	}

	// Check if the provided json file has "version" key set
	if cfg.Version == "" {
		return r, errors.New("Missing or unset \"version\" key in json file")
	}
	// Validate there are no duplicate keys in the JSON
	if err = quick.CheckDuplicateKeys(string(configBytes)); err != nil {
		return r, errors.New("Duplicate key in json file: " + err.Error())
	}

	reqData := requestData{
		relPath: "/v1/config",
		content: configBytes,
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
