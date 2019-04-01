/*
 * MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
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
	"net/http"

	config "github.com/minio/minio/pkg/server-config"
)

// GetConfig - returns the config.json of a minio setup, incoming data is encrypted.
func (adm *AdminClient) GetConfig() ([]byte, error) {
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
	defer closeResponse(resp)

	return DecryptData(adm.secretAccessKey, resp.Body)
}

// GetConfigKeyValue - get config entry, if no such
// key exists, returns an error.
func (adm *AdminClient) GetConfigKeyValue(input string) (config.KVS, error) {
	configBytes, err := adm.GetConfig()
	if err != nil {
		return nil, err
	}
	var m = make(config.Config)
	if err = json.Unmarshal(configBytes, &m); err != nil {
		return nil, err
	}
	return config.GetKVS(input, m)
}

// SetConfigKeyValue - updates/overwrites the config, if no such
// key exists additionally adds a new one as well.
func (adm *AdminClient) SetConfigKeyValue(input string) error {
	configBytes, err := adm.GetConfig()
	if err != nil {
		return err

	}
	var m = make(config.Config)
	if err = json.Unmarshal(configBytes, &m); err != nil {
		return err
	}
	if err = config.SetKVS(input, m); err != nil {
		return err
	}
	configBytes, err = json.Marshal(m)
	if err != nil {
		return err
	}
	return adm.SetConfig(configBytes)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(configBytes []byte) error {
	econfigBytes, err := EncryptData(adm.secretAccessKey, configBytes)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: "/v1/config",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v1/config to set config.
	resp, err := adm.executeMethod("PUT", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
