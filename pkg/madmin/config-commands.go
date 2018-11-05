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
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/minio/minio/pkg/quick"
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

// GetConfigKeys - returns partial json or json value from config.json of a minio setup.
func (adm *AdminClient) GetConfigKeys(keys []string) ([]byte, error) {
	queryVals := make(url.Values)
	for _, k := range keys {
		queryVals.Add(k, "")
	}

	// Execute GET on /minio/admin/v1/config-keys to get config of a setup.
	resp, err := adm.executeMethod("GET",
		requestData{
			relPath:     "/v1/config-keys",
			queryValues: queryVals,
		})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return DecryptData(adm.secretAccessKey, resp.Body)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(config io.Reader) (err error) {
	const maxConfigJSONSize = 256 * 1024 // 256KiB

	// Read configuration bytes
	configBuf := make([]byte, maxConfigJSONSize+1)
	n, err := io.ReadFull(config, configBuf)
	if err == nil {
		return bytes.ErrTooLarge
	}
	if err != io.ErrUnexpectedEOF {
		return err
	}
	configBytes := configBuf[:n]

	type configVersion struct {
		Version string `json:"version,omitempty"`
	}
	var cfg configVersion

	// Check if read data is in json format
	if err = json.Unmarshal(configBytes, &cfg); err != nil {
		return errors.New("Invalid JSON format: " + err.Error())
	}

	// Check if the provided json file has "version" key set
	if cfg.Version == "" {
		return errors.New("Missing or unset \"version\" key in json file")
	}
	// Validate there are no duplicate keys in the JSON
	if err = quick.CheckDuplicateKeys(string(configBytes)); err != nil {
		return errors.New("Duplicate key in json file: " + err.Error())
	}

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

// SetConfigKeys - set config keys supplied as config.json for the setup.
func (adm *AdminClient) SetConfigKeys(params map[string]string) error {
	queryVals := make(url.Values)
	for k, v := range params {
		encryptedVal, err := EncryptData(adm.secretAccessKey, []byte(v))
		if err != nil {
			return err
		}
		encodedVal := base64.StdEncoding.EncodeToString(encryptedVal)
		queryVals.Add(k, string(encodedVal))
	}

	reqData := requestData{
		relPath:     "/v1/config-keys",
		queryValues: queryVals,
	}

	// Execute PUT on /minio/admin/v1/config-keys to set config.
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
