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
	"bytes"
	"io"
	"net/http"
	"net/url"
)

// DelConfigKV - set key value config to server.
func (adm *AdminClient) DelConfigKV(k string) (err error) {
	econfigBytes, err := EncryptData(adm.secretAccessKey, []byte(k))
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: "/v1/del-config-kv",
		content: econfigBytes,
	}

	// Execute DELETE on /minio/admin/v1/del-config-kv to delete config key.
	resp, err := adm.executeMethod(http.MethodDelete, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// SetConfigKV - set key value config to server.
func (adm *AdminClient) SetConfigKV(kv string) (err error) {
	econfigBytes, err := EncryptData(adm.secretAccessKey, []byte(kv))
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: "/v1/set-config-kv",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v1/set-config-kv to set config key/value.
	resp, err := adm.executeMethod(http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// GetConfigKV - returns the key, value of the requested key, incoming data is encrypted.
func (adm *AdminClient) GetConfigKV(key string) ([]byte, error) {
	v := url.Values{}
	v.Set("key", key)

	// Execute GET on /minio/admin/v1/get-config-kv?key={key} to get value of key.
	resp, err := adm.executeMethod(http.MethodGet,
		requestData{
			relPath:     "/v1/get-config-kv",
			queryValues: v,
		})
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

// GetConfig - returns the config.json of a minio setup, incoming data is encrypted.
func (adm *AdminClient) GetConfig() ([]byte, error) {
	// Execute GET on /minio/admin/v1/config to get config of a setup.
	resp, err := adm.executeMethod(http.MethodGet,
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
	econfigBytes, err := EncryptData(adm.secretAccessKey, configBytes)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: "/v1/config",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v1/config to set config.
	resp, err := adm.executeMethod(http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
