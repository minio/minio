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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/minio/minio/pkg/quick"
	"github.com/minio/sio"
	"golang.org/x/crypto/argon2"
)

// EncryptServerConfigData - encrypts server config data.
func EncryptServerConfigData(password string, data []byte) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}

	// derive an encryption key from the master key and the nonce
	var key [32]byte
	copy(key[:], argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32))

	encrypted, err := sio.EncryptReader(bytes.NewReader(data), sio.Config{
		Key: key[:]},
	)
	if err != nil {
		return nil, err
	}
	edata, err := ioutil.ReadAll(encrypted)
	return append(salt, edata...), err
}

// DecryptServerConfigData - decrypts server config data.
func DecryptServerConfigData(password string, data io.Reader) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := io.ReadFull(data, salt); err != nil {
		return nil, err
	}
	// derive an encryption key from the master key and the nonce
	var key [32]byte
	copy(key[:], argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32))

	decrypted, err := sio.DecryptReader(data, sio.Config{
		Key: key[:]},
	)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(decrypted)
}

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

	return DecryptServerConfigData(adm.secretAccessKey, resp.Body)
}

// GetConfigKeys - returns partial json or json value from config.json of a minio setup.
func (adm *AdminClient) GetConfigKeys(keys []string) ([]byte, error) {
	// No TLS?
	if !adm.secure {
		// return nil, fmt.Errorf("credentials/configuration cannot be retrieved over an insecure connection")
	}

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

	return DecryptServerConfigData(adm.secretAccessKey, resp.Body)
}

// SetConfig - set config supplied as config.json for the setup.
func (adm *AdminClient) SetConfig(config io.Reader) (err error) {
	const maxConfigJSONSize = 256 * 1024 // 256KiB

	// Read configuration bytes
	configBuf := make([]byte, maxConfigJSONSize+1)
	n, err := io.ReadFull(config, configBuf)
	if err == nil {
		return fmt.Errorf("too large file")
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

	econfigBytes, err := EncryptServerConfigData(adm.secretAccessKey, configBytes)
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
		encryptedVal, err := EncryptServerConfigData(adm.secretAccessKey, []byte(v))
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
