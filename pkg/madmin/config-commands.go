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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

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
	defer resp.Body.Close()

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
