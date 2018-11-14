/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"net/url"

	"github.com/minio/minio/pkg/auth"
)

// AccountStatus - account status.
type AccountStatus string

// Account status per user.
const (
	AccountEnabled  AccountStatus = "enabled"
	AccountDisabled AccountStatus = "disabled"
)

// UserInfo carries information about long term users.
type UserInfo struct {
	SecretKey  string        `json:"secretKey,omitempty"`
	PolicyName string        `json:"policyName,omitempty"`
	Status     AccountStatus `json:"status"`
}

// RemoveUser - remove a user.
func (adm *AdminClient) RemoveUser(accessKey string) error {
	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)

	reqData := requestData{
		relPath:     "/v1/remove-user",
		queryValues: queryValues,
	}

	// Execute DELETE on /minio/admin/v1/remove-user to remove a user.
	resp, err := adm.executeMethod("DELETE", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// ListUsers - list all users.
func (adm *AdminClient) ListUsers() (map[string]UserInfo, error) {
	reqData := requestData{
		relPath: "/v1/list-users",
	}

	// Execute GET on /minio/admin/v1/list-users
	resp, err := adm.executeMethod("GET", reqData)

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	data, err := DecryptData(adm.secretAccessKey, resp.Body)
	if err != nil {
		return nil, err
	}

	var users = make(map[string]UserInfo)
	if err = json.Unmarshal(data, &users); err != nil {
		return nil, err
	}

	return users, nil
}

// SetUser - sets a user info.
func (adm *AdminClient) SetUser(accessKey, secretKey string, status AccountStatus) error {

	if !auth.IsAccessKeyValid(accessKey) {
		return auth.ErrInvalidAccessKeyLength
	}

	if !auth.IsSecretKeyValid(secretKey) {
		return auth.ErrInvalidSecretKeyLength
	}

	data, err := json.Marshal(UserInfo{
		SecretKey: secretKey,
		Status:    status,
	})
	if err != nil {
		return err
	}
	econfigBytes, err := EncryptData(adm.secretAccessKey, data)
	if err != nil {
		return err
	}

	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)

	reqData := requestData{
		relPath:     "/v1/add-user",
		queryValues: queryValues,
		content:     econfigBytes,
	}

	// Execute PUT on /minio/admin/v1/add-user to set a user.
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

// AddUser - adds a user.
func (adm *AdminClient) AddUser(accessKey, secretKey string) error {
	return adm.SetUser(accessKey, secretKey, AccountEnabled)
}

// SetUserPolicy - adds a policy for a user.
func (adm *AdminClient) SetUserPolicy(accessKey, policyName string) error {
	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     "/v1/set-user-policy",
		queryValues: queryValues,
	}

	// Execute PUT on /minio/admin/v1/set-user-policy to set policy.
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

// SetUserStatus - adds a status for a user.
func (adm *AdminClient) SetUserStatus(accessKey string, status AccountStatus) error {
	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)
	queryValues.Set("status", string(status))

	reqData := requestData{
		relPath:     "/v1/set-user-status",
		queryValues: queryValues,
	}

	// Execute PUT on /minio/admin/v1/set-user-status to set status.
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
