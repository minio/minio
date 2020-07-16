/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// AccountAccess contains information about
type AccountAccess struct {
	Read  bool `json:"read"`
	Write bool `json:"write"`
}

// BucketUsageInfo represents bucket usage of a bucket, and its relevant
// access type for an account
type BucketUsageInfo struct {
	Name    string        `json:"name"`
	Size    uint64        `json:"size"`
	Created time.Time     `json:"created"`
	Access  AccountAccess `json:"access"`
}

// AccountUsageInfo represents the account usage info of an
// account across buckets.
type AccountUsageInfo struct {
	AccountName string
	Buckets     []BucketUsageInfo
}

// AccountUsageInfo returns the usage info for the authenticating account.
func (adm *AdminClient) AccountUsageInfo(ctx context.Context) (AccountUsageInfo, error) {
	resp, err := adm.executeMethod(ctx, http.MethodGet, requestData{relPath: adminAPIPrefix + "/accountusageinfo"})
	defer closeResponse(resp)
	if err != nil {
		return AccountUsageInfo{}, err
	}

	// Check response http status code
	if resp.StatusCode != http.StatusOK {
		return AccountUsageInfo{}, httpRespToErrorResponse(resp)
	}

	// Unmarshal the server's json response
	var accountInfo AccountUsageInfo

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return AccountUsageInfo{}, err
	}

	err = json.Unmarshal(respBytes, &accountInfo)
	if err != nil {
		return AccountUsageInfo{}, err
	}

	return accountInfo, nil
}

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
	MemberOf   []string      `json:"memberOf,omitempty"`
}

// RemoveUser - remove a user.
func (adm *AdminClient) RemoveUser(ctx context.Context, accessKey string) error {
	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/remove-user",
		queryValues: queryValues,
	}

	// Execute DELETE on /minio/admin/v3/remove-user to remove a user.
	resp, err := adm.executeMethod(ctx, http.MethodDelete, reqData)

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
func (adm *AdminClient) ListUsers(ctx context.Context) (map[string]UserInfo, error) {
	reqData := requestData{
		relPath: adminAPIPrefix + "/list-users",
	}

	// Execute GET on /minio/admin/v3/list-users
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	data, err := DecryptData(adm.getSecretKey(), resp.Body)
	if err != nil {
		return nil, err
	}

	var users = make(map[string]UserInfo)
	if err = json.Unmarshal(data, &users); err != nil {
		return nil, err
	}

	return users, nil
}

// GetUserInfo - get info on a user
func (adm *AdminClient) GetUserInfo(ctx context.Context, name string) (u UserInfo, err error) {
	queryValues := url.Values{}
	queryValues.Set("accessKey", name)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/user-info",
		queryValues: queryValues,
	}

	// Execute GET on /minio/admin/v3/user-info
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return u, err
	}

	if resp.StatusCode != http.StatusOK {
		return u, httpRespToErrorResponse(resp)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return u, err
	}

	if err = json.Unmarshal(b, &u); err != nil {
		return u, err
	}

	return u, nil
}

// SetUser - sets a user info.
func (adm *AdminClient) SetUser(ctx context.Context, accessKey, secretKey string, status AccountStatus) error {

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
	econfigBytes, err := EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return err
	}

	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/add-user",
		queryValues: queryValues,
		content:     econfigBytes,
	}

	// Execute PUT on /minio/admin/v3/add-user to set a user.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)

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
func (adm *AdminClient) AddUser(ctx context.Context, accessKey, secretKey string) error {
	return adm.SetUser(ctx, accessKey, secretKey, AccountEnabled)
}

// SetUserStatus - adds a status for a user.
func (adm *AdminClient) SetUserStatus(ctx context.Context, accessKey string, status AccountStatus) error {
	queryValues := url.Values{}
	queryValues.Set("accessKey", accessKey)
	queryValues.Set("status", string(status))

	reqData := requestData{
		relPath:     adminAPIPrefix + "/set-user-status",
		queryValues: queryValues,
	}

	// Execute PUT on /minio/admin/v3/set-user-status to set status.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// AddServiceAccountReq is the request body of the add service account admin call
type AddServiceAccountReq struct {
	Policy *iampolicy.Policy `json:"policy,omitempty"`
}

// AddServiceAccountResp is the response body of the add service account admin call
type AddServiceAccountResp struct {
	Credentials auth.Credentials `json:"credentials"`
}

// AddServiceAccount - creates a new service account belonging to the user sending
// the request while restricting the service account permission by the given policy document.
func (adm *AdminClient) AddServiceAccount(ctx context.Context, policy *iampolicy.Policy) (auth.Credentials, error) {
	if policy != nil {
		if err := policy.Validate(); err != nil {
			return auth.Credentials{}, err
		}
	}

	data, err := json.Marshal(AddServiceAccountReq{
		Policy: policy,
	})
	if err != nil {
		return auth.Credentials{}, err
	}

	econfigBytes, err := EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return auth.Credentials{}, err
	}

	reqData := requestData{
		relPath: adminAPIPrefix + "/add-service-account",
		content: econfigBytes,
	}

	// Execute PUT on /minio/admin/v3/add-service-account to set a user.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)
	defer closeResponse(resp)
	if err != nil {
		return auth.Credentials{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return auth.Credentials{}, httpRespToErrorResponse(resp)
	}

	data, err = DecryptData(adm.getSecretKey(), resp.Body)
	if err != nil {
		return auth.Credentials{}, err
	}

	var serviceAccountResp AddServiceAccountResp
	if err = json.Unmarshal(data, &serviceAccountResp); err != nil {
		return auth.Credentials{}, err
	}
	return serviceAccountResp.Credentials, nil
}

// ListServiceAccountsResp is the response body of the list service accounts call
type ListServiceAccountsResp struct {
	Accounts []string `json:"accounts"`
}

// ListServiceAccounts - list service accounts belonging to the specified user
func (adm *AdminClient) ListServiceAccounts(ctx context.Context) (ListServiceAccountsResp, error) {
	reqData := requestData{
		relPath: adminAPIPrefix + "/list-service-accounts",
	}

	// Execute GET on /minio/admin/v3/list-service-accounts
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	defer closeResponse(resp)
	if err != nil {
		return ListServiceAccountsResp{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return ListServiceAccountsResp{}, httpRespToErrorResponse(resp)
	}

	data, err := DecryptData(adm.getSecretKey(), resp.Body)
	if err != nil {
		return ListServiceAccountsResp{}, err
	}

	var listResp ListServiceAccountsResp
	if err = json.Unmarshal(data, &listResp); err != nil {
		return ListServiceAccountsResp{}, err
	}
	return listResp, nil
}

// DeleteServiceAccount - delete a specified service account. The server will reject
// the request if the service account does not belong to the user initiating the request
func (adm *AdminClient) DeleteServiceAccount(ctx context.Context, serviceAccount string) error {
	if !auth.IsAccessKeyValid(serviceAccount) {
		return auth.ErrInvalidAccessKeyLength
	}

	queryValues := url.Values{}
	queryValues.Set("accessKey", serviceAccount)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/delete-service-account",
		queryValues: queryValues,
	}

	// Execute DELETE on /minio/admin/v3/delete-service-account
	resp, err := adm.executeMethod(ctx, http.MethodDelete, reqData)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
