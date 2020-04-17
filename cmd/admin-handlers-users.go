/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
 */

package cmd

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

func validateAdminUsersReq(ctx context.Context, w http.ResponseWriter, r *http.Request, action iampolicy.AdminAction) (ObjectLayer, auth.Credentials) {
	var cred auth.Credentials
	var adminAPIErr APIErrorCode

	// Get current object layer instance.
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil || globalNotificationSys == nil || globalIAMSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return nil, cred
	}

	// Validate request signature.
	cred, adminAPIErr = checkAdminRequestAuthType(ctx, r, action, "")
	if adminAPIErr != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(adminAPIErr), r.URL)
		return nil, cred
	}

	return objectAPI, cred
}

// RemoveUser - DELETE /minio/admin/v3/remove-user?accessKey=<access_key>
func (a adminAPIHandlers) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveUser")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.DeleteUserAdminAction)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	ok, err := globalIAMSys.IsTempUser(accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if ok {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
		return
	}

	if err := globalIAMSys.DeleteUser(accessKey); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to delete user.
	for _, nerr := range globalNotificationSys.DeleteUser(accessKey) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// ListUsers - GET /minio/admin/v3/list-users
func (a adminAPIHandlers) ListUsers(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListUsers")

	objectAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.ListUsersAdminAction)
	if objectAPI == nil {
		return
	}

	password := cred.SecretKey

	allCredentials, err := globalIAMSys.ListUsers()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(allCredentials)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// GetUserInfo - GET /minio/admin/v3/user-info
func (a adminAPIHandlers) GetUserInfo(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetUserInfo")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetUserAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	name := vars["accessKey"]

	userInfo, err := globalIAMSys.GetUserInfo(name)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(userInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, data)
}

// UpdateGroupMembers - PUT /minio/admin/v3/update-group-members
func (a adminAPIHandlers) UpdateGroupMembers(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "UpdateGroupMembers")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.AddUserToGroupAdminAction)
	if objectAPI == nil {
		return
	}

	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	var updReq madmin.GroupAddRemove
	err = json.Unmarshal(data, &updReq)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if updReq.IsRemove {
		err = globalIAMSys.RemoveUsersFromGroup(updReq.Group, updReq.Members)
	} else {
		err = globalIAMSys.AddUsersToGroup(updReq.Group, updReq.Members)
	}

	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to load group.
	for _, nerr := range globalNotificationSys.LoadGroup(updReq.Group) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// GetGroup - /minio/admin/v3/group?group=mygroup1
func (a adminAPIHandlers) GetGroup(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetGroup")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetGroupAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	group := vars["group"]

	gdesc, err := globalIAMSys.GetGroupDescription(group)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	body, err := json.Marshal(gdesc)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, body)
}

// ListGroups - GET /minio/admin/v3/groups
func (a adminAPIHandlers) ListGroups(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListGroups")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.ListGroupsAdminAction)
	if objectAPI == nil {
		return
	}

	groups, err := globalIAMSys.ListGroups()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	body, err := json.Marshal(groups)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, body)
}

// SetGroupStatus - PUT /minio/admin/v3/set-group-status?group=mygroup1&status=enabled
func (a adminAPIHandlers) SetGroupStatus(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetGroupStatus")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.EnableGroupAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	group := vars["group"]
	status := vars["status"]

	var err error
	if status == statusEnabled {
		err = globalIAMSys.SetGroupStatus(group, true)
	} else if status == statusDisabled {
		err = globalIAMSys.SetGroupStatus(group, false)
	} else {
		err = errInvalidArgument
	}
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload user.
	for _, nerr := range globalNotificationSys.LoadGroup(group) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// SetUserStatus - PUT /minio/admin/v3/set-user-status?accessKey=<access_key>&status=[enabled|disabled]
func (a adminAPIHandlers) SetUserStatus(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetUserStatus")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.EnableUserAdminAction)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	status := vars["status"]

	// Custom IAM policies not allowed for admin user.
	if accessKey == globalActiveCred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := globalIAMSys.SetUserStatus(accessKey, madmin.AccountStatus(status)); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload user.
	for _, nerr := range globalNotificationSys.LoadUser(accessKey, false) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// AddUser - PUT /minio/admin/v3/add-user?accessKey=<access_key>
func (a adminAPIHandlers) AddUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddUser")

	objectAPI, cred := validateAdminUsersReq(ctx, w, r, iampolicy.CreateUserAdminAction)
	if objectAPI == nil {
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	// Custom IAM policies not allowed for admin user.
	if accessKey == globalActiveCred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	password := cred.SecretKey
	configBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var uinfo madmin.UserInfo
	if err = json.Unmarshal(configBytes, &uinfo); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	if err = globalIAMSys.SetUser(accessKey, uinfo); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other Minio peers to reload user
	for _, nerr := range globalNotificationSys.LoadUser(accessKey, false) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// AddServiceAccount - PUT /minio/admin/v3/add-service-account
func (a adminAPIHandlers) AddServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddServiceAccount")

	// Get current object layer instance.
	objectAPI := newObjectLayerWithoutSafeModeFn()
	if objectAPI == nil || globalNotificationSys == nil || globalIAMSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, _, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	password := cred.SecretKey
	configBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var createReq madmin.AddServiceAccountReq
	if err = json.Unmarshal(configBytes, &createReq); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	if createReq.Parent == "" {
		apiErr := APIError{
			Code:           "XMinioAdminInvalidArgument",
			Description:    "Service account parent cannot be empty",
			HTTPStatusCode: http.StatusBadRequest,
		}
		writeErrorResponseJSON(ctx, w, apiErr, r.URL)
		return
	}

	// Disallow creating service accounts by root user as well.
	if createReq.Parent == globalActiveCred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddServiceAccountInvalidArgument), r.URL)
		return
	}

	// Disallow creating service accounts by users who are not the requested parent.
	// this restriction is not required for Owner account i.e root user.
	if createReq.Parent != cred.AccessKey && !owner {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddServiceAccountInvalidParent), r.URL)
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	creds, err := globalIAMSys.NewServiceAccount(ctx, createReq.Parent, createReq.Policy)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other Minio peers to reload user
	for _, nerr := range globalNotificationSys.LoadUser(creds.AccessKey, false) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}

	var createResp = madmin.AddServiceAccountResp{
		Credentials: auth.Credentials{
			AccessKey: creds.AccessKey,
			SecretKey: creds.SecretKey,
		},
	}

	data, err := json.Marshal(createResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, econfigData)
}

// InfoCannedPolicyV2 - GET /minio/admin/v2/info-canned-policy?name={policyName}
func (a adminAPIHandlers) InfoCannedPolicyV2(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "InfoCannedPolicyV2")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	policy, err := globalIAMSys.InfoPolicy(mux.Vars(r)["name"])
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	data, err := json.Marshal(policy)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.Write(data)
	w.(http.Flusher).Flush()
}

// InfoCannedPolicy - GET /minio/admin/v3/info-canned-policy?name={policyName}
func (a adminAPIHandlers) InfoCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "InfoCannedPolicy")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.GetPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	policy, err := globalIAMSys.InfoPolicy(mux.Vars(r)["name"])
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	json.NewEncoder(w).Encode(policy)
	w.(http.Flusher).Flush()
}

// ListCannedPoliciesV2 - GET /minio/admin/v2/list-canned-policies
func (a adminAPIHandlers) ListCannedPoliciesV2(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListCannedPoliciesV2")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	policies, err := globalIAMSys.ListPolicies()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	policyMap := make(map[string][]byte, len(policies))
	for k, p := range policies {
		var err error
		policyMap[k], err = json.Marshal(p)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
	}
	if err = json.NewEncoder(w).Encode(policyMap); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

// ListCannedPolicies - GET /minio/admin/v3/list-canned-policies
func (a adminAPIHandlers) ListCannedPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListCannedPolicies")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	policies, err := globalIAMSys.ListPolicies()
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var newPolicies = make(map[string]iampolicy.Policy)
	for name, p := range policies {
		_, err = json.Marshal(p)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		newPolicies[name] = p
	}
	if err = json.NewEncoder(w).Encode(newPolicies); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.(http.Flusher).Flush()
}

// RemoveCannedPolicy - DELETE /minio/admin/v3/remove-canned-policy?name=<policy_name>
func (a adminAPIHandlers) RemoveCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveCannedPolicy")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.DeletePolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	if err := globalIAMSys.DeletePolicy(policyName); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to delete policy
	for _, nerr := range globalNotificationSys.DeletePolicy(policyName) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// AddCannedPolicy - PUT /minio/admin/v3/add-canned-policy?name=<policy_name>
func (a adminAPIHandlers) AddCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddCannedPolicy")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.CreatePolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	// Error out if Content-Length is missing.
	if r.ContentLength <= 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// Error out if Content-Length is beyond allowed size.
	if r.ContentLength > maxBucketPolicySize {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	iamPolicy, err := iampolicy.ParseConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Version in policy must not be empty
	if iamPolicy.Version == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPolicy), r.URL)
		return
	}

	if err = globalIAMSys.SetPolicy(policyName, *iamPolicy); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload policy
	for _, nerr := range globalNotificationSys.LoadPolicy(policyName) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}

// SetPolicyForUserOrGroup - PUT /minio/admin/v3/set-policy?policy=xxx&user-or-group=?[&is-group]
func (a adminAPIHandlers) SetPolicyForUserOrGroup(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetPolicyForUserOrGroup")

	objectAPI, _ := validateAdminUsersReq(ctx, w, r, iampolicy.AttachPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["policyName"]
	entityName := vars["userOrGroup"]
	isGroup := vars["isGroup"] == "true"

	// Deny if WORM is enabled
	if globalWORMEnabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	if !isGroup {
		ok, err := globalIAMSys.IsTempUser(entityName)
		if err != nil && err != errNoSuchUser {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if ok {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
			return
		}
	}

	if err := globalIAMSys.PolicyDBSet(entityName, policyName, isGroup); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Notify all other MinIO peers to reload policy
	for _, nerr := range globalNotificationSys.LoadPolicyMapping(entityName, isGroup) {
		if nerr.Err != nil {
			logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
			logger.LogIf(ctx, nerr.Err)
		}
	}
}
