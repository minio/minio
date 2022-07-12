// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
)

// RemoveUser - DELETE /minio/admin/v3/remove-user?accessKey=<access_key>
func (a adminAPIHandlers) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveUser")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DeleteUserAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	ok, _, err := globalIAMSys.IsTempUser(accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if ok {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
		return
	}

	if err := globalIAMSys.DeleteUser(ctx, accessKey, true); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: true,
		},
		UpdatedAt: UTCNow(),
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// ListBucketUsers - GET /minio/admin/v3/list-users?bucket={bucket}
func (a adminAPIHandlers) ListBucketUsers(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBucketUsers")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ListUsersAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := mux.Vars(r)["bucket"]

	password := cred.SecretKey

	allCredentials, err := globalIAMSys.ListBucketUsers(ctx, bucket)
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

// ListUsers - GET /minio/admin/v3/list-users
func (a adminAPIHandlers) ListUsers(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListUsers")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.ListUsersAdminAction)
	if objectAPI == nil {
		return
	}

	password := cred.SecretKey

	allCredentials, err := globalIAMSys.ListUsers(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Add ldap users which have mapped policies if in LDAP mode
	// FIXME(vadmeste): move this to policy info in the future
	ldapUsers, err := globalIAMSys.ListLDAPUsers()
	if err != nil && err != errIAMActionNotAllowed {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	for k, v := range ldapUsers {
		allCredentials[k] = v
	}

	// Marshal the response
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	name := vars["accessKey"]

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	checkDenyOnly := false
	if name == cred.AccessKey {
		// Check that there is no explicit deny - otherwise it's allowed
		// to view one's own info.
		checkDenyOnly = true
	}

	if !globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          iampolicy.GetUserAdminAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
		DenyOnly:        checkDenyOnly,
	}) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	userInfo, err := globalIAMSys.GetUserInfo(ctx, name)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.AddUserToGroupAdminAction)
	if objectAPI == nil {
		return
	}

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
	var updatedAt time.Time
	if updReq.IsRemove {
		updatedAt, err = globalIAMSys.RemoveUsersFromGroup(ctx, updReq.Group, updReq.Members)
	} else {
		// Check if group already exists
		if _, gerr := globalIAMSys.GetGroupDescription(updReq.Group); gerr != nil {
			// If group does not exist, then check if the group has beginning and end space characters
			// we will reject such group names.
			if errors.Is(gerr, errNoSuchGroup) && hasSpaceBE(updReq.Group) {
				writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
				return
			}
		}
		updatedAt, err = globalIAMSys.AddUsersToGroup(ctx, updReq.Group, updReq.Members)
	}
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemGroupInfo,
		GroupInfo: &madmin.SRGroupInfo{
			UpdateReq: updReq,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// GetGroup - /minio/admin/v3/group?group=mygroup1
func (a adminAPIHandlers) GetGroup(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetGroup")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.GetGroupAdminAction)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ListGroupsAdminAction)
	if objectAPI == nil {
		return
	}

	groups, err := globalIAMSys.ListGroups(ctx)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.EnableGroupAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	group := vars["group"]
	status := vars["status"]

	var (
		err       error
		updatedAt time.Time
	)
	switch status {
	case statusEnabled:
		updatedAt, err = globalIAMSys.SetGroupStatus(ctx, group, true)
	case statusDisabled:
		updatedAt, err = globalIAMSys.SetGroupStatus(ctx, group, false)
	default:
		err = errInvalidArgument
	}
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemGroupInfo,
		GroupInfo: &madmin.SRGroupInfo{
			UpdateReq: madmin.GroupAddRemove{
				Group:    group,
				Status:   madmin.GroupStatus(status),
				IsRemove: false,
			},
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SetUserStatus - PUT /minio/admin/v3/set-user-status?accessKey=<access_key>&status=[enabled|disabled]
func (a adminAPIHandlers) SetUserStatus(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetUserStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.EnableUserAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	status := vars["status"]

	// This API is not allowed to lookup master access key user status
	if accessKey == globalActiveCred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.SetUserStatus(ctx, accessKey, madmin.AccountStatus(status))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: false,
			UserReq: &madmin.AddOrUpdateUserReq{
				Status: madmin.AccountStatus(status),
			},
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// AddUser - PUT /minio/admin/v3/add-user?accessKey=<access_key>
func (a adminAPIHandlers) AddUser(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddUser")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	// Not allowed to add a user with same access key as root credential
	if owner && accessKey == cred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	user, exists := globalIAMSys.GetUser(ctx, accessKey)
	if exists && (user.Credentials.IsTemp() || user.Credentials.IsServiceAccount()) {
		// Updating STS credential is not allowed, and this API does not
		// support updating service accounts.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	if (cred.IsTemp() || cred.IsServiceAccount()) && cred.ParentUser == accessKey {
		// Incoming access key matches parent user then we should
		// reject password change requests.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	// Check if accessKey has beginning and end space characters, this only applies to new users.
	if !exists && hasSpaceBE(accessKey) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
		return
	}

	checkDenyOnly := false
	if accessKey == cred.AccessKey {
		// Check that there is no explicit deny - otherwise it's allowed
		// to change one's own password.
		checkDenyOnly = true
	}

	if !globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          iampolicy.CreateUserAdminAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
		DenyOnly:        checkDenyOnly,
	}) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
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

	var ureq madmin.AddOrUpdateUserReq
	if err = json.Unmarshal(configBytes, &ureq); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.CreateUser(ctx, accessKey, ureq)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: false,
			UserReq:     &ureq,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// AddServiceAccount - PUT /minio/admin/v3/add-service-account
func (a adminAPIHandlers) AddServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddServiceAccount")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var createReq madmin.AddServiceAccountReq
	if err = json.Unmarshal(reqBytes, &createReq); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	// service account access key cannot have space characters beginning and end of the string.
	if hasSpaceBE(createReq.AccessKey) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
		return
	}

	var (
		targetUser   string
		targetGroups []string
	)

	// If the request did not set a TargetUser, the service account is
	// created for the request sender.
	targetUser = createReq.TargetUser
	if targetUser == "" {
		targetUser = cred.AccessKey
	}

	opts := newServiceAccountOpts{
		accessKey: createReq.AccessKey,
		secretKey: createReq.SecretKey,
		claims:    make(map[string]interface{}),
	}

	// Find the user for the request sender (as it may be sent via a service
	// account or STS account):
	requestorUser := cred.AccessKey
	requestorParentUser := cred.AccessKey
	requestorGroups := cred.Groups
	requestorIsDerivedCredential := false
	if cred.IsServiceAccount() || cred.IsTemp() {
		requestorParentUser = cred.ParentUser
		requestorIsDerivedCredential = true
	}

	// Check if we are creating svc account for request sender.
	isSvcAccForRequestor := false
	if targetUser == requestorUser || targetUser == requestorParentUser {
		isSvcAccForRequestor = true
	}

	// If we are creating svc account for request sender, ensure
	// that targetUser is a real user (i.e. not derived
	// credentials).
	if isSvcAccForRequestor {
		// Check if adding service account is explicitly denied.
		//
		// This allows turning off service accounts for request sender,
		// if there is no deny statement this call is implicitly enabled.
		if !globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     requestorUser,
			Groups:          requestorGroups,
			Action:          iampolicy.CreateServiceAccountAdminAction,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			Claims:          claims,
			DenyOnly:        true,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}

		if requestorIsDerivedCredential {
			if requestorParentUser == "" {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx,
					errors.New("service accounts cannot be generated for temporary credentials without parent")), r.URL)
				return
			}
			targetUser = requestorParentUser
		}
		targetGroups = requestorGroups

		// In case of LDAP/OIDC we need to set `opts.claims` to ensure
		// it is associated with the LDAP/OIDC user properly.
		for k, v := range cred.Claims {
			if k == expClaim {
				continue
			}
			opts.claims[k] = v
		}
	} else {
		// Need permission if we are creating a service account for a
		// user <> to the request sender
		if !globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     requestorUser,
			Groups:          requestorGroups,
			Action:          iampolicy.CreateServiceAccountAdminAction,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			Claims:          claims,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}

		// In case of LDAP we need to resolve the targetUser to a DN and
		// query their groups:
		if globalLDAPConfig.Enabled {
			opts.claims[ldapUserN] = targetUser // simple username
			targetUser, targetGroups, err = globalLDAPConfig.LookupUserDN(targetUser)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			opts.claims[ldapUser] = targetUser // username DN
		}

		// NOTE: if not using LDAP, then internal IDP or open ID is
		// being used - in the former, group info is enforced when
		// generated credentials are used to make requests, and in the
		// latter, a group notion is not supported.
	}

	var sp *iampolicy.Policy
	if len(createReq.Policy) > 0 {
		sp, err = iampolicy.ParseConfig(bytes.NewReader(createReq.Policy))
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	opts.sessionPolicy = sp
	newCred, updatedAt, err := globalIAMSys.NewServiceAccount(ctx, targetUser, targetGroups, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	createResp := madmin.AddServiceAccountResp{
		Credentials: madmin.Credentials{
			AccessKey: newCred.AccessKey,
			SecretKey: newCred.SecretKey,
		},
	}

	data, err := json.Marshal(createResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)

	// Call hook for cluster-replication if the service account is not for a
	// root user.
	if newCred.ParentUser != globalActiveCred.AccessKey {
		err = globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Create: &madmin.SRSvcAccCreate{
					Parent:        newCred.ParentUser,
					AccessKey:     newCred.AccessKey,
					SecretKey:     newCred.SecretKey,
					Groups:        newCred.Groups,
					Claims:        opts.claims,
					SessionPolicy: createReq.Policy,
					Status:        auth.AccountOn,
				},
			},
			UpdatedAt: updatedAt,
		})
		if err != nil {
			logger.LogIf(ctx, err)
			return
		}
	}
}

// UpdateServiceAccount - POST /minio/admin/v3/update-service-account
func (a adminAPIHandlers) UpdateServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "UpdateServiceAccount")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	accessKey := mux.Vars(r)["accessKey"]
	if accessKey == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	svcAccount, _, err := globalIAMSys.GetServiceAccount(ctx, accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if !globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          iampolicy.UpdateServiceAccountAdminAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
	}) {
		requestUser := cred.AccessKey
		if cred.ParentUser != "" {
			requestUser = cred.ParentUser
		}

		if requestUser != svcAccount.ParentUser {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var updateReq madmin.UpdateServiceAccountReq
	if err = json.Unmarshal(reqBytes, &updateReq); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err), r.URL)
		return
	}

	var sp *iampolicy.Policy
	if len(updateReq.NewPolicy) > 0 {
		sp, err = iampolicy.ParseConfig(bytes.NewReader(updateReq.NewPolicy))
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	opts := updateServiceAccountOpts{
		secretKey:     updateReq.NewSecretKey,
		status:        updateReq.NewStatus,
		sessionPolicy: sp,
	}
	updatedAt, err := globalIAMSys.UpdateServiceAccount(ctx, accessKey, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call site replication hook - non-root user accounts are replicated.
	if svcAccount.ParentUser != globalActiveCred.AccessKey {
		err = globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Update: &madmin.SRSvcAccUpdate{
					AccessKey:     accessKey,
					SecretKey:     opts.secretKey,
					Status:        opts.status,
					SessionPolicy: updateReq.NewPolicy,
				},
			},
			UpdatedAt: updatedAt,
		})
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	writeSuccessNoContent(w)
}

// InfoServiceAccount - GET /minio/admin/v3/info-service-account
func (a adminAPIHandlers) InfoServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "InfoServiceAccount")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	accessKey := mux.Vars(r)["accessKey"]
	if accessKey == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	svcAccount, policy, err := globalIAMSys.GetServiceAccount(ctx, accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if !globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          iampolicy.ListServiceAccountsAdminAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
	}) {
		requestUser := cred.AccessKey
		if cred.ParentUser != "" {
			requestUser = cred.ParentUser
		}

		if requestUser != svcAccount.ParentUser {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	}

	var svcAccountPolicy iampolicy.Policy

	if policy != nil {
		svcAccountPolicy = *policy
	} else {
		policiesNames, err := globalIAMSys.PolicyDBGet(svcAccount.ParentUser, false)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		svcAccountPolicy = globalIAMSys.GetCombinedPolicy(policiesNames...)
	}

	policyJSON, err := json.MarshalIndent(svcAccountPolicy, "", " ")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	infoResp := madmin.InfoServiceAccountResp{
		ParentUser:    svcAccount.ParentUser,
		AccountStatus: svcAccount.Status,
		ImpliedPolicy: policy == nil,
		Policy:        string(policyJSON),
	}

	data, err := json.Marshal(infoResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(cred.SecretKey, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)
}

// ListServiceAccounts - GET /minio/admin/v3/list-service-accounts
func (a adminAPIHandlers) ListServiceAccounts(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListServiceAccounts")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	var targetAccount string

	// If listing is requested for a specific user (who is not the request
	// sender), check that the user has permissions.
	user := r.Form.Get("user")
	if user != "" && user != cred.AccessKey {
		if !globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Action:          iampolicy.ListServiceAccountsAdminAction,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			Claims:          claims,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
		targetAccount = user
	} else {
		targetAccount = cred.AccessKey
		if cred.ParentUser != "" {
			targetAccount = cred.ParentUser
		}
	}

	serviceAccounts, err := globalIAMSys.ListServiceAccounts(ctx, targetAccount)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var serviceAccountsNames []string

	for _, svc := range serviceAccounts {
		serviceAccountsNames = append(serviceAccountsNames, svc.AccessKey)
	}

	listResp := madmin.ListServiceAccountsResp{
		Accounts: serviceAccountsNames,
	}

	data, err := json.Marshal(listResp)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	encryptedData, err := madmin.EncryptData(cred.SecretKey, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)
}

// DeleteServiceAccount - DELETE /minio/admin/v3/delete-service-account
func (a adminAPIHandlers) DeleteServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteServiceAccount")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	serviceAccount := mux.Vars(r)["accessKey"]
	if serviceAccount == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}

	// We do not care if service account is readable or not at this point,
	// since this is a delete call we shall allow it to be deleted if possible.
	svcAccount, _, _ := globalIAMSys.GetServiceAccount(ctx, serviceAccount)

	adminPrivilege := globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Action:          iampolicy.RemoveServiceAccountAdminAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
		IsOwner:         owner,
		Claims:          claims,
	})

	if !adminPrivilege {
		parentUser := cred.AccessKey
		if cred.ParentUser != "" {
			parentUser = cred.ParentUser
		}
		if svcAccount.ParentUser != "" && parentUser != svcAccount.ParentUser {
			// The service account belongs to another user but return not
			// found error to mitigate brute force attacks. or the
			// serviceAccount doesn't exist.
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminServiceAccountNotFound), r.URL)
			return
		}
	}

	if err := globalIAMSys.DeleteServiceAccount(ctx, serviceAccount, true); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call site replication hook - non-root user accounts are replicated.
	if svcAccount.ParentUser != "" && svcAccount.ParentUser != globalActiveCred.AccessKey {
		if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Delete: &madmin.SRSvcAccDelete{
					AccessKey: serviceAccount,
				},
			},
			UpdatedAt: UTCNow(),
		}); err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	writeSuccessNoContent(w)
}

// AccountInfoHandler returns usage
func (a adminAPIHandlers) AccountInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AccountInfo")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	// Set prefix value for "s3:prefix" policy conditionals.
	r.Header.Set("prefix", "")

	// Set delimiter value for "s3:delimiter" policy conditionals.
	r.Header.Set("delimiter", SlashSeparator)

	// Check if we are asked to return prefix usage
	enablePrefixUsage := r.Form.Get("prefix-usage") == "true"

	isAllowedAccess := func(bucketName string) (rd, wr bool) {
		if globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          iampolicy.ListBucketAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          claims,
		}) {
			rd = true
		}

		if globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          iampolicy.GetBucketLocationAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          claims,
		}) {
			rd = true
		}

		if globalIAMSys.IsAllowed(iampolicy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          iampolicy.PutObjectAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          claims,
		}) {
			wr = true
		}

		return rd, wr
	}

	var dataUsageInfo DataUsageInfo
	var err error
	if !globalIsGateway {
		// Load the latest calculated data usage
		dataUsageInfo, _ = loadDataUsageFromBackend(ctx, objectAPI)
	}

	// If etcd, dns federation configured list buckets from etcd.
	var buckets []BucketInfo
	if globalDNSConfig != nil && globalBucketFederation {
		dnsBuckets, err := globalDNSConfig.List()
		if err != nil && !IsErrIgnored(err,
			dns.ErrNoEntriesFound,
			dns.ErrDomainMissing) {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		for _, dnsRecords := range dnsBuckets {
			buckets = append(buckets, BucketInfo{
				Name:    dnsRecords[0].Key,
				Created: dnsRecords[0].CreationDate,
			})
		}
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].Name < buckets[j].Name
		})
	} else {
		buckets, err = objectAPI.ListBuckets(ctx, BucketOptions{})
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	accountName := cred.AccessKey
	if cred.IsTemp() || cred.IsServiceAccount() {
		// For derived credentials, check the parent user's permissions.
		accountName = cred.ParentUser
	}
	policies, err := globalIAMSys.PolicyDBGet(accountName, false, cred.Groups...)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	buf, err := json.MarshalIndent(globalIAMSys.GetCombinedPolicy(policies...), "", " ")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	acctInfo := madmin.AccountInfo{
		AccountName: accountName,
		Server:      objectAPI.BackendInfo(),
		Policy:      buf,
	}

	for _, bucket := range buckets {
		rd, wr := isAllowedAccess(bucket.Name)
		if rd || wr {
			// Fetch the data usage of the current bucket
			var size uint64
			var objectsCount uint64
			var objectsHist map[string]uint64
			if !dataUsageInfo.LastUpdate.IsZero() {
				size = dataUsageInfo.BucketsUsage[bucket.Name].Size
				objectsCount = dataUsageInfo.BucketsUsage[bucket.Name].ObjectsCount
				objectsHist = dataUsageInfo.BucketsUsage[bucket.Name].ObjectSizesHistogram
			}
			// Fetch the prefix usage of the current bucket
			var prefixUsage map[string]uint64
			if enablePrefixUsage {
				prefixUsage, _ = loadPrefixUsageFromBackend(ctx, objectAPI, bucket.Name)
			}

			lcfg, _ := globalBucketObjectLockSys.Get(bucket.Name)
			quota, _ := globalBucketQuotaSys.Get(ctx, bucket.Name)
			rcfg, _, _ := globalBucketMetadataSys.GetReplicationConfig(ctx, bucket.Name)
			tcfg, _, _ := globalBucketMetadataSys.GetTaggingConfig(bucket.Name)

			acctInfo.Buckets = append(acctInfo.Buckets, madmin.BucketAccessInfo{
				Name:                 bucket.Name,
				Created:              bucket.Created,
				Size:                 size,
				Objects:              objectsCount,
				ObjectSizesHistogram: objectsHist,
				PrefixUsage:          prefixUsage,
				Details: &madmin.BucketDetails{
					Versioning:          globalBucketVersioningSys.Enabled(bucket.Name),
					VersioningSuspended: globalBucketVersioningSys.Suspended(bucket.Name),
					Replication:         rcfg != nil,
					Locking:             lcfg.LockEnabled,
					Quota:               quota,
					Tagging:             tcfg,
				},
				Access: madmin.AccountAccess{
					Read:  rd,
					Write: wr,
				},
			})
		}
	}

	usageInfoJSON, err := json.Marshal(acctInfo)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, usageInfoJSON)
}

// InfoCannedPolicy - GET /minio/admin/v3/info-canned-policy?name={policyName}
//
// Newer API response with policy timestamps is returned with query parameter
// `v=2` like:
//
// GET /minio/admin/v3/info-canned-policy?name={policyName}&v=2
//
// The newer API will eventually become the default (and only) one. The older
// response is to return only the policy JSON. The newer response returns
// timestamps along with the policy JSON. Both versions are supported for now,
// for smooth transition to new API.
func (a adminAPIHandlers) InfoCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "InfoCannedPolicy")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.GetPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	name := mux.Vars(r)["name"]
	policies := newMappedPolicy(name).toSlice()
	if len(policies) != 1 {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTooManyPolicies), r.URL)
		return
	}

	policyDoc, err := globalIAMSys.InfoPolicy(name)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Is the new API version being requested?
	infoPolicyAPIVersion := r.Form.Get("v")
	if infoPolicyAPIVersion == "2" {
		buf, err := json.MarshalIndent(policyDoc, "", " ")
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		w.Write(buf)
		return
	} else if infoPolicyAPIVersion != "" {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errors.New("invalid version parameter 'v' supplied")), r.URL)
		return
	}

	// Return the older API response value of just the policy json.
	buf, err := json.MarshalIndent(policyDoc.Policy, "", " ")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	w.Write(buf)
}

// ListBucketPolicies - GET /minio/admin/v3/list-canned-policies?bucket={bucket}
func (a adminAPIHandlers) ListBucketPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBucketPolicies")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := mux.Vars(r)["bucket"]
	policies, err := globalIAMSys.ListPolicies(ctx, bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	newPolicies := make(map[string]iampolicy.Policy)
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
}

// ListCannedPolicies - GET /minio/admin/v3/list-canned-policies
func (a adminAPIHandlers) ListCannedPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListCannedPolicies")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	policies, err := globalIAMSys.ListPolicies(ctx, "")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	newPolicies := make(map[string]iampolicy.Policy)
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
}

// RemoveCannedPolicy - DELETE /minio/admin/v3/remove-canned-policy?name=<policy_name>
func (a adminAPIHandlers) RemoveCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RemoveCannedPolicy")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DeletePolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	if err := globalIAMSys.DeletePolicy(ctx, policyName, true); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call cluster-replication policy creation hook to replicate policy deletion to
	// other minio clusters.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type:      madmin.SRIAMItemPolicy,
		Name:      policyName,
		UpdatedAt: UTCNow(),
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// AddCannedPolicy - PUT /minio/admin/v3/add-canned-policy?name=<policy_name>
func (a adminAPIHandlers) AddCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AddCannedPolicy")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.CreatePolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]

	// Policy has space characters in begin and end reject such inputs.
	if hasSpaceBE(policyName) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
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

	iamPolicyBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	iamPolicy, err := iampolicy.ParseConfig(bytes.NewReader(iamPolicyBytes))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Version in policy must not be empty
	if iamPolicy.Version == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrMalformedPolicy), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.SetPolicy(ctx, policyName, *iamPolicy)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call cluster-replication policy creation hook to replicate policy to
	// other minio clusters.
	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type:      madmin.SRIAMItemPolicy,
		Name:      policyName,
		Policy:    iamPolicyBytes,
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

// SetPolicyForUserOrGroup - PUT /minio/admin/v3/set-policy?policy=xxx&user-or-group=?[&is-group]
func (a adminAPIHandlers) SetPolicyForUserOrGroup(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SetPolicyForUserOrGroup")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.AttachPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["policyName"]
	entityName := vars["userOrGroup"]
	isGroup := vars["isGroup"] == "true"

	if !isGroup {
		ok, _, err := globalIAMSys.IsTempUser(entityName)
		if err != nil && err != errNoSuchUser {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if ok {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
			return
		}
	}

	updatedAt, err := globalIAMSys.PolicyDBSet(ctx, entityName, policyName, isGroup)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemPolicyMapping,
		PolicyMapping: &madmin.SRPolicyMapping{
			UserOrGroup: entityName,
			IsGroup:     isGroup,
			Policy:      policyName,
		},
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

const (
	allPoliciesFile            = "policies.json"
	allUsersFile               = "users.json"
	allGroupsFile              = "groups.json"
	allSvcAcctsFile            = "svcaccts.json"
	userPolicyMappingsFile     = "user_mappings.json"
	groupPolicyMappingsFile    = "group_mappings.json"
	stsUserPolicyMappingsFile  = "stsuser_mappings.json"
	stsGroupPolicyMappingsFile = "stsgroup_mappings.json"
	iamAssetsDir               = "iam-assets"
)

// ExportIAMHandler - exports all iam info as a zipped file
func (a adminAPIHandlers) ExportIAM(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ExportIAM")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ExportIAMAction)
	if objectAPI == nil {
		return
	}
	// Initialize a zip writer which will provide a zipped content
	// of bucket metadata
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()
	rawDataFn := func(r io.Reader, filename string, sz int) error {
		header, zerr := zip.FileInfoHeader(dummyFileInfo{
			name:    filename,
			size:    int64(sz),
			mode:    0o600,
			modTime: time.Now(),
			isDir:   false,
			sys:     nil,
		})
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		header.Method = zip.Deflate
		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			logger.LogIf(ctx, zerr)
			return nil
		}
		if _, err := io.Copy(zwriter, r); err != nil {
			logger.LogIf(ctx, err)
		}
		return nil
	}

	iamFiles := []string{
		allPoliciesFile,
		allUsersFile,
		allGroupsFile,
		allSvcAcctsFile,
		userPolicyMappingsFile,
		groupPolicyMappingsFile,
		stsUserPolicyMappingsFile,
		stsGroupPolicyMappingsFile,
	}
	for _, f := range iamFiles {
		iamFile := pathJoin(iamAssetsDir, f)
		switch f {
		case allPoliciesFile:
			allPolicies, err := globalIAMSys.ListPolicies(ctx, "")
			if err != nil {
				logger.LogIf(ctx, err)
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			policiesData, err := json.Marshal(allPolicies)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			if err = rawDataFn(bytes.NewReader(policiesData), iamFile, len(policiesData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case allUsersFile:
			userIdentities := make(map[string]UserIdentity)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadUsers(ctx, regUser, userIdentities)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userAccounts := make(map[string]madmin.AddOrUpdateUserReq)
			for u, uid := range userIdentities {
				status := madmin.AccountDisabled
				if uid.Credentials.IsValid() {
					status = madmin.AccountEnabled
				}
				userAccounts[u] = madmin.AddOrUpdateUserReq{
					SecretKey: uid.Credentials.SecretKey,
					Status:    status,
				}
			}
			userData, err := json.Marshal(userAccounts)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(userData), iamFile, len(userData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case allGroupsFile:
			groups := make(map[string]GroupInfo)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadGroups(ctx, groups)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			groupData, err := json.Marshal(groups)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(groupData), iamFile, len(groupData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case allSvcAcctsFile:
			serviceAccounts := make(map[string]UserIdentity)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadUsers(ctx, svcUser, serviceAccounts)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			svcAccts := make(map[string]madmin.SRSvcAccCreate)
			for user, acc := range serviceAccounts {
				claims, err := globalIAMSys.GetClaimsForSvcAcc(ctx, acc.Credentials.AccessKey)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
					return
				}
				_, policy, err := globalIAMSys.GetServiceAccount(ctx, acc.Credentials.AccessKey)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
					return
				}

				var policyJSON []byte
				if policy != nil {
					policyJSON, err = json.Marshal(policy)
					if err != nil {
						writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
						return
					}
				}
				svcAccts[user] = madmin.SRSvcAccCreate{
					Parent:        acc.Credentials.ParentUser,
					AccessKey:     user,
					SecretKey:     acc.Credentials.SecretKey,
					Groups:        acc.Credentials.Groups,
					Claims:        claims,
					SessionPolicy: json.RawMessage(policyJSON),
					Status:        acc.Credentials.Status,
				}
			}

			svcAccData, err := json.Marshal(svcAccts)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(svcAccData), iamFile, len(svcAccData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case userPolicyMappingsFile:
			userPolicyMap := make(map[string]MappedPolicy)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadMappedPolicies(ctx, regUser, false, userPolicyMap)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userPolData, err := json.Marshal(userPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(userPolData), iamFile, len(userPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case groupPolicyMappingsFile:
			groupPolicyMap := make(map[string]MappedPolicy)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadMappedPolicies(ctx, regUser, true, groupPolicyMap)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			grpPolData, err := json.Marshal(groupPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(grpPolData), iamFile, len(grpPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case stsUserPolicyMappingsFile:
			userPolicyMap := make(map[string]MappedPolicy)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, false, userPolicyMap)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userPolData, err := json.Marshal(userPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			if err = rawDataFn(bytes.NewReader(userPolData), iamFile, len(userPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case stsGroupPolicyMappingsFile:
			groupPolicyMap := make(map[string]MappedPolicy)
			globalIAMSys.store.rlock()
			err := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, true, groupPolicyMap)
			globalIAMSys.store.runlock()
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			grpPolData, err := json.Marshal(groupPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			if err = rawDataFn(bytes.NewReader(grpPolData), iamFile, len(grpPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		}
	}
}

// ImportIAM - imports all IAM info into MinIO
func (a adminAPIHandlers) ImportIAM(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ImportIAM")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	cred, claims, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	reader := bytes.NewReader(data)
	zr, err := zip.NewReader(reader, int64(len(data)))
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}
	// import policies first
	{

		f, err := zr.Open(pathJoin(iamAssetsDir, allPoliciesFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allPoliciesFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var allPolicies map[string]iampolicy.Policy
			data, err = ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allPoliciesFile, ""), r.URL)
				return
			}
			err = json.Unmarshal(data, &allPolicies)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, allPoliciesFile, ""), r.URL)
				return
			}
			for policyName, policy := range allPolicies {
				if policy.IsEmpty() {
					err = globalIAMSys.DeletePolicy(ctx, policyName, true)
				} else {
					_, err = globalIAMSys.SetPolicy(ctx, policyName, policy)
				}
				if err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, allPoliciesFile, policyName), r.URL)
					return
				}
			}
		}
	}

	// import users
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, allUsersFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allUsersFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var userAccts map[string]madmin.AddOrUpdateUserReq
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allUsersFile, ""), r.URL)
				return
			}
			err = json.Unmarshal(data, &userAccts)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, allUsersFile, ""), r.URL)
				return
			}
			for accessKey, ureq := range userAccts {
				// Not allowed to add a user with same access key as root credential
				if owner && accessKey == cred.AccessKey {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAddUserInvalidArgument, err, allUsersFile, accessKey), r.URL)
					return
				}

				user, exists := globalIAMSys.GetUser(ctx, accessKey)
				if exists && (user.Credentials.IsTemp() || user.Credentials.IsServiceAccount()) {
					// Updating STS credential is not allowed, and this API does not
					// support updating service accounts.
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAddUserInvalidArgument, err, allUsersFile, accessKey), r.URL)
					return
				}

				if (cred.IsTemp() || cred.IsServiceAccount()) && cred.ParentUser == accessKey {
					// Incoming access key matches parent user then we should
					// reject password change requests.
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAddUserInvalidArgument, err, allUsersFile, accessKey), r.URL)
					return
				}

				// Check if accessKey has beginning and end space characters, this only applies to new users.
				if !exists && hasSpaceBE(accessKey) {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminResourceInvalidArgument, err, allUsersFile, accessKey), r.URL)
					return
				}

				checkDenyOnly := false
				if accessKey == cred.AccessKey {
					// Check that there is no explicit deny - otherwise it's allowed
					// to change one's own password.
					checkDenyOnly = true
				}

				if !globalIAMSys.IsAllowed(iampolicy.Args{
					AccountName:     cred.AccessKey,
					Groups:          cred.Groups,
					Action:          iampolicy.CreateUserAdminAction,
					ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
					IsOwner:         owner,
					Claims:          claims,
					DenyOnly:        checkDenyOnly,
				}) {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAccessDenied, err, allUsersFile, accessKey), r.URL)
					return
				}
				if _, err = globalIAMSys.CreateUser(ctx, accessKey, ureq); err != nil {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, toAdminAPIErrCode(ctx, err), err, allUsersFile, accessKey), r.URL)
					return
				}

			}
		}
	}

	// import groups
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, allGroupsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allGroupsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var grpInfos map[string]GroupInfo
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allGroupsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &grpInfos); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, allGroupsFile, ""), r.URL)
				return
			}
			for group, grpInfo := range grpInfos {
				// Check if group already exists
				if _, gerr := globalIAMSys.GetGroupDescription(group); gerr != nil {
					// If group does not exist, then check if the group has beginning and end space characters
					// we will reject such group names.
					if errors.Is(gerr, errNoSuchGroup) && hasSpaceBE(group) {
						writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminResourceInvalidArgument, err, allGroupsFile, group), r.URL)
						return
					}
				}
				if _, gerr := globalIAMSys.AddUsersToGroup(ctx, group, grpInfo.Members); gerr != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, allGroupsFile, group), r.URL)
					return
				}
			}
		}
	}

	// import service accounts
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, allSvcAcctsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allSvcAcctsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var serviceAcctReqs map[string]madmin.SRSvcAccCreate
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allSvcAcctsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &serviceAcctReqs); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, allSvcAcctsFile, ""), r.URL)
				return
			}
			for user, svcAcctReq := range serviceAcctReqs {
				var sp *iampolicy.Policy
				var err error
				if len(svcAcctReq.SessionPolicy) > 0 {
					sp, err = iampolicy.ParseConfig(bytes.NewReader(svcAcctReq.SessionPolicy))
					if err != nil {
						writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
						return
					}
				}
				// service account access key cannot have space characters beginning and end of the string.
				if hasSpaceBE(svcAcctReq.AccessKey) {
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
					return
				}
				if !globalIAMSys.IsAllowed(iampolicy.Args{
					AccountName:     svcAcctReq.AccessKey,
					Groups:          svcAcctReq.Groups,
					Action:          iampolicy.CreateServiceAccountAdminAction,
					ConditionValues: getConditionValues(r, "", cred.AccessKey, claims),
					IsOwner:         owner,
					Claims:          claims,
				}) {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAccessDenied, err, allSvcAcctsFile, user), r.URL)
					return
				}
				updateReq := true
				_, _, err = globalIAMSys.GetServiceAccount(ctx, svcAcctReq.AccessKey)
				if err != nil {
					if !errors.Is(err, errNoSuchServiceAccount) {
						writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
						return
					}
					updateReq = false
				}
				if updateReq {
					opts := updateServiceAccountOpts{
						secretKey:     svcAcctReq.SecretKey,
						status:        svcAcctReq.Status,
						sessionPolicy: sp,
					}
					_, err = globalIAMSys.UpdateServiceAccount(ctx, svcAcctReq.AccessKey, opts)
					if err != nil {
						writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
						return
					}
					continue
				}
				opts := newServiceAccountOpts{
					accessKey:     user,
					secretKey:     svcAcctReq.SecretKey,
					sessionPolicy: sp,
					claims:        svcAcctReq.Claims,
				}

				// In case of LDAP we need to resolve the targetUser to a DN and
				// query their groups:
				if globalLDAPConfig.Enabled {
					opts.claims[ldapUserN] = svcAcctReq.AccessKey // simple username
					targetUser, _, err := globalLDAPConfig.LookupUserDN(svcAcctReq.AccessKey)
					if err != nil {
						writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
						return
					}
					opts.claims[ldapUser] = targetUser // username DN
				}

				if _, _, err = globalIAMSys.NewServiceAccount(ctx, svcAcctReq.Parent, svcAcctReq.Groups, opts); err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
					return
				}

			}
		}
	}

	// import user policy mappings
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, userPolicyMappingsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, userPolicyMappingsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var userPolicyMap map[string]MappedPolicy
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, userPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &userPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, userPolicyMappingsFile, ""), r.URL)
				return
			}
			for u, pm := range userPolicyMap {
				// disallow setting policy mapping if user is a temporary user
				ok, _, err := globalIAMSys.IsTempUser(u)
				if err != nil && err != errNoSuchUser {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, userPolicyMappingsFile, u), r.URL)
					return
				}
				if ok {
					writeErrorResponseJSON(ctx, w, importError(ctx, errIAMActionNotAllowed, userPolicyMappingsFile, u), r.URL)
					return
				}
				if _, err := globalIAMSys.PolicyDBSet(ctx, u, pm.Policies, false); err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, userPolicyMappingsFile, u), r.URL)
					return
				}
			}
		}
	}

	// import group policy mappings
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, groupPolicyMappingsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, groupPolicyMappingsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var grpPolicyMap map[string]MappedPolicy
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, groupPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &grpPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, groupPolicyMappingsFile, ""), r.URL)
				return
			}
			for g, pm := range grpPolicyMap {
				if _, err := globalIAMSys.PolicyDBSet(ctx, g, pm.Policies, true); err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, groupPolicyMappingsFile, g), r.URL)
					return
				}
			}
		}
	}

	// import sts user policy mappings
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, stsUserPolicyMappingsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, stsUserPolicyMappingsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var userPolicyMap map[string]MappedPolicy
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, stsUserPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &userPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, stsUserPolicyMappingsFile, ""), r.URL)
				return
			}
			for u, pm := range userPolicyMap {
				// disallow setting policy mapping if user is a temporary user
				ok, _, err := globalIAMSys.IsTempUser(u)
				if err != nil && err != errNoSuchUser {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, stsUserPolicyMappingsFile, u), r.URL)
					return
				}
				if ok {
					writeErrorResponseJSON(ctx, w, importError(ctx, errIAMActionNotAllowed, stsUserPolicyMappingsFile, u), r.URL)
					return
				}
				if _, err := globalIAMSys.PolicyDBSet(ctx, u, pm.Policies, false); err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, stsUserPolicyMappingsFile, u), r.URL)
					return
				}
			}
		}
	}

	// import sts group policy mappings
	{
		f, err := zr.Open(pathJoin(iamAssetsDir, stsGroupPolicyMappingsFile))
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, stsGroupPolicyMappingsFile, ""), r.URL)
			return
		default:
			defer f.Close()
			var grpPolicyMap map[string]MappedPolicy
			data, err := ioutil.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, stsGroupPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &grpPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, stsGroupPolicyMappingsFile, ""), r.URL)
				return
			}
			for g, pm := range grpPolicyMap {
				if _, err := globalIAMSys.PolicyDBSet(ctx, g, pm.Policies, true); err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, stsGroupPolicyMappingsFile, g), r.URL)
					return
				}
			}
		}
	}
}
