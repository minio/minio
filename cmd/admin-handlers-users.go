// Copyright (c) 2015-2023 MinIO, Inc.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zip"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	xldap "github.com/minio/pkg/v3/ldap"
	"github.com/minio/pkg/v3/policy"
	"github.com/puzpuzpuz/xsync/v3"
)

// RemoveUser - DELETE /minio/admin/v3/remove-user?accessKey=<access_key>
func (a adminAPIHandlers) RemoveUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.DeleteUserAdminAction)
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

	// This API only supports removal of internal users not service accounts.
	ok, _, err = globalIAMSys.IsServiceAccount(accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if ok {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
		return
	}

	// When the user is root credential you are not allowed to
	// remove the root user. Also you cannot delete yourself.
	if accessKey == globalActiveCred.AccessKey || accessKey == cred.AccessKey {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
		return
	}

	if err := globalIAMSys.DeleteUser(ctx, accessKey, true); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: true,
		},
		UpdatedAt: UTCNow(),
	}))
}

// ListBucketUsers - GET /minio/admin/v3/list-users?bucket={bucket}
func (a adminAPIHandlers) ListBucketUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.ListUsersAdminAction)
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
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.ListUsersAdminAction)
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
	ldapUsers, err := globalIAMSys.ListLDAPUsers(ctx)
	if err != nil && err != errIAMActionNotAllowed {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	maps.Copy(allCredentials, ldapUsers)

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
	ctx := r.Context()

	vars := mux.Vars(r)
	name := vars["accessKey"]

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	checkDenyOnly := name == cred.AccessKey

	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.GetUserAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.AddUserToGroupAdminAction)
	if objectAPI == nil {
		return
	}

	data, err := io.ReadAll(r.Body)
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

	// Reject if the group add and remove are temporary credentials, or root credential.
	for _, member := range updReq.Members {
		ok, _, err := globalIAMSys.IsTempUser(member)
		if err != nil && err != errNoSuchUser {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if ok {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
			return
		}
		// When the user is root credential you are not allowed to
		// add policies for root user.
		if member == globalActiveCred.AccessKey {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
			return
		}
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

		if globalIAMSys.LDAPConfig.Enabled() {
			// We don't allow internal group manipulation in this API when LDAP
			// is enabled for now.
			err = errIAMActionNotAllowed
		} else {
			updatedAt, err = globalIAMSys.AddUsersToGroup(ctx, updReq.Group, updReq.Members)
		}
	}
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemGroupInfo,
		GroupInfo: &madmin.SRGroupInfo{
			UpdateReq: updReq,
		},
		UpdatedAt: updatedAt,
	}))
}

// GetGroup - /minio/admin/v3/group?group=mygroup1
func (a adminAPIHandlers) GetGroup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.GetGroupAdminAction)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ListGroupsAdminAction)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.EnableGroupAdminAction)
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

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemGroupInfo,
		GroupInfo: &madmin.SRGroupInfo{
			UpdateReq: madmin.GroupAddRemove{
				Group:    group,
				Status:   madmin.GroupStatus(status),
				IsRemove: false,
			},
		},
		UpdatedAt: updatedAt,
	}))
}

// SetUserStatus - PUT /minio/admin/v3/set-user-status?accessKey=<access_key>&status=[enabled|disabled]
func (a adminAPIHandlers) SetUserStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, creds := validateAdminReq(ctx, w, r, policy.EnableUserAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]
	status := vars["status"]

	// you cannot enable or disable yourself.
	if accessKey == creds.AccessKey {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.SetUserStatus(ctx, accessKey, madmin.AccountStatus(status))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: false,
			UserReq: &madmin.AddOrUpdateUserReq{
				Status: madmin.AccountStatus(status),
			},
		},
		UpdatedAt: updatedAt,
	}))
}

// AddUser - PUT /minio/admin/v3/add-user?accessKey=<access_key>
func (a adminAPIHandlers) AddUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	accessKey := vars["accessKey"]

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	// Not allowed to add a user with same access key as root credential
	if accessKey == globalActiveCred.AccessKey {
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

	if !utf8.ValidString(accessKey) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserValidUTF), r.URL)
		return
	}

	checkDenyOnly := accessKey == cred.AccessKey

	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.CreateUserAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
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
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var ureq madmin.AddOrUpdateUserReq
	if err = json.Unmarshal(configBytes, &ureq); err != nil {
		adminLogIf(ctx, err)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	// We don't allow internal user creation with LDAP enabled for now.
	if globalIAMSys.LDAPConfig.Enabled() {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.CreateUser(ctx, accessKey, ureq)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemIAMUser,
		IAMUser: &madmin.SRIAMUser{
			AccessKey:   accessKey,
			IsDeleteReq: false,
			UserReq:     &ureq,
		},
		UpdatedAt: updatedAt,
	}))
}

// TemporaryAccountInfo - GET /minio/admin/v3/temporary-account-info
func (a adminAPIHandlers) TemporaryAccountInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	accessKey := mux.Vars(r)["accessKey"]
	if accessKey == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	args := policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.ListTemporaryAccountsAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
	}

	if !globalIAMSys.IsAllowed(args) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	stsAccount, sessionPolicy, err := globalIAMSys.GetTemporaryAccount(ctx, accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var stsAccountPolicy policy.Policy

	if sessionPolicy != nil {
		stsAccountPolicy = *sessionPolicy
	} else {
		policiesNames, err := globalIAMSys.PolicyDBGet(stsAccount.ParentUser, stsAccount.Groups...)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if len(policiesNames) == 0 {
			policySet, _ := args.GetPolicies(iamPolicyClaimNameOpenID())
			policiesNames = policySet.ToSlice()
		}

		stsAccountPolicy = globalIAMSys.GetCombinedPolicy(policiesNames...)
	}

	policyJSON, err := json.MarshalIndent(stsAccountPolicy, "", " ")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	infoResp := madmin.TemporaryAccountInfoResp{
		ParentUser:    stsAccount.ParentUser,
		AccountStatus: stsAccount.Status,
		ImpliedPolicy: sessionPolicy == nil,
		Policy:        string(policyJSON),
		Expiration:    &stsAccount.Expiration,
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

// AddServiceAccount - PUT /minio/admin/v3/add-service-account
func (a adminAPIHandlers) AddServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx, cred, opts, createReq, targetUser, APIError := commonAddServiceAccount(r, false)
	if APIError.Code != "" {
		writeErrorResponseJSON(ctx, w, APIError, r.URL)
		return
	}

	if createReq.AccessKey == globalActiveCred.AccessKey {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAddUserInvalidArgument), r.URL)
		return
	}

	var (
		targetGroups []string
		err          error
	)

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

	if globalIAMSys.GetUsersSysType() == MinIOUsersSysType && targetUser != cred.AccessKey {
		// For internal IDP, ensure that the targetUser's parent account exists.
		// It could be a regular user account or the root account.
		_, isRegularUser := globalIAMSys.GetUser(ctx, targetUser)
		if !isRegularUser && targetUser != globalActiveCred.AccessKey {
			apiErr := toAdminAPIErr(ctx, errNoSuchUser)
			apiErr.Description = fmt.Sprintf("Specified target user %s does not exist", targetUser)
			writeErrorResponseJSON(ctx, w, apiErr, r.URL)
			return
		}
	}

	// Check if we are creating svc account for request sender.
	isSvcAccForRequestor := targetUser == requestorUser || targetUser == requestorParentUser

	// If we are creating svc account for request sender, ensure
	// that targetUser is a real user (i.e. not derived
	// credentials).
	if isSvcAccForRequestor {
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
	} else if globalIAMSys.LDAPConfig.Enabled() {
		// In case of LDAP we need to resolve the targetUser to a DN and
		// query their groups:
		opts.claims[ldapUserN] = targetUser // simple username
		var lookupResult *xldap.DNSearchResult
		lookupResult, targetGroups, err = globalIAMSys.LDAPConfig.LookupUserDN(targetUser)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		targetUser = lookupResult.NormDN
		opts.claims[ldapUser] = targetUser // username DN
		opts.claims[ldapActualUser] = lookupResult.ActualDN

		// Add LDAP attributes that were looked up into the claims.
		for attribKey, attribValue := range lookupResult.Attributes {
			opts.claims[ldapAttribPrefix+attribKey] = attribValue
		}

		// NOTE: if not using LDAP, then internal IDP or open ID is
		// being used - in the former, group info is enforced when
		// generated credentials are used to make requests, and in the
		// latter, a group notion is not supported.
	}

	newCred, updatedAt, err := globalIAMSys.NewServiceAccount(ctx, targetUser, targetGroups, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	createResp := madmin.AddServiceAccountResp{
		Credentials: madmin.Credentials{
			AccessKey:  newCred.AccessKey,
			SecretKey:  newCred.SecretKey,
			Expiration: newCred.Expiration,
		},
	}

	data, err := json.Marshal(createResp)
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

	// Call hook for cluster-replication if the service account is not for a
	// root user.
	if newCred.ParentUser != globalActiveCred.AccessKey {
		replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Create: &madmin.SRSvcAccCreate{
					Parent:        newCred.ParentUser,
					AccessKey:     newCred.AccessKey,
					SecretKey:     newCred.SecretKey,
					Groups:        newCred.Groups,
					Name:          newCred.Name,
					Description:   newCred.Description,
					Claims:        opts.claims,
					SessionPolicy: madmin.SRSessionPolicy(createReq.Policy),
					Status:        auth.AccountOn,
					Expiration:    createReq.Expiration,
				},
			},
			UpdatedAt: updatedAt,
		}))
	}
}

// UpdateServiceAccount - POST /minio/admin/v3/update-service-account
func (a adminAPIHandlers) UpdateServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
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

	if err := updateReq.Validate(); err != nil {
		// Since this validation would happen client side as well, we only send
		// a generic error message here.
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
		return
	}

	condValues := getConditionValues(r, "", cred)
	err = addExpirationToCondValues(updateReq.NewExpiration, condValues)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Permission checks:
	//
	// 1. Any type of account (i.e. access keys (previously/still called service
	// accounts), STS accounts, internal IDP accounts, etc) with the
	// policy.UpdateServiceAccountAdminAction permission can update any service
	// account.
	//
	// 2. We would like to let a user update their own access keys, however it
	// is currently blocked pending a re-design. Users are still able to delete
	// and re-create them.
	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.UpdateServiceAccountAdminAction,
		ConditionValues: condValues,
		IsOwner:         owner,
		Claims:          cred.Claims,
	}) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	var sp *policy.Policy
	if len(updateReq.NewPolicy) > 0 {
		sp, err = policy.ParseConfig(bytes.NewReader(updateReq.NewPolicy))
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if sp.Version == "" && len(sp.Statements) == 0 {
			sp = nil
		}
	}
	opts := updateServiceAccountOpts{
		secretKey:     updateReq.NewSecretKey,
		status:        updateReq.NewStatus,
		name:          updateReq.NewName,
		description:   updateReq.NewDescription,
		expiration:    updateReq.NewExpiration,
		sessionPolicy: sp,
	}
	updatedAt, err := globalIAMSys.UpdateServiceAccount(ctx, accessKey, opts)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call site replication hook - non-root user accounts are replicated.
	if svcAccount.ParentUser != globalActiveCred.AccessKey {
		replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Update: &madmin.SRSvcAccUpdate{
					AccessKey:     accessKey,
					SecretKey:     opts.secretKey,
					Status:        opts.status,
					Name:          opts.name,
					Description:   opts.description,
					SessionPolicy: madmin.SRSessionPolicy(updateReq.NewPolicy),
					Expiration:    updateReq.NewExpiration,
				},
			},
			UpdatedAt: updatedAt,
		}))
	}

	writeSuccessNoContent(w)
}

// InfoServiceAccount - GET /minio/admin/v3/info-service-account
func (a adminAPIHandlers) InfoServiceAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	accessKey := mux.Vars(r)["accessKey"]
	if accessKey == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	svcAccount, sessionPolicy, err := globalIAMSys.GetServiceAccount(ctx, accessKey)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.ListServiceAccountsAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
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

	// if session policy is nil or empty, then it is implied policy
	impliedPolicy := sessionPolicy == nil || (sessionPolicy.Version == "" && len(sessionPolicy.Statements) == 0)

	var svcAccountPolicy policy.Policy

	if !impliedPolicy {
		svcAccountPolicy = *sessionPolicy
	} else {
		policiesNames, err := globalIAMSys.PolicyDBGet(svcAccount.ParentUser, svcAccount.Groups...)
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

	var expiration *time.Time
	if !svcAccount.Expiration.IsZero() && !svcAccount.Expiration.Equal(timeSentinel) {
		expiration = &svcAccount.Expiration
	}

	infoResp := madmin.InfoServiceAccountResp{
		ParentUser:    svcAccount.ParentUser,
		Name:          svcAccount.Name,
		Description:   svcAccount.Description,
		AccountStatus: svcAccount.Status,
		ImpliedPolicy: impliedPolicy,
		Policy:        string(policyJSON),
		Expiration:    expiration,
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
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	var targetAccount string

	// If listing is requested for a specific user (who is not the request
	// sender), check that the user has permissions.
	user := r.Form.Get("user")
	if user != "" && user != cred.AccessKey {
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListServiceAccountsAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
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

	var serviceAccountList []madmin.ServiceAccountInfo

	for _, svc := range serviceAccounts {
		expiryTime := svc.Expiration
		serviceAccountList = append(serviceAccountList, madmin.ServiceAccountInfo{
			Description:   svc.Description,
			ParentUser:    svc.ParentUser,
			Name:          svc.Name,
			AccountStatus: svc.Status,
			AccessKey:     svc.AccessKey,
			ImpliedPolicy: svc.IsImpliedPolicy(),
			Expiration:    &expiryTime,
		})
	}

	listResp := madmin.ListServiceAccountsResp{
		Accounts: serviceAccountList,
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
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	serviceAccount := mux.Vars(r)["accessKey"]
	if serviceAccount == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}

	if serviceAccount == siteReplicatorSvcAcc && globalSiteReplicationSys.isEnabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidArgument), r.URL)
		return
	}
	// We do not care if service account is readable or not at this point,
	// since this is a delete call we shall allow it to be deleted if possible.
	svcAccount, _, err := globalIAMSys.GetServiceAccount(ctx, serviceAccount)
	if errors.Is(err, errNoSuchServiceAccount) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminServiceAccountNotFound), r.URL)
		return
	}

	adminPrivilege := globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.RemoveServiceAccountAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
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
		replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
			Type: madmin.SRIAMItemSvcAcc,
			SvcAccChange: &madmin.SRSvcAccChange{
				Delete: &madmin.SRSvcAccDelete{
					AccessKey: serviceAccount,
				},
			},
			UpdatedAt: UTCNow(),
		}))
	}

	writeSuccessNoContent(w)
}

// ListAccessKeysBulk - GET /minio/admin/v3/list-access-keys-bulk
func (a adminAPIHandlers) ListAccessKeysBulk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	users := r.Form["users"]
	isAll := r.Form.Get("all") == "true"
	selfOnly := !isAll && len(users) == 0

	if isAll && len(users) > 0 {
		// This should be checked on client side, so return generic error
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	// Empty user list and not self, list access keys for all users
	if isAll {
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListUsersAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	} else if len(users) == 1 {
		if users[0] == cred.AccessKey || users[0] == cred.ParentUser {
			selfOnly = true
		}
	}

	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.ListServiceAccountsAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
		DenyOnly:        selfOnly,
	}) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	if selfOnly && len(users) == 0 {
		selfUser := cred.AccessKey
		if cred.ParentUser != "" {
			selfUser = cred.ParentUser
		}
		users = append(users, selfUser)
	}

	var checkedUserList []string
	if isAll {
		users, err := globalIAMSys.ListUsers(ctx)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		for user := range users {
			checkedUserList = append(checkedUserList, user)
		}
		checkedUserList = append(checkedUserList, globalActiveCred.AccessKey)
	} else {
		for _, user := range users {
			// Validate the user
			_, ok := globalIAMSys.GetUser(ctx, user)
			if !ok {
				continue
			}
			checkedUserList = append(checkedUserList, user)
		}
	}

	listType := r.Form.Get("listType")
	var listSTSKeys, listServiceAccounts bool
	switch listType {
	case madmin.AccessKeyListUsersOnly:
		listSTSKeys = false
		listServiceAccounts = false
	case madmin.AccessKeyListSTSOnly:
		listSTSKeys = true
		listServiceAccounts = false
	case madmin.AccessKeyListSvcaccOnly:
		listSTSKeys = false
		listServiceAccounts = true
	case madmin.AccessKeyListAll:
		listSTSKeys = true
		listServiceAccounts = true
	default:
		err := errors.New("invalid list type")
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrInvalidRequest, err), r.URL)
		return
	}

	accessKeyMap := make(map[string]madmin.ListAccessKeysResp)
	for _, user := range checkedUserList {
		accessKeys := madmin.ListAccessKeysResp{}
		if listSTSKeys {
			stsKeys, err := globalIAMSys.ListSTSAccounts(ctx, user)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			for _, sts := range stsKeys {
				accessKeys.STSKeys = append(accessKeys.STSKeys, madmin.ServiceAccountInfo{
					AccessKey:  sts.AccessKey,
					Expiration: &sts.Expiration,
				})
			}
			// if only STS keys, skip if user has no STS keys
			if !listServiceAccounts && len(stsKeys) == 0 {
				continue
			}
		}

		if listServiceAccounts {
			serviceAccounts, err := globalIAMSys.ListServiceAccounts(ctx, user)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			for _, svc := range serviceAccounts {
				accessKeys.ServiceAccounts = append(accessKeys.ServiceAccounts, madmin.ServiceAccountInfo{
					AccessKey:  svc.AccessKey,
					Expiration: &svc.Expiration,
				})
			}
			// if only service accounts, skip if user has no service accounts
			if !listSTSKeys && len(serviceAccounts) == 0 {
				continue
			}
		}
		accessKeyMap[user] = accessKeys
	}

	data, err := json.Marshal(accessKeyMap)
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

// AccountInfoHandler returns usage, permissions and other bucket metadata for incoming us
func (a adminAPIHandlers) AccountInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
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
		if globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListBucketAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          cred.Claims,
		}) {
			rd = true
		}

		if globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.GetBucketLocationAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          cred.Claims,
		}) {
			rd = true
		}

		if globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.PutObjectAction,
			BucketName:      bucketName,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			ObjectName:      "",
			Claims:          cred.Claims,
		}) {
			wr = true
		}

		return rd, wr
	}

	// If etcd, dns federation configured list buckets from etcd.
	var err error
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
		buckets, err = objectAPI.ListBuckets(ctx, BucketOptions{Cached: true})
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

	roleArn := policy.Args{Claims: cred.Claims}.GetRoleArn()
	policySetFromClaims, hasPolicyClaim := policy.GetPoliciesFromClaims(cred.Claims, iamPolicyClaimNameOpenID())
	var effectivePolicy policy.Policy

	var buf []byte
	switch {
	case accountName == globalActiveCred.AccessKey || newGlobalAuthZPluginFn() != nil:
		// For owner account and when plugin authZ is configured always set
		// effective policy as `consoleAdmin`.
		//
		// In the latter case, we let the UI render everything, but individual
		// actions would fail if not permitted by the external authZ service.
		for _, policy := range policy.DefaultPolicies {
			if policy.Name == "consoleAdmin" {
				effectivePolicy = policy.Definition
				break
			}
		}

	case roleArn != "":
		_, policy, err := globalIAMSys.GetRolePolicy(roleArn)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		policySlice := newMappedPolicy(policy).toSlice()
		effectivePolicy = globalIAMSys.GetCombinedPolicy(policySlice...)

	case hasPolicyClaim:
		effectivePolicy = globalIAMSys.GetCombinedPolicy(policySetFromClaims.ToSlice()...)

	default:
		policies, err := globalIAMSys.PolicyDBGet(accountName, cred.Groups...)
		if err != nil {
			adminLogIf(ctx, err)
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		effectivePolicy = globalIAMSys.GetCombinedPolicy(policies...)
	}

	buf, err = json.MarshalIndent(effectivePolicy, "", " ")
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
			bui := globalBucketQuotaSys.GetBucketUsageInfo(ctx, bucket.Name)
			size := bui.Size
			objectsCount := bui.ObjectsCount
			objectsHist := bui.ObjectSizesHistogram
			versionsHist := bui.ObjectVersionsHistogram

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
				Name:                    bucket.Name,
				Created:                 bucket.Created,
				Size:                    size,
				Objects:                 objectsCount,
				ObjectSizesHistogram:    objectsHist,
				ObjectVersionsHistogram: versionsHist,
				PrefixUsage:             prefixUsage,
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.GetPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	name := mux.Vars(r)["name"]
	policies := newMappedPolicy(name).toSlice()
	if len(policies) != 1 {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errTooManyPolicies), r.URL)
		return
	}
	setReqInfoPolicyName(ctx, name)

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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	bucket := mux.Vars(r)["bucket"]
	policies, err := globalIAMSys.ListPolicies(ctx, bucket)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	newPolicies := make(map[string]policy.Policy)
	for name, p := range policies {
		_, err = json.Marshal(p)
		if err != nil {
			adminLogIf(ctx, err)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	policies, err := globalIAMSys.ListPolicies(ctx, "")
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	newPolicies := make(map[string]policy.Policy)
	for name, p := range policies {
		_, err = json.Marshal(p)
		if err != nil {
			adminLogIf(ctx, err)
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
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.DeletePolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["name"]
	setReqInfoPolicyName(ctx, policyName)

	if err := globalIAMSys.DeletePolicy(ctx, policyName, true); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call cluster-replication policy creation hook to replicate policy deletion to
	// other minio clusters.
	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type:      madmin.SRIAMItemPolicy,
		Name:      policyName,
		UpdatedAt: UTCNow(),
	}))
}

// AddCannedPolicy - PUT /minio/admin/v3/add-canned-policy?name=<policy_name>
func (a adminAPIHandlers) AddCannedPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.CreatePolicyAdminAction)
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
	setReqInfoPolicyName(ctx, policyName)

	// Reject policy names with commas.
	if strings.Contains(policyName, ",") {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrPolicyInvalidName), r.URL)
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

	iamPolicyBytes, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	iamPolicy, err := policy.ParseConfig(bytes.NewReader(iamPolicyBytes))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Version in policy must not be empty
	if iamPolicy.Version == "" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrPolicyInvalidVersion), r.URL)
		return
	}

	updatedAt, err := globalIAMSys.SetPolicy(ctx, policyName, *iamPolicy)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Call cluster-replication policy creation hook to replicate policy to
	// other minio clusters.
	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type:      madmin.SRIAMItemPolicy,
		Name:      policyName,
		Policy:    iamPolicyBytes,
		UpdatedAt: updatedAt,
	}))
}

// SetPolicyForUserOrGroup - sets a policy on a user or a group.
//
// PUT /minio/admin/v3/set-policy?policy=xxx&user-or-group=?[&is-group]
//
// Deprecated: This API is replaced by attach/detach policy APIs for specific
// type of users (builtin or LDAP).
func (a adminAPIHandlers) SetPolicyForUserOrGroup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.AttachPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	vars := mux.Vars(r)
	policyName := vars["policyName"]
	entityName := vars["userOrGroup"]
	isGroup := vars["isGroup"] == "true"
	setReqInfoPolicyName(ctx, policyName)

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
		// When the user is root credential you are not allowed to
		// add policies for root user.
		if entityName == globalActiveCred.AccessKey {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errIAMActionNotAllowed), r.URL)
			return
		}
	}

	// Validate that user or group exists.
	if !isGroup {
		if globalIAMSys.GetUsersSysType() == MinIOUsersSysType {
			_, ok := globalIAMSys.GetUser(ctx, entityName)
			if !ok {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errNoSuchUser), r.URL)
				return
			}
		}
	} else {
		_, err := globalIAMSys.GetGroupDescription(entityName)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	userType := regUser
	if globalIAMSys.GetUsersSysType() == LDAPUsersSysType {
		userType = stsUser

		// Validate that the user or group exists in LDAP and use the normalized
		// form of the entityName (which will be an LDAP DN).
		var err error
		if isGroup {
			var foundGroupDN *xldap.DNSearchResult
			var underBaseDN bool
			if foundGroupDN, underBaseDN, err = globalIAMSys.LDAPConfig.GetValidatedGroupDN(nil, entityName); err != nil {
				iamLogIf(ctx, err)
			} else if foundGroupDN == nil || !underBaseDN {
				err = errNoSuchGroup
			} else {
				entityName = foundGroupDN.NormDN
			}
		} else {
			var foundUserDN *xldap.DNSearchResult
			if foundUserDN, err = globalIAMSys.LDAPConfig.GetValidatedDNForUsername(entityName); err != nil {
				iamLogIf(ctx, err)
			} else if foundUserDN == nil {
				err = errNoSuchUser
			} else {
				entityName = foundUserDN.NormDN
			}
		}
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	updatedAt, err := globalIAMSys.PolicyDBSet(ctx, entityName, policyName, userType, isGroup)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
		Type: madmin.SRIAMItemPolicyMapping,
		PolicyMapping: &madmin.SRPolicyMapping{
			UserOrGroup: entityName,
			UserType:    int(userType),
			IsGroup:     isGroup,
			Policy:      policyName,
		},
		UpdatedAt: updatedAt,
	}))
}

// ListPolicyMappingEntities - GET /minio/admin/v3/idp/builtin/policy-entities?policy=xxx&user=xxx&group=xxx
func (a adminAPIHandlers) ListPolicyMappingEntities(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check authorization.
	objectAPI, cred := validateAdminReq(ctx, w, r,
		policy.ListGroupsAdminAction, policy.ListUsersAdminAction, policy.ListUserPoliciesAdminAction)
	if objectAPI == nil {
		return
	}

	// Validate API arguments.
	q := madmin.PolicyEntitiesQuery{
		Users:  r.Form["user"],
		Groups: r.Form["group"],
		Policy: r.Form["policy"],
	}

	// Query IAM
	res, err := globalIAMSys.QueryPolicyEntities(r.Context(), q)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// Encode result and send response.
	data, err := json.Marshal(res)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	password := cred.SecretKey
	econfigData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	writeSuccessResponseJSON(w, econfigData)
}

// AttachDetachPolicyBuiltin - POST /minio/admin/v3/idp/builtin/policy/{operation}
func (a adminAPIHandlers) AttachDetachPolicyBuiltin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.UpdatePolicyAssociationAction,
		policy.AttachPolicyAdminAction)
	if objectAPI == nil {
		return
	}

	if r.ContentLength > maxEConfigJSONSize || r.ContentLength == -1 {
		// More than maxConfigSize bytes were available
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigTooLarge), r.URL)
		return
	}

	// Ensure body content type is opaque to ensure that request body has not
	// been interpreted as form data.
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/octet-stream" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	operation := mux.Vars(r)["operation"]
	if operation != "attach" && operation != "detach" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}
	isAttach := operation == "attach"

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	var par madmin.PolicyAssociationReq
	if err = json.Unmarshal(reqBytes, &par); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	if err = par.IsValid(); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	updatedAt, addedOrRemoved, _, err := globalIAMSys.PolicyDBUpdateBuiltin(ctx, isAttach, par)
	if err != nil {
		if err == errNoSuchUser || err == errNoSuchGroup {
			if globalIAMSys.LDAPConfig.Enabled() {
				// When LDAP is enabled, warn user that they are using the wrong
				// API. FIXME: error can be no such group as well - fix errNoSuchUserLDAPWarn
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errNoSuchUserLDAPWarn), r.URL)
				return
			}
		}
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	setReqInfoPolicyName(ctx, strings.Join(addedOrRemoved, ","))

	respBody := madmin.PolicyAssociationResp{
		UpdatedAt: updatedAt,
	}
	if isAttach {
		respBody.PoliciesAttached = addedOrRemoved
	} else {
		respBody.PoliciesDetached = addedOrRemoved
	}

	data, err := json.Marshal(respBody)
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
}

// RevokeTokens - POST /minio/admin/v3/revoke-tokens/{userProvider}
func (a adminAPIHandlers) RevokeTokens(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	userProvider := mux.Vars(r)["userProvider"]

	user := r.Form.Get("user")
	tokenRevokeType := r.Form.Get("tokenRevokeType")
	fullRevoke := r.Form.Get("fullRevoke") == "true"
	isTokenSelfRevoke := user == ""
	if !isTokenSelfRevoke {
		var err error
		user, err = getUserWithProvider(ctx, userProvider, user, false)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	if (user != "" && tokenRevokeType == "" && !fullRevoke) || (tokenRevokeType != "" && fullRevoke) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	adminPrivilege := globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.RemoveServiceAccountAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
	})

	if !adminPrivilege || isTokenSelfRevoke {
		parentUser := cred.AccessKey
		if cred.ParentUser != "" {
			parentUser = cred.ParentUser
		}
		if !isTokenSelfRevoke && user != parentUser {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
		user = parentUser
	}

	// Infer token revoke type from the request if requestor is STS.
	if isTokenSelfRevoke && tokenRevokeType == "" && !fullRevoke {
		if cred.IsTemp() {
			tokenRevokeType, _ = cred.Claims[tokenRevokeTypeClaim].(string)
		}
		if tokenRevokeType == "" {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNoTokenRevokeType), r.URL)
			return
		}
	}

	err := globalIAMSys.RevokeTokens(ctx, user, tokenRevokeType)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessNoContent(w)
}

// InfoAccessKey - GET /minio/admin/v3/info-access-key?access-key=<access-key>
func (a adminAPIHandlers) InfoAccessKey(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	accessKey := mux.Vars(r)["accessKey"]
	if accessKey == "" {
		accessKey = cred.AccessKey
	}

	u, ok := globalIAMSys.GetUser(ctx, accessKey)
	targetCred := u.Credentials

	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.ListServiceAccountsAdminAction,
		ConditionValues: getConditionValues(r, "", cred),
		IsOwner:         owner,
		Claims:          cred.Claims,
	}) {
		// If requested user does not exist and requestor is not allowed to list service accounts, return access denied.
		if !ok {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}

		requestUser := cred.AccessKey
		if cred.ParentUser != "" {
			requestUser = cred.ParentUser
		}

		if requestUser != targetCred.ParentUser {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	}

	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchAccessKey), r.URL)
		return
	}

	var (
		sessionPolicy *policy.Policy
		err           error
		userType      string
	)
	switch {
	case targetCred.IsTemp():
		userType = "STS"
		_, sessionPolicy, err = globalIAMSys.GetTemporaryAccount(ctx, accessKey)
		if err == errNoSuchTempAccount {
			err = errNoSuchAccessKey
		}
	case targetCred.IsServiceAccount():
		userType = "Service Account"
		_, sessionPolicy, err = globalIAMSys.GetServiceAccount(ctx, accessKey)
		if err == errNoSuchServiceAccount {
			err = errNoSuchAccessKey
		}
	default:
		err = errNoSuchAccessKey
	}
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	// if session policy is nil or empty, then it is implied policy
	impliedPolicy := sessionPolicy == nil || (sessionPolicy.Version == "" && len(sessionPolicy.Statements) == 0)

	var svcAccountPolicy policy.Policy

	if !impliedPolicy {
		svcAccountPolicy = *sessionPolicy
	} else {
		policiesNames, err := globalIAMSys.PolicyDBGet(targetCred.ParentUser, targetCred.Groups...)
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

	var expiration *time.Time
	if !targetCred.Expiration.IsZero() && !targetCred.Expiration.Equal(timeSentinel) {
		expiration = &targetCred.Expiration
	}

	userProvider := guessUserProvider(targetCred)

	infoResp := madmin.InfoAccessKeyResp{
		AccessKey: accessKey,
		InfoServiceAccountResp: madmin.InfoServiceAccountResp{
			ParentUser:    targetCred.ParentUser,
			Name:          targetCred.Name,
			Description:   targetCred.Description,
			AccountStatus: targetCred.Status,
			ImpliedPolicy: impliedPolicy,
			Policy:        string(policyJSON),
			Expiration:    expiration,
		},

		UserType:     userType,
		UserProvider: userProvider,
	}

	populateProviderInfoFromClaims(targetCred.Claims, userProvider, &infoResp)

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

const (
	allPoliciesFile           = "policies.json"
	allUsersFile              = "users.json"
	allGroupsFile             = "groups.json"
	allSvcAcctsFile           = "svcaccts.json"
	userPolicyMappingsFile    = "user_mappings.json"
	groupPolicyMappingsFile   = "group_mappings.json"
	stsUserPolicyMappingsFile = "stsuser_mappings.json"

	iamAssetsDir = "iam-assets"
)

var iamExportFiles = []string{
	allPoliciesFile,
	allUsersFile,
	allGroupsFile,
	allSvcAcctsFile,
	userPolicyMappingsFile,
	groupPolicyMappingsFile,
	stsUserPolicyMappingsFile,
}

// ExportIAMHandler - exports all iam info as a zipped file
func (a adminAPIHandlers) ExportIAM(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ExportIAMAction)
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
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
			adminLogIf(ctx, zerr)
			return nil
		}
		header.Method = zip.Deflate
		zwriter, zerr := zipWriter.CreateHeader(header)
		if zerr != nil {
			adminLogIf(ctx, zerr)
			return nil
		}
		if _, err := io.Copy(zwriter, r); err != nil {
			adminLogIf(ctx, err)
		}
		return nil
	}

	for _, f := range iamExportFiles {
		iamFile := pathJoin(iamAssetsDir, f)
		switch f {
		case allPoliciesFile:
			allPolicies, err := globalIAMSys.ListPolicies(ctx, "")
			if err != nil {
				adminLogIf(ctx, err)
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
			err := globalIAMSys.store.loadUsers(ctx, regUser, userIdentities)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userAccounts := make(map[string]madmin.AddOrUpdateUserReq)
			for u, uid := range userIdentities {
				userAccounts[u] = madmin.AddOrUpdateUserReq{
					SecretKey: uid.Credentials.SecretKey,
					Status: func() madmin.AccountStatus {
						// Export current credential status
						if uid.Credentials.Status == auth.AccountOff {
							return madmin.AccountDisabled
						}
						return madmin.AccountEnabled
					}(),
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
			err := globalIAMSys.store.loadGroups(ctx, groups)
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
			err := globalIAMSys.store.loadUsers(ctx, svcUser, serviceAccounts)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			svcAccts := make(map[string]madmin.SRSvcAccCreate)
			for user, acc := range serviceAccounts {
				if user == siteReplicatorSvcAcc {
					// skip site-replication service account.
					continue
				}
				claims, err := globalIAMSys.GetClaimsForSvcAcc(ctx, acc.Credentials.AccessKey)
				if err != nil {
					writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
					return
				}
				sa, policy, err := globalIAMSys.GetServiceAccount(ctx, acc.Credentials.AccessKey)
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
					SessionPolicy: policyJSON,
					Status:        acc.Credentials.Status,
					Name:          sa.Name,
					Description:   sa.Description,
					Expiration:    &sa.Expiration,
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
			userPolicyMap := xsync.NewMapOf[string, MappedPolicy]()
			err := globalIAMSys.store.loadMappedPolicies(ctx, regUser, false, userPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userPolData, err := json.Marshal(mappedPoliciesToMap(userPolicyMap))
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(userPolData), iamFile, len(userPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case groupPolicyMappingsFile:
			groupPolicyMap := xsync.NewMapOf[string, MappedPolicy]()
			err := globalIAMSys.store.loadMappedPolicies(ctx, regUser, true, groupPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			grpPolData, err := json.Marshal(mappedPoliciesToMap(groupPolicyMap))
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}

			if err = rawDataFn(bytes.NewReader(grpPolData), iamFile, len(grpPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		case stsUserPolicyMappingsFile:
			userPolicyMap := xsync.NewMapOf[string, MappedPolicy]()
			err := globalIAMSys.store.loadMappedPolicies(ctx, stsUser, false, userPolicyMap)
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			userPolData, err := json.Marshal(mappedPoliciesToMap(userPolicyMap))
			if err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
			if err = rawDataFn(bytes.NewReader(userPolData), iamFile, len(userPolData)); err != nil {
				writeErrorResponse(ctx, w, exportError(ctx, err, iamFile, ""), r.URL)
				return
			}
		}
	}
}

// ImportIAM - imports all IAM info into MinIO
func (a adminAPIHandlers) ImportIAM(w http.ResponseWriter, r *http.Request) {
	a.importIAM(w, r, "")
}

// ImportIAMV2 - imports all IAM info into MinIO
func (a adminAPIHandlers) ImportIAMV2(w http.ResponseWriter, r *http.Request) {
	a.importIAM(w, r, "v2")
}

// ImportIAM - imports all IAM info into MinIO
func (a adminAPIHandlers) importIAM(w http.ResponseWriter, r *http.Request, apiVer string) {
	ctx := r.Context()

	// Validate signature, permissions and get current object layer instance.
	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ImportIAMAction)
	if objectAPI == nil || globalNotificationSys == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	data, err := io.ReadAll(r.Body)
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

	var skipped, removed, added madmin.IAMEntities
	var failed madmin.IAMErrEntities

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
			var allPolicies map[string]policy.Policy
			data, err = io.ReadAll(f)
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
					removed.Policies = append(removed.Policies, policyName)
				} else {
					_, err = globalIAMSys.SetPolicy(ctx, policyName, policy)
					added.Policies = append(added.Policies, policyName)
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
			data, err := io.ReadAll(f)
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
				if accessKey == globalActiveCred.AccessKey {
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

				// Check if accessKey has beginning and end space characters, this only applies to new users.
				if !exists && hasSpaceBE(accessKey) {
					writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminResourceInvalidArgument, err, allUsersFile, accessKey), r.URL)
					return
				}

				if _, err = globalIAMSys.CreateUser(ctx, accessKey, ureq); err != nil {
					failed.Users = append(failed.Users, madmin.IAMErrEntity{Name: accessKey, Error: err})
				} else {
					added.Users = append(added.Users, accessKey)
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
			data, err := io.ReadAll(f)
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
						writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminResourceInvalidArgument, gerr, allGroupsFile, group), r.URL)
						return
					}
				}
				if _, gerr := globalIAMSys.AddUsersToGroup(ctx, group, grpInfo.Members); gerr != nil {
					failed.Groups = append(failed.Groups, madmin.IAMErrEntity{Name: group, Error: err})
				} else {
					added.Groups = append(added.Groups, group)
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
			data, err := io.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, allSvcAcctsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &serviceAcctReqs); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, allSvcAcctsFile, ""), r.URL)
				return
			}

			// Validations for LDAP enabled deployments.
			if globalIAMSys.LDAPConfig.Enabled() {
				skippedAccessKeys, err := globalIAMSys.NormalizeLDAPAccessKeypairs(ctx, serviceAcctReqs)
				skipped.ServiceAccounts = append(skipped.ServiceAccounts, skippedAccessKeys...)
				if err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, ""), r.URL)
					return
				}
			}

			for user, svcAcctReq := range serviceAcctReqs {
				if slices.Contains(skipped.ServiceAccounts, user) {
					continue
				}
				var sp *policy.Policy
				var err error
				if len(svcAcctReq.SessionPolicy) > 0 {
					sp, err = policy.ParseConfig(bytes.NewReader(svcAcctReq.SessionPolicy))
					if err != nil {
						writeErrorResponseJSON(ctx, w, importError(ctx, err, allSvcAcctsFile, user), r.URL)
						return
					}
				}
				// service account access key cannot have space characters
				// beginning and end of the string.
				if hasSpaceBE(svcAcctReq.AccessKey) {
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument), r.URL)
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
					// If the service account exists, we remove it to ensure a
					// clean import.
					err := globalIAMSys.DeleteServiceAccount(ctx, svcAcctReq.AccessKey, true)
					if err != nil {
						delErr := fmt.Errorf("failed to delete existing service account (%s) before importing it: %w", svcAcctReq.AccessKey, err)
						writeErrorResponseJSON(ctx, w, importError(ctx, delErr, allSvcAcctsFile, user), r.URL)
						return
					}
				}
				opts := newServiceAccountOpts{
					accessKey:                  user,
					secretKey:                  svcAcctReq.SecretKey,
					sessionPolicy:              sp,
					claims:                     svcAcctReq.Claims,
					name:                       svcAcctReq.Name,
					description:                svcAcctReq.Description,
					expiration:                 svcAcctReq.Expiration,
					allowSiteReplicatorAccount: false,
				}

				if _, _, err = globalIAMSys.NewServiceAccount(ctx, svcAcctReq.Parent, svcAcctReq.Groups, opts); err != nil {
					failed.ServiceAccounts = append(failed.ServiceAccounts, madmin.IAMErrEntity{Name: user, Error: err})
				} else {
					added.ServiceAccounts = append(added.ServiceAccounts, user)
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
			data, err := io.ReadAll(f)
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
				if _, err := globalIAMSys.PolicyDBSet(ctx, u, pm.Policies, regUser, false); err != nil {
					failed.UserPolicies = append(
						failed.UserPolicies,
						madmin.IAMErrPolicyEntity{
							Name:     u,
							Policies: strings.Split(pm.Policies, ","),
							Error:    err,
						})
				} else {
					added.UserPolicies = append(added.UserPolicies, map[string][]string{u: strings.Split(pm.Policies, ",")})
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
			data, err := io.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, groupPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &grpPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, groupPolicyMappingsFile, ""), r.URL)
				return
			}

			// Validations for LDAP enabled deployments.
			if globalIAMSys.LDAPConfig.Enabled() {
				isGroup := true
				skippedDN, err := globalIAMSys.NormalizeLDAPMappingImport(ctx, isGroup, grpPolicyMap)
				skipped.Groups = append(skipped.Groups, skippedDN...)
				if err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, groupPolicyMappingsFile, ""), r.URL)
					return
				}
			}

			for g, pm := range grpPolicyMap {
				if slices.Contains(skipped.Groups, g) {
					continue
				}
				if _, err := globalIAMSys.PolicyDBSet(ctx, g, pm.Policies, unknownIAMUserType, true); err != nil {
					failed.GroupPolicies = append(
						failed.GroupPolicies,
						madmin.IAMErrPolicyEntity{
							Name:     g,
							Policies: strings.Split(pm.Policies, ","),
							Error:    err,
						})
				} else {
					added.GroupPolicies = append(added.GroupPolicies, map[string][]string{g: strings.Split(pm.Policies, ",")})
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
			data, err := io.ReadAll(f)
			if err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrInvalidRequest, err, stsUserPolicyMappingsFile, ""), r.URL)
				return
			}
			if err = json.Unmarshal(data, &userPolicyMap); err != nil {
				writeErrorResponseJSON(ctx, w, importErrorWithAPIErr(ctx, ErrAdminConfigBadJSON, err, stsUserPolicyMappingsFile, ""), r.URL)
				return
			}

			// Validations for LDAP enabled deployments.
			if globalIAMSys.LDAPConfig.Enabled() {
				isGroup := true
				skippedDN, err := globalIAMSys.NormalizeLDAPMappingImport(ctx, !isGroup, userPolicyMap)
				skipped.Users = append(skipped.Users, skippedDN...)
				if err != nil {
					writeErrorResponseJSON(ctx, w, importError(ctx, err, stsUserPolicyMappingsFile, ""), r.URL)
					return
				}
			}
			for u, pm := range userPolicyMap {
				if slices.Contains(skipped.Users, u) {
					continue
				}
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

				if _, err := globalIAMSys.PolicyDBSet(ctx, u, pm.Policies, stsUser, false); err != nil {
					failed.STSPolicies = append(
						failed.STSPolicies,
						madmin.IAMErrPolicyEntity{
							Name:     u,
							Policies: strings.Split(pm.Policies, ","),
							Error:    err,
						})
				} else {
					added.STSPolicies = append(added.STSPolicies, map[string][]string{u: strings.Split(pm.Policies, ",")})
				}
			}
		}
	}

	if apiVer == "v2" {
		iamr := madmin.ImportIAMResult{
			Skipped: skipped,
			Removed: removed,
			Added:   added,
			Failed:  failed,
		}

		b, err := json.Marshal(iamr)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}

		writeSuccessResponseJSON(w, b)
	}
}

func addExpirationToCondValues(exp *time.Time, condValues map[string][]string) error {
	if exp == nil || exp.IsZero() || exp.Equal(timeSentinel) {
		return nil
	}
	dur := time.Until(*exp)
	if dur <= 0 {
		return errors.New("unsupported expiration time")
	}
	condValues["DurationSeconds"] = []string{strconv.FormatInt(int64(dur.Seconds()), 10)}
	return nil
}

func commonAddServiceAccount(r *http.Request, ldap bool) (context.Context, auth.Credentials, newServiceAccountOpts, madmin.AddServiceAccountReq, string, APIError) {
	ctx := r.Context()

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil || globalNotificationSys == nil {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErr(ErrServerNotInitialized)
	}

	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
	if s3Err != ErrNone {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErr(s3Err)
	}

	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err)
	}

	var createReq madmin.AddServiceAccountReq
	if err = json.Unmarshal(reqBytes, &createReq); err != nil {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErrWithErr(ErrAdminConfigBadJSON, err)
	}

	if createReq.Expiration != nil && !createReq.Expiration.IsZero() {
		// truncate expiration at the second.
		truncateTime := createReq.Expiration.Truncate(time.Second)
		createReq.Expiration = &truncateTime
	}

	// service account access key cannot have space characters beginning and end of the string.
	if hasSpaceBE(createReq.AccessKey) {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument)
	}

	if err := createReq.Validate(); err != nil {
		// Since this validation would happen client side as well, we only send
		// a generic error message here.
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErr(ErrAdminResourceInvalidArgument)
	}
	// If the request did not set a TargetUser, the service account is
	// created for the request sender.
	targetUser := createReq.TargetUser
	if targetUser == "" {
		targetUser = cred.AccessKey
	}

	description := createReq.Description
	if description == "" {
		description = createReq.Comment
	}
	opts := newServiceAccountOpts{
		accessKey:   createReq.AccessKey,
		secretKey:   createReq.SecretKey,
		name:        createReq.Name,
		description: description,
		expiration:  createReq.Expiration,
		claims:      make(map[string]any),
	}

	condValues := getConditionValues(r, "", cred)
	err = addExpirationToCondValues(createReq.Expiration, condValues)
	if err != nil {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", toAdminAPIErr(ctx, err)
	}

	denyOnly := (targetUser == cred.AccessKey || targetUser == cred.ParentUser)
	if ldap && !denyOnly {
		res, _ := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(targetUser)
		if res != nil && res.NormDN == cred.ParentUser {
			denyOnly = true
		}
	}

	// Check if action is allowed if creating access key for another user
	// Check if action is explicitly denied if for self
	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.CreateServiceAccountAdminAction,
		ConditionValues: condValues,
		IsOwner:         owner,
		Claims:          cred.Claims,
		DenyOnly:        denyOnly,
	}) {
		return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", errorCodes.ToAPIErr(ErrAccessDenied)
	}

	var sp *policy.Policy
	if len(createReq.Policy) > 0 {
		sp, err = policy.ParseConfig(bytes.NewReader(createReq.Policy))
		if err != nil {
			return ctx, auth.Credentials{}, newServiceAccountOpts{}, madmin.AddServiceAccountReq{}, "", toAdminAPIErr(ctx, err)
		}
	}

	opts.sessionPolicy = sp

	return ctx, cred, opts, createReq, targetUser, APIError{}
}

// setReqInfoPolicyName will set the given policyName as a tag on the context's request info,
// so that it appears in audit logs.
func setReqInfoPolicyName(ctx context.Context, policyName string) {
	reqInfo := logger.GetReqInfo(ctx)
	reqInfo.SetTags("policyName", policyName)
}
