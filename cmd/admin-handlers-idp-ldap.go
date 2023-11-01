// Copyright (c) 2015-2022 MinIO, Inc.
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
	"net/http"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	"github.com/minio/pkg/v2/policy"
)

// ListLDAPPolicyMappingEntities lists users/groups mapped to given/all policies.
//
// GET <admin-prefix>/idp/ldap/policy-entities?[query-params]
//
// Query params:
//
//	user=... -> repeatable query parameter, specifying users to query for
//	policy mapping
//
//	group=... -> repeatable query parameter, specifying groups to query for
//	policy mapping
//
//	policy=... -> repeatable query parameter, specifying policy to query for
//	user/group mapping
//
// When all query parameters are omitted, returns mappings for all policies.
func (a adminAPIHandlers) ListLDAPPolicyMappingEntities(w http.ResponseWriter, r *http.Request) {
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

	res, err := globalIAMSys.QueryLDAPPolicyEntities(r.Context(), q)
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

// AttachDetachPolicyLDAP attaches or detaches policies from an LDAP entity
// (user or group).
//
// POST <admin-prefix>/idp/ldap/policy/{operation}
func (a adminAPIHandlers) AttachDetachPolicyLDAP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check authorization.

	objectAPI, cred := validateAdminReq(ctx, w, r, policy.UpdatePolicyAssociationAction)
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

	// Validate operation
	operation := mux.Vars(r)["operation"]
	if operation != "attach" && operation != "detach" {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminInvalidArgument), r.URL)
		return
	}

	isAttach := operation == "attach"

	// Validate API arguments in body.
	password := cred.SecretKey
	reqBytes, err := madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	var par madmin.PolicyAssociationReq
	err = json.Unmarshal(reqBytes, &par)
	if err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if err := par.IsValid(); err != nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminConfigBadJSON), r.URL)
		return
	}

	// Call IAM subsystem
	updatedAt, addedOrRemoved, _, err := globalIAMSys.PolicyDBUpdateLDAP(ctx, isAttach, par)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

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

// AddServiceAccountLDAP adds a new service account for provided LDAP username or DN
//
// PUT /minio/admin/v3/ldap/add-service-account
func (a adminAPIHandlers) AddServiceAccountLDAP(w http.ResponseWriter, r *http.Request) {
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

	if err := createReq.Validate(); err != nil {
		// Since this validation would happen client side as well, we only send
		// a generic error message here.
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
		claims:      make(map[string]interface{}),
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
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     requestorUser,
			Groups:          requestorGroups,
			Action:          policy.CreateServiceAccountAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
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

		// Deny if the target user is not LDAP
		isLDAP, err := globalIAMSys.LDAPConfig.DoesUsernameExist(targetUser)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if isLDAP == "" {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchUser), r.URL)
			return
		}

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
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     requestorUser,
			Groups:          requestorGroups,
			Action:          policy.CreateServiceAccountAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}

		// fail if ldap is not enabled
		if !globalIAMSys.LDAPConfig.Enabled() {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errors.New("LDAP not enabled")), r.URL)
		}

		// check if targetUser is username or DN
		if globalIAMSys.LDAPConfig.IsLDAPUserDN(targetUser) {
			opts.claims[ldapUser] = targetUser // DN
			targetUser, targetGroups, err = globalIAMSys.LDAPConfig.LookupDN(targetUser)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			opts.claims[ldapUserN] = targetUser // simple username
		} else {
			// targetUser is simple username
			opts.claims[ldapUserN] = targetUser // simple username
			targetUser, targetGroups, err = globalIAMSys.LDAPConfig.LookupUserDN(targetUser)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			opts.claims[ldapUser] = targetUser // DN
		}

	}

	var sp *policy.Policy
	if len(createReq.Policy) > 0 {
		sp, err = policy.ParseConfig(bytes.NewReader(createReq.Policy))
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

	encryptedData, err := madmin.EncryptData(password, data)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, encryptedData)

	// Call hook for cluster-replication if the service account is not for a
	// root user.
	if newCred.ParentUser != globalActiveCred.AccessKey {
		logger.LogIf(ctx, globalSiteReplicationSys.IAMChangeHook(ctx, madmin.SRIAMItem{
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
					SessionPolicy: createReq.Policy,
					Status:        auth.AccountOn,
					Expiration:    createReq.Expiration,
				},
			},
			UpdatedAt: updatedAt,
		}))
	}
}

// // ListAccessKeysLDAP - GET /minio/admin/v3/ldap/list-access-keys
// func (a adminAPIHandlers) ListAccessKeysLDAP(w http.ResponseWriter, r *http.Request) {
// 	ctx := r.Context()

// 	// Get current object layer instance.
// 	objectAPI := newObjectLayerFn()
// 	if objectAPI == nil || globalNotificationSys == nil {
// 		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
// 		return
// 	}

// 	cred, owner, s3Err := validateAdminSignature(ctx, r, "")
// 	if s3Err != ErrNone {
// 		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
// 		return
// 	}

// 	var targetAccount string

// 	// If listing is requested for a specific user (who is not the request
// 	// sender), check that the user has permissions.
// 	user := r.Form.Get("user")
// 	if user != "" && user != cred.AccessKey {
// 		if !globalIAMSys.IsAllowed(policy.Args{
// 			AccountName:     cred.AccessKey,
// 			Groups:          cred.Groups,
// 			Action:          policy.ListServiceAccountsAdminAction,
// 			ConditionValues: getConditionValues(r, "", cred),
// 			IsOwner:         owner,
// 			Claims:          cred.Claims,
// 		}) {
// 			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
// 			return
// 		}
// 		targetAccount = user
// 	} else {
// 		targetAccount = cred.AccessKey
// 		if cred.ParentUser != "" {
// 			targetAccount = cred.ParentUser
// 		}
// 	}

// 	listType := r.Form.Get("list-type")
// 	if listType != "sts-only" && listType != "svcacct-only" && listType != "" {
// 		// default to both
// 		listType = ""
// 	}

// 	var serviceAccounts []auth.Credentials
// 	var stsKeys []UserIdentity
// 	var err error

// 	if listType == "" || listType == "sts-only" {
// 		stsKeys, err = globalIAMSys.ListTempAccounts(ctx, targetAccount)
// 		if err != nil {
// 			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
// 			return
// 		}
// 	}
// 	if listType == "" || listType == "svcacct-only" {
// 		serviceAccounts, err = globalIAMSys.ListServiceAccounts(ctx, targetAccount)
// 		if err != nil {
// 			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
// 			return
// 		}
// 	}

// 	var serviceAccountList []madmin.ServiceAccountInfo
// 	var stsKeyList []madmin.ServiceAccountInfo

// 	for _, svc := range serviceAccounts {
// 		expiryTime := svc.Expiration
// 		serviceAccountList = append(serviceAccountList, madmin.ServiceAccountInfo{
// 			AccessKey:  svc.AccessKey,
// 			Expiration: &expiryTime,
// 		})
// 	}
// 	for _, sts := range stsKeys {
// 		expiryTime := sts.Credentials.Expiration
// 		stsKeyList = append(stsKeyList, madmin.ServiceAccountInfo{
// 			AccessKey:  sts.Credentials.AccessKey,
// 			Expiration: &expiryTime,
// 		})
// 	}

// 	listResp := madmin.ListAccessKeysLDAPResp{
// 		ServiceAccounts: serviceAccountList,
// 		STSKeys:         stsKeyList,
// 	}

// 	data, err := json.Marshal(listResp)
// 	if err != nil {
// 		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
// 		return
// 	}

// 	encryptedData, err := madmin.EncryptData(cred.SecretKey, data)
// 	if err != nil {
// 		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
// 		return
// 	}

// 	writeSuccessResponseJSON(w, encryptedData)
// }
