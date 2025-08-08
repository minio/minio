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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/mux"
	xldap "github.com/minio/pkg/v3/ldap"
	"github.com/minio/pkg/v3/policy"
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

	// fail if ldap is not enabled
	if !globalIAMSys.LDAPConfig.Enabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminLDAPNotEnabled), r.URL)
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
		adminLogIf(ctx, err)
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
// PUT /minio/admin/v3/idp/ldap/add-service-account
func (a adminAPIHandlers) AddServiceAccountLDAP(w http.ResponseWriter, r *http.Request) {
	ctx, cred, opts, createReq, targetUser, APIError := commonAddServiceAccount(r, true)
	if APIError.Code != "" {
		writeErrorResponseJSON(ctx, w, APIError, r.URL)
		return
	}

	// fail if ldap is not enabled
	if !globalIAMSys.LDAPConfig.Enabled() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminLDAPNotEnabled), r.URL)
		return
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
	isSvcAccForRequestor := targetUser == requestorUser || targetUser == requestorParentUser

	var (
		targetGroups []string
		err          error
	)

	// If we are creating svc account for request sender, ensure that targetUser
	// is a real user (i.e. not derived credentials).
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

		// Deny if the target user is not LDAP
		foundResult, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(targetUser)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if foundResult == nil {
			err := errors.New("Specified user does not exist on LDAP server")
			APIErr := errorCodes.ToAPIErrWithErr(ErrAdminNoSuchUser, err)
			writeErrorResponseJSON(ctx, w, APIErr, r.URL)
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
		// We still need to ensure that the target user is a valid LDAP user.
		//
		// The target user may be supplied as a (short) username or a DN.
		// However, for now, we only support using the short username.

		isDN := globalIAMSys.LDAPConfig.ParsesAsDN(targetUser)
		opts.claims[ldapUserN] = targetUser // simple username
		var lookupResult *xldap.DNSearchResult
		lookupResult, targetGroups, err = globalIAMSys.LDAPConfig.LookupUserDN(targetUser)
		if err != nil {
			// if not found, check if DN
			if strings.Contains(err.Error(), "User DN not found for:") {
				if isDN {
					// warn user that DNs are not allowed
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminLDAPExpectedLoginName, err), r.URL)
				} else {
					writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminNoSuchUser, err), r.URL)
				}
			}
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		targetUser = lookupResult.NormDN
		opts.claims[ldapUser] = targetUser // DN
		opts.claims[ldapActualUser] = lookupResult.ActualDN

		// Check if this user or their groups have a policy applied.
		ldapPolicies, err := globalIAMSys.PolicyDBGet(targetUser, targetGroups...)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		if len(ldapPolicies) == 0 {
			err = fmt.Errorf("No policy set for user `%s` or any of their groups: `%s`", opts.claims[ldapActualUser], strings.Join(targetGroups, "`,`"))
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErrWithErr(ErrAdminNoSuchUser, err), r.URL)
			return
		}

		// Add LDAP attributes that were looked up into the claims.
		for attribKey, attribValue := range lookupResult.Attributes {
			opts.claims[ldapAttribPrefix+attribKey] = attribValue
		}
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

// ListAccessKeysLDAP - GET /minio/admin/v3/idp/ldap/list-access-keys
func (a adminAPIHandlers) ListAccessKeysLDAP(w http.ResponseWriter, r *http.Request) {
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

	userDN := r.Form.Get("userDN")

	// If listing is requested for a specific user (who is not the request
	// sender), check that the user has permissions.
	if userDN != "" && userDN != cred.ParentUser {
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
	} else {
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.ListServiceAccountsAdminAction,
			ConditionValues: getConditionValues(r, "", cred),
			IsOwner:         owner,
			Claims:          cred.Claims,
			DenyOnly:        true,
		}) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
		userDN = cred.AccessKey
		if cred.ParentUser != "" {
			userDN = cred.ParentUser
		}
	}

	dnResult, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(userDN)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	if dnResult == nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errNoSuchUser), r.URL)
		return
	}
	targetAccount := dnResult.NormDN

	listType := r.Form.Get("listType")
	if listType != "sts-only" && listType != "svcacc-only" && listType != "" {
		// default to both
		listType = ""
	}

	var serviceAccounts []auth.Credentials
	var stsKeys []auth.Credentials

	if listType == "" || listType == "sts-only" {
		stsKeys, err = globalIAMSys.ListSTSAccounts(ctx, targetAccount)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
	if listType == "" || listType == "svcacc-only" {
		serviceAccounts, err = globalIAMSys.ListServiceAccounts(ctx, targetAccount)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}

	var serviceAccountList []madmin.ServiceAccountInfo
	var stsKeyList []madmin.ServiceAccountInfo

	for _, svc := range serviceAccounts {
		expiryTime := svc.Expiration
		serviceAccountList = append(serviceAccountList, madmin.ServiceAccountInfo{
			AccessKey:   svc.AccessKey,
			Expiration:  &expiryTime,
			Name:        svc.Name,
			Description: svc.Description,
		})
	}
	for _, sts := range stsKeys {
		expiryTime := sts.Expiration
		stsKeyList = append(stsKeyList, madmin.ServiceAccountInfo{
			AccessKey:  sts.AccessKey,
			Expiration: &expiryTime,
		})
	}

	listResp := madmin.ListAccessKeysLDAPResp{
		ServiceAccounts: serviceAccountList,
		STSKeys:         stsKeyList,
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

// ListAccessKeysLDAPBulk - GET /minio/admin/v3/idp/ldap/list-access-keys-bulk
func (a adminAPIHandlers) ListAccessKeysLDAPBulk(w http.ResponseWriter, r *http.Request) {
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

	dnList := r.Form["userDNs"]
	isAll := r.Form.Get("all") == "true"
	selfOnly := !isAll && len(dnList) == 0

	if isAll && len(dnList) > 0 {
		// This should be checked on client side, so return generic error
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	// Empty DN list and not self, list access keys for all users
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
	} else if len(dnList) == 1 {
		var dn string
		foundResult, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(dnList[0])
		if err == nil {
			dn = foundResult.NormDN
		}
		if dn == cred.ParentUser || dnList[0] == cred.ParentUser {
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

	if selfOnly && len(dnList) == 0 {
		selfDN := cred.AccessKey
		if cred.ParentUser != "" {
			selfDN = cred.ParentUser
		}
		dnList = append(dnList, selfDN)
	}

	var ldapUserList []string
	if isAll {
		ldapUsers, err := globalIAMSys.ListLDAPUsers(ctx)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		for user := range ldapUsers {
			ldapUserList = append(ldapUserList, user)
		}
	} else {
		for _, userDN := range dnList {
			// Validate the userDN
			foundResult, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(userDN)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			if foundResult == nil {
				continue
			}
			ldapUserList = append(ldapUserList, foundResult.NormDN)
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

	accessKeyMap := make(map[string]madmin.ListAccessKeysLDAPResp)
	for _, internalDN := range ldapUserList {
		externalDN := globalIAMSys.LDAPConfig.DecodeDN(internalDN)
		accessKeys := madmin.ListAccessKeysLDAPResp{}
		if listSTSKeys {
			stsKeys, err := globalIAMSys.ListSTSAccounts(ctx, internalDN)
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
			serviceAccounts, err := globalIAMSys.ListServiceAccounts(ctx, internalDN)
			if err != nil {
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
				return
			}
			for _, svc := range serviceAccounts {
				accessKeys.ServiceAccounts = append(accessKeys.ServiceAccounts, madmin.ServiceAccountInfo{
					AccessKey:   svc.AccessKey,
					Expiration:  &svc.Expiration,
					Name:        svc.Name,
					Description: svc.Description,
				})
			}
			// if only service accounts, skip if user has no service accounts
			if !listSTSKeys && len(serviceAccounts) == 0 {
				continue
			}
		}
		accessKeyMap[externalDN] = accessKeys
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
