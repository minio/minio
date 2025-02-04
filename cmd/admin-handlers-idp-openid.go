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
	"net/http"
	"sort"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/identity/openid"
	xldap "github.com/minio/pkg/v3/ldap"
	"github.com/minio/pkg/v3/policy"
)

// AddServiceAccountOpenID adds a new service account for provided LDAP username or DN
//
// PUT /minio/admin/v3/idp/openid/add-service-account
func (a adminAPIHandlers) AddServiceAccountOpenID(w http.ResponseWriter, r *http.Request) {
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
	isSvcAccForRequestor := false
	if targetUser == requestorUser || targetUser == requestorParentUser {
		isSvcAccForRequestor = true
	}

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
					SessionPolicy: createReq.Policy,
					Status:        auth.AccountOn,
					Expiration:    createReq.Expiration,
				},
			},
			UpdatedAt: updatedAt,
		}))
	}
}

const dummyRoleARN = "dummy-arn"

// ListAccessKeysOpenIDBulk - GET /minio/admin/v3/idp/openid/list-access-keys-bulk
func (a adminAPIHandlers) ListAccessKeysOpenIDBulk(w http.ResponseWriter, r *http.Request) {
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

	userList := r.Form["users"]
	isAll := r.Form.Get("all") == "true"
	selfOnly := !isAll && len(userList) == 0
	cfgName := r.Form.Get("configName")

	if isAll && len(userList) > 0 {
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
	} else if len(userList) == 1 && userList[0] == cred.ParentUser {
		selfOnly = true
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

	// TODO: Add this error code
	// if !globalIAMSys.OpenIDConfig.Enabled() {
	// 	writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminOpenIDNotEnabled), r.URL)
	// 	return
	// }

	if selfOnly && len(userList) == 0 {
		selfDN := cred.AccessKey
		if cred.ParentUser != "" {
			selfDN = cred.ParentUser
		}
		userList = append(userList, selfDN)
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

	s := globalServerConfig.Clone()
	roleArnMap := make(map[string]string)
	// Map of configs to a map of users to their access keys
	cfgToUsersMap := make(map[string]map[string]madmin.OpenIDUserAccessKeys)
	if cfgName != "" {
		config, err := globalIAMSys.OpenIDConfig.GetConfigInfo(s, cfgName)
		if errors.Is(err, openid.ErrProviderConfigNotFound) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
			return
		}
		for _, info := range config {
			if info.Key == "roleARN" {
				roleArnMap[info.Value] = cfgName
				break
			}
		}
		newResp := make(map[string]madmin.OpenIDUserAccessKeys)
		cfgToUsersMap[cfgName] = newResp
	} else {
		configs, err := globalIAMSys.OpenIDConfig.GetConfigList(s)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		for _, config := range configs {
			arn := dummyRoleARN
			if config.RoleARN != "" {
				arn = config.RoleARN
			}
			roleArnMap[arn] = config.Name
			newResp := make(map[string]madmin.OpenIDUserAccessKeys)
			cfgToUsersMap[config.Name] = newResp
		}
	}

	userSet := set.CreateStringSet(userList...)
	accessKeys, err := globalIAMSys.ListAllAccessKeys(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	for _, accessKey := range accessKeys {
		// Filter out any disqualifying access keys
		sub, ok := accessKey.Claims[subClaim]
		if !ok {
			continue // OpenID access keys must have a sub claim
		}
		if (!listSTSKeys && accessKey.IsTemp()) || (!listServiceAccounts && accessKey.IsServiceAccount()) {
			continue // skip if not the type we want
		}
		if !userSet.IsEmpty() && !userSet.Contains(accessKey.ParentUser) {
			continue // skip if not in the user list
		}
		arn, ok := accessKey.Claims[roleArnClaim].(string)
		if !ok {
			continue // TODO: allow claim based OpenID access keys
		}
		matchingCfgName, ok := roleArnMap[arn]
		if !ok {
			continue // skip if not part of the target config
		}
		openIDUserAccessKeys, ok := cfgToUsersMap[matchingCfgName][accessKey.ParentUser]

		// Add new user to map if not already present
		if !ok {
			subStr, ok := sub.(string)
			if !ok {
				continue
			}
			readableClaimName := globalIAMSys.OpenIDConfig.GetReadableClaimName(matchingCfgName)
			var readableClaim string
			if readableClaimName != "" {
				readableClaim, _ = accessKey.Claims[readableClaimName].(string)
			}
			openIDUserAccessKeys = madmin.OpenIDUserAccessKeys{
				UserID:       accessKey.ParentUser,
				Sub:          subStr,
				ReadableName: readableClaim,
			}
		}
		svcAccInfo := madmin.ServiceAccountInfo{
			AccessKey:  accessKey.AccessKey,
			Expiration: &accessKey.Expiration,
		}
		if accessKey.IsServiceAccount() {
			openIDUserAccessKeys.ServiceAccounts = append(openIDUserAccessKeys.ServiceAccounts, svcAccInfo)
		} else if accessKey.IsTemp() {
			openIDUserAccessKeys.STSKeys = append(openIDUserAccessKeys.STSKeys, svcAccInfo)
		} else {
			err := fmt.Errorf("expected service account or STS key, got neither")
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		}

		cfgToUsersMap[matchingCfgName][accessKey.ParentUser] = openIDUserAccessKeys
	}

	// Convert map to slice and sort
	resp := make([]madmin.ListAccessKeysOpenIDResp, 0, len(cfgToUsersMap))
	for cfgName, usersMap := range cfgToUsersMap {
		users := make([]madmin.OpenIDUserAccessKeys, 0, len(usersMap))
		for _, user := range usersMap {
			users = append(users, user)
		}
		sort.Slice(users, func(i, j int) bool {
			return users[i].UserID < users[j].UserID
		})
		resp = append(resp, madmin.ListAccessKeysOpenIDResp{
			ConfigName: cfgName,
			Users:      users,
		})
	}
	sort.Slice(resp, func(i, j int) bool {
		return resp[i].ConfigName < resp[j].ConfigName
	})

	data, err := json.Marshal(resp)
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
