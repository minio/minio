// Copyright (c) 2015-2025 MinIO, Inc.
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
	"net/http"
	"sort"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/pkg/v3/policy"
)

const dummyRoleARN = "dummy-internal"

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

	if !globalIAMSys.OpenIDConfig.Enabled {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminOpenIDNotEnabled), r.URL)
		return
	}

	userList := r.Form["users"]
	isAll := r.Form.Get("all") == "true"
	selfOnly := !isAll && len(userList) == 0
	cfgName := r.Form.Get("configName")
	allConfigs := r.Form.Get("allConfigs") == "true"
	if cfgName == "" && !allConfigs {
		cfgName = madmin.Default
	}

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
	configs, err := globalIAMSys.OpenIDConfig.GetConfigList(s)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	for _, config := range configs {
		if !allConfigs && cfgName != config.Name {
			continue
		}
		arn := dummyRoleARN
		if config.RoleARN != "" {
			arn = config.RoleARN
		}
		roleArnMap[arn] = config.Name
		newResp := make(map[string]madmin.OpenIDUserAccessKeys)
		cfgToUsersMap[config.Name] = newResp
	}
	if len(roleArnMap) == 0 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminNoSuchConfigTarget), r.URL)
		return
	}

	userSet := set.CreateStringSet(userList...)
	accessKeys, err := globalIAMSys.ListAllAccessKeys(ctx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	for _, accessKey := range accessKeys {
		// Filter out any disqualifying access keys
		_, ok := accessKey.Claims[subClaim]
		if !ok {
			continue // OpenID access keys must have a sub claim
		}
		if (!listSTSKeys && !accessKey.IsServiceAccount()) || (!listServiceAccounts && accessKey.IsServiceAccount()) {
			continue // skip if not the type we want
		}
		arn, ok := accessKey.Claims[roleArnClaim].(string)
		if !ok {
			if _, ok := accessKey.Claims[iamPolicyClaimNameOpenID()]; !ok {
				continue // skip if no roleArn and no policy claim
			}
			// claim-based provider is in the roleArnMap under dummy ARN
			arn = dummyRoleARN
		}
		matchingCfgName, ok := roleArnMap[arn]
		if !ok {
			continue // skip if not part of the target config
		}
		var id string
		if idClaim := globalIAMSys.OpenIDConfig.GetUserIDClaim(matchingCfgName); idClaim != "" {
			id, _ = accessKey.Claims[idClaim].(string)
		}
		if !userSet.IsEmpty() && !userSet.Contains(accessKey.ParentUser) && !userSet.Contains(id) {
			continue // skip if not in the user list
		}
		openIDUserAccessKeys, ok := cfgToUsersMap[matchingCfgName][accessKey.ParentUser]

		// Add new user to map if not already present
		if !ok {
			var readableClaim string
			if rc := globalIAMSys.OpenIDConfig.GetUserReadableClaim(matchingCfgName); rc != "" {
				readableClaim, _ = accessKey.Claims[rc].(string)
			}
			openIDUserAccessKeys = madmin.OpenIDUserAccessKeys{
				MinioAccessKey: accessKey.ParentUser,
				ID:             id,
				ReadableName:   readableClaim,
			}
		}
		svcAccInfo := madmin.ServiceAccountInfo{
			AccessKey:  accessKey.AccessKey,
			Expiration: &accessKey.Expiration,
		}
		if accessKey.IsServiceAccount() {
			openIDUserAccessKeys.ServiceAccounts = append(openIDUserAccessKeys.ServiceAccounts, svcAccInfo)
		} else {
			openIDUserAccessKeys.STSKeys = append(openIDUserAccessKeys.STSKeys, svcAccInfo)
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
			return users[i].MinioAccessKey < users[j].MinioAccessKey
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
