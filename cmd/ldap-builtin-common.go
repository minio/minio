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
	"io"
	"net/http"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v2/policy"
)

func commonAddServiceAccount(r *http.Request) (context.Context, auth.Credentials, newServiceAccountOpts, madmin.AddServiceAccountReq, string, APIError) {
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
		claims:      make(map[string]interface{}),
	}

	condValues := getConditionValues(r, "", cred)
	addExpirationToCondValues(createReq.Expiration, condValues)

	// Check if action is allowed if creating access key for another user
	// Check if action is explicitly denied if for self
	if !globalIAMSys.IsAllowed(policy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          policy.CreateServiceAccountAdminAction,
		ConditionValues: condValues,
		IsOwner:         owner,
		Claims:          cred.Claims,
		DenyOnly:        (targetUser == cred.AccessKey || targetUser == cred.ParentUser),
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

// iamErrorLDAPHint - converts certain IAM errors to errors with LDAP hints when LDAP is enabled.
func iamErrorLDAPHint(err error) error {
	if !globalIAMSys.LDAPConfig.Enabled() {
		return err
	}
	switch err {
	case errNoSuchUser:
		return errNoSuchUserLDAPWarn
	case errNoSuchGroup:
		return errNoSuchGroupLDAPWarn
	default:
		return err
	}
}
