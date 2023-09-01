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
	"io"
	"net/http"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	iampolicy "github.com/minio/pkg/v2/policy"
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
		iampolicy.ListGroupsAdminAction, iampolicy.ListUsersAdminAction, iampolicy.ListUserPoliciesAdminAction)
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

	objectAPI, cred := validateAdminReq(ctx, w, r, iampolicy.UpdatePolicyAssociationAction)
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
