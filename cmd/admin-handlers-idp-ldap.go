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
	"net/http"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
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
	ctx := newContext(r, w, "ListLDAPPolicyMappingEntities")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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
