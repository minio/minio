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

package madmin

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

// InfoCannedPolicy - expand canned policy into JSON structure.
func (adm *AdminClient) InfoCannedPolicy(ctx context.Context, policyName string) (*iampolicy.Policy, error) {
	queryValues := url.Values{}
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/info-canned-policy",
		queryValues: queryValues,
	}

	// Execute GET on /minio/admin/v3/info-canned-policy
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return iampolicy.ParseConfig(resp.Body)
}

// ListCannedPolicies - list all configured canned policies.
func (adm *AdminClient) ListCannedPolicies(ctx context.Context) (map[string]*iampolicy.Policy, error) {
	reqData := requestData{
		relPath: adminAPIPrefix + "/list-canned-policies",
	}

	// Execute GET on /minio/admin/v3/list-canned-policies
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)

	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var policies = make(map[string]*iampolicy.Policy)
	if err = json.Unmarshal(respBytes, &policies); err != nil {
		return nil, err
	}

	return policies, nil
}

// RemoveCannedPolicy - remove a policy for a canned.
func (adm *AdminClient) RemoveCannedPolicy(ctx context.Context, policyName string) error {
	queryValues := url.Values{}
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/remove-canned-policy",
		queryValues: queryValues,
	}

	// Execute DELETE on /minio/admin/v3/remove-canned-policy to remove policy.
	resp, err := adm.executeMethod(ctx, http.MethodDelete, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// AddCannedPolicy - adds a policy for a canned.
func (adm *AdminClient) AddCannedPolicy(ctx context.Context, policyName string, policy *iampolicy.Policy) error {
	if policy == nil {
		return ErrInvalidArgument("policy input cannot be empty")
	}

	if err := policy.Validate(); err != nil {
		return err
	}

	buf, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	queryValues := url.Values{}
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/add-canned-policy",
		queryValues: queryValues,
		content:     buf,
	}

	// Execute PUT on /minio/admin/v3/add-canned-policy to set policy.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// SetPolicy - sets the policy for a user or a group.
func (adm *AdminClient) SetPolicy(ctx context.Context, policyName, entityName string, isGroup bool) error {
	queryValues := url.Values{}
	queryValues.Set("policyName", policyName)
	queryValues.Set("userOrGroup", entityName)
	groupStr := "false"
	if isGroup {
		groupStr = "true"
	}
	queryValues.Set("isGroup", groupStr)

	reqData := requestData{
		relPath:     adminAPIPrefix + "/set-user-or-group-policy",
		queryValues: queryValues,
	}

	// Execute PUT on /minio/admin/v3/set-user-or-group-policy to set policy.
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}
