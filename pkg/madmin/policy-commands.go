/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package madmin

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
)

// ListCannedPolicies - list all configured canned policies.
func (adm *AdminClient) ListCannedPolicies() (map[string][]byte, error) {
	reqData := requestData{
		relPath: "/v1/list-canned-policies",
	}

	// Execute GET on /minio/admin/v1/list-canned-policies
	resp, err := adm.executeMethod("GET", reqData)

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

	var policies = make(map[string][]byte)
	if err = json.Unmarshal(respBytes, &policies); err != nil {
		return nil, err
	}

	return policies, nil
}

// RemoveCannedPolicy - remove a policy for a canned.
func (adm *AdminClient) RemoveCannedPolicy(policyName string) error {
	queryValues := url.Values{}
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     "/v1/remove-canned-policy",
		queryValues: queryValues,
	}

	// Execute DELETE on /minio/admin/v1/remove-canned-policy to remove policy.
	resp, err := adm.executeMethod("DELETE", reqData)

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
func (adm *AdminClient) AddCannedPolicy(policyName, policy string) error {
	queryValues := url.Values{}
	queryValues.Set("name", policyName)

	reqData := requestData{
		relPath:     "/v1/add-canned-policy",
		queryValues: queryValues,
		content:     []byte(policy),
	}

	// Execute PUT on /minio/admin/v1/add-canned-policy to set policy.
	resp, err := adm.executeMethod("PUT", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
