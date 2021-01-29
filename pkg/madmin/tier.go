/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
)

const TierAPI = "tier"

func (adm *AdminClient) AddTier(ctx context.Context, cfg TierConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	encData, err := EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: strings.Join([]string{adminAPIPrefix, TierAPI}, "/"),
		content: encData,
	}

	// Execute PUT on /minio/admin/v3/tier to add a remote tier
	resp, err := adm.executeMethod(ctx, http.MethodPut, reqData)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

func (adm *AdminClient) ListTiers(ctx context.Context) ([]TierConfig, error) {
	reqData := requestData{
		relPath: strings.Join([]string{adminAPIPrefix, TierAPI}, "/"),
	}

	// Execute GET on /minio/admin/v3/tier to list remote tiers configured.
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	var tiers []TierConfig
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return tiers, err
	}

	err = json.Unmarshal(b, &tiers)
	if err != nil {
		return tiers, err
	}

	return tiers, nil
}

type TierCreds struct {
	AccessKey string `json:"access,omitempty"`
	SecretKey string `json:"secret,omitempty"`
	CredsJSON []byte `json:"creds,omitempty"`
}

func (adm *AdminClient) EditTier(ctx context.Context, tierName string, creds TierCreds) error {
	data, err := json.Marshal(creds)
	if err != nil {
		return err
	}

	encData, err := EncryptData(adm.getSecretKey(), data)
	reqData := requestData{
		relPath: strings.Join([]string{adminAPIPrefix, TierAPI, tierName}, "/"),
		content: encData,
	}

	// Execute POST on /minio/admin/v3/tier/tierName" to edit a tier
	// configured.
	resp, err := adm.executeMethod(ctx, http.MethodPost, reqData)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

func (adm *AdminClient) RemoveTier(ctx context.Context, tierName string) error {
	reqData := requestData{
		relPath: strings.Join([]string{adminAPIPrefix, TierAPI, tierName}, "/"),
	}

	// Execute DELETE on /minio/admin/v3/tier/tierName" to remove a tier
	// configured.
	resp, err := adm.executeMethod(ctx, http.MethodDelete, reqData)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
