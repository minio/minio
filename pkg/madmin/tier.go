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
	"path"
)

// tierAPI is API path prefix for tier related admin APIs
const tierAPI = "tier"

// AddTier adds a new remote tier.
func (adm *AdminClient) AddTier(ctx context.Context, cfg *TierConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	encData, err := EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: path.Join(adminAPIPrefix, tierAPI),
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

// ListTiers returns a list of remote tiers configured.
func (adm *AdminClient) ListTiers(ctx context.Context) ([]*TierConfig, error) {
	reqData := requestData{
		relPath: path.Join(adminAPIPrefix, tierAPI),
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

	var tiers []*TierConfig
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

// TierCreds is used to pass remote tier credentials in a tier-edit operation.
type TierCreds struct {
	AccessKey string `json:"access,omitempty"`
	SecretKey string `json:"secret,omitempty"`
	CredsJSON []byte `json:"creds,omitempty"`
}

// EditTier supports updating credentials for the remote tier identified by tierName.
func (adm *AdminClient) EditTier(ctx context.Context, tierName string, creds TierCreds) error {
	data, err := json.Marshal(creds)
	if err != nil {
		return err
	}

	var encData []byte
	encData, err = EncryptData(adm.getSecretKey(), data)
	if err != nil {
		return err
	}

	reqData := requestData{
		relPath: path.Join(adminAPIPrefix, tierAPI, tierName),
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
