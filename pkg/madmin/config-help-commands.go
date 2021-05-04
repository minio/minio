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
	"net/http"
	"net/url"
)

// Help - return sub-system level help
type Help struct {
	SubSys          string  `json:"subSys"`
	Description     string  `json:"description"`
	MultipleTargets bool    `json:"multipleTargets"`
	KeysHelp        HelpKVS `json:"keysHelp"`
}

// HelpKV - implements help messages for keys
// with value as description of the keys.
type HelpKV struct {
	Key             string `json:"key"`
	Description     string `json:"description"`
	Optional        bool   `json:"optional"`
	Type            string `json:"type"`
	MultipleTargets bool   `json:"multipleTargets"`
}

// HelpKVS - implement order of keys help messages.
type HelpKVS []HelpKV

// Keys returns help keys
func (h Help) Keys() []string {
	var keys []string
	for _, kh := range h.KeysHelp {
		keys = append(keys, kh.Key)
	}
	return keys
}

// HelpConfigKV - return help for a given sub-system.
func (adm *AdminClient) HelpConfigKV(ctx context.Context, subSys, key string, envOnly bool) (Help, error) {
	v := url.Values{}
	v.Set("subSys", subSys)
	v.Set("key", key)
	if envOnly {
		v.Set("env", "")
	}

	reqData := requestData{
		relPath:     adminAPIPrefix + "/help-config-kv",
		queryValues: v,
	}

	// Execute GET on /minio/admin/v3/help-config-kv
	resp, err := adm.executeMethod(ctx, http.MethodGet, reqData)
	if err != nil {
		return Help{}, err
	}
	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return Help{}, httpRespToErrorResponse(resp)
	}

	var help = Help{}
	d := json.NewDecoder(resp.Body)
	d.DisallowUnknownFields()
	if err = d.Decode(&help); err != nil {
		return help, err
	}

	return help, nil
}
