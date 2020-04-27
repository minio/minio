/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
